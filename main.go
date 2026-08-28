package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// WAL header size
const walHeaderSize = 32

// WAL frame header size
const walFrameHeaderSize = 24

func main() {
	mode := flag.String("mode", "", "primary or replica")
	dbPath := flag.String("db", "", "path to SQLite database file")
	replicas := flag.String("replicas", "", "comma-separated replica URLs (primary mode)")
	listen := flag.String("listen", ":9090", "HTTP listen address (replica mode)")
	flag.Parse()

	if *mode == "" || *dbPath == "" {
		flag.Usage()
		os.Exit(1)
	}

	switch *mode {
	case "primary":
		runPrimary(*dbPath, *replicas)
	case "replica":
		runReplica(*dbPath, *listen)
	default:
		log.Fatalf("unknown mode: %s (use primary or replica)", *mode)
	}
}

// ============================================================
// PRIMARY MODE
// ============================================================

func runPrimary(dbPath string, replicasCSV string) {
	if replicasCSV == "" {
		log.Fatal("primary mode requires -replicas")
	}

	replicas := splitCSV(replicasCSV)
	log.Printf("walsync primary starting | db=%s | replicas=%v", dbPath, replicas)

	walPath := dbPath + "-wal"

	// Ship initial snapshot to all replicas
	log.Println("shipping initial snapshot...")
	shipSnapshot(dbPath, walPath, replicas)
	log.Println("initial snapshot shipped")

	// Watch WAL file for changes
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("failed to create watcher: %v", err)
	}
	defer watcher.Close()

	// Watch the directory (WAL file may be recreated by SQLite)
	dir := filepath.Dir(dbPath)
	if err := watcher.Add(dir); err != nil {
		log.Fatalf("failed to watch directory %s: %v", dir, err)
	}

	// Track last shipped WAL size and DB mtime
	lastShippedSize := fileSize(walPath)
	lastShippedDBMod := fileModTime(dbPath)

	// Debounce: use timer to batch rapid WAL changes
	debounceMs := 100 * time.Millisecond
	var debounceTimer *time.Timer
	debounceCh := make(chan struct{}, 1)

	// Helper: schedule a ship after debounce period
	scheduleShip := func() {
		if debounceTimer != nil {
			debounceTimer.Stop()
		}
		debounceTimer = time.AfterFunc(debounceMs, func() {
			select {
			case debounceCh <- struct{}{}:
			default:
			}
		})
	}
	// Polling fallback: check WAL size every 100ms (fsnotify unreliable on macOS)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			// Check if WAL file changed
			if event.Name == walPath && (event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create) {
				currentSize := fileSize(walPath)
				if currentSize > lastShippedSize {
					scheduleShip()
				}
			}

			// Check if DB file changed (checkpoint happened)
			if event.Name == dbPath && (event.Op&fsnotify.Write == fsnotify.Write) {
				// Checkpoint: WAL may have been reset
				newWalSize := fileSize(walPath)
				if newWalSize == 0 || newWalSize < lastShippedSize {
					// WAL was checkpointed, ship new snapshot
					log.Println("checkpoint detected, shipping snapshot...")
					shipSnapshot(dbPath, walPath, replicas)
					lastShippedSize = fileSize(walPath)
				}
			}

		case <-ticker.C:
			// Polling fallback: check if WAL grew
			currentSize := fileSize(walPath)
			if currentSize > lastShippedSize {
				scheduleShip()
			}

			// Also check if DB file changed (checkpoint happened without WAL)
			currentDBMod := fileModTime(dbPath)
			if currentDBMod != lastShippedDBMod {
				// DB file changed — checkpoint happened, ship snapshot
				log.Printf("DB file modified, shipping snapshot...")
				shipSnapshot(dbPath, walPath, replicas)
				lastShippedSize = fileSize(walPath)
				lastShippedDBMod = currentDBMod
			}

		case <-debounceCh:
			// Debounce period elapsed, ship WAL
			currentSize := fileSize(walPath)
			if currentSize <= lastShippedSize {
				continue
			}
			shipWAL(walPath, lastShippedSize, currentSize, replicas)
			lastShippedSize = currentSize

		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Printf("watcher error: %v", err)
		}
	}
}

func shipSnapshot(dbPath, walPath string, replicas []string) {
	dbData, err := os.ReadFile(dbPath)
	if err != nil {
		log.Printf("error reading db file: %v", err)
		return
	}

	walData := []byte{}
	if _, err := os.Stat(walPath); err == nil {
		walData, err = os.ReadFile(walPath)
		if err != nil {
			log.Printf("error reading wal file: %v", err)
			return
		}
	}

	// Send snapshot to each replica
	var wg sync.WaitGroup
	for _, replica := range replicas {
		wg.Add(1)
		go func(r string) {
			defer wg.Done()

			// Send DB file
			url := r + "/snapshot"
			resp, err := http.Post(url, "application/octet-stream", bytes.NewReader(dbData))
			if err != nil {
				log.Printf("error sending snapshot to %s: %v", r, err)
				return
			}
			resp.Body.Close()
			if resp.StatusCode != 200 {
				log.Printf("snapshot to %s returned %d", r, resp.StatusCode)
				return
			}

			// Send WAL file if exists
			if len(walData) > 0 {
				walURL := r + "/wal?offset=0"
				resp2, err := http.Post(walURL, "application/octet-stream", bytes.NewReader(walData))
				if err != nil {
					log.Printf("error sending wal to %s: %v", r, err)
					return
				}
				resp2.Body.Close()
			}

			log.Printf("snapshot shipped to %s (%d bytes db, %d bytes wal)", r, len(dbData), len(walData))
		}(replica)
	}
	wg.Wait()
}

func shipWAL(walPath string, offset, size int64, replicas []string) {
	// Read new WAL data from offset
	f, err := os.Open(walPath)
	if err != nil {
		log.Printf("error opening wal file: %v", err)
		return
	}
	defer f.Close()

	if _, err := f.Seek(offset, 0); err != nil {
		log.Printf("error seeking wal file: %v", err)
		return
	}

	data := make([]byte, size-offset)
	n, err := io.ReadFull(f, data)
	if err != nil && err != io.ErrUnexpectedEOF {
		log.Printf("error reading wal data: %v", err)
		return
	}
	data = data[:n]

	if len(data) == 0 {
		return
	}

	// Ship to each replica
	var wg sync.WaitGroup
	for _, replica := range replicas {
		wg.Add(1)
		go func(r string) {
			defer wg.Done()
			url := fmt.Sprintf("%s/wal?offset=%d", r, offset)
			resp, err := http.Post(url, "application/octet-stream", bytes.NewReader(data))
			if err != nil {
				log.Printf("error shipping wal to %s: %v", r, err)
				return
			}
			resp.Body.Close()
			if resp.StatusCode != 200 {
				log.Printf("wal ship to %s returned %d", r, resp.StatusCode)
			}
		}(replica)
	}
	wg.Wait()

	log.Printf("WAL shipped: %d bytes from offset %d to %d replicas", len(data), offset, len(replicas))
}

// ============================================================
// REPLICA MODE
// ============================================================

func runReplica(dbPath string, listen string) {
	walPath := dbPath + "-wal"
	log.Printf("walsync replica starting | db=%s | listen=%s", dbPath, listen)

	mux := http.NewServeMux()

	// Receive snapshot (full DB file)
	mux.HandleFunc("/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "method not allowed", 405)
			return
		}

		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		// Write to temp file, then atomically replace
		tmpPath := dbPath + ".tmp"
		if err := os.WriteFile(tmpPath, data, 0644); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		// Remove existing WAL (stale WAL from old DB)
		os.Remove(walPath)

		if err := os.Rename(tmpPath, dbPath); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		log.Printf("snapshot received: %d bytes", len(data))
		w.WriteHeader(200)
	})

	// Receive WAL data
	mux.HandleFunc("/wal", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "method not allowed", 405)
			return
		}

		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		if len(data) == 0 {
			w.WriteHeader(200)
			return
		}

		// Parse offset from query string
		offsetStr := r.URL.Query().Get("offset")
		var offset int64
		fmt.Sscanf(offsetStr, "%d", &offset)

		// Write WAL data at offset
		// If offset=0, this is a full WAL replacement (from snapshot)
		var f *os.File
		if offset == 0 {
			// Full WAL replacement
			f, err = os.Create(walPath)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
		} else {
			// Append at offset
			f, err = os.OpenFile(walPath, os.O_WRONLY, 0644)
			if err != nil {
				// WAL file doesn't exist, create it
				f, err = os.Create(walPath)
				if err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
			}
			if _, err := f.Seek(offset, 0); err != nil {
				f.Close()
				http.Error(w, err.Error(), 500)
				return
			}
		}

		if _, err := f.Write(data); err != nil {
			f.Close()
			http.Error(w, err.Error(), 500)
			return
		}
		f.Close()

		log.Printf("WAL received: %d bytes at offset %d", len(data), offset)
		w.WriteHeader(200)
	})

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		fmt.Fprint(w, "ok")
	})

	log.Printf("replica listening on %s", listen)
	if err := http.ListenAndServe(listen, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

// ============================================================
// UTILITIES
// ============================================================

func splitCSV(s string) []string {
	var result []string
	current := ""
	for _, c := range s {
		if c == ',' {
			if current != "" {
				result = append(result, current)
			}
			current = ""
		} else {
			current += string(c)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func fileModTime(path string) time.Time {
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}
	}
	return info.ModTime()
}

// WAL frame structure (for future use — incremental frame shipping)
type walFrame struct {
	pageNo     uint32
	commitSize uint32
	salt1      uint32
	salt2      uint32
	checksum1  uint32
	checksum2  uint32
	pageData   []byte
}

// parseWALFrames parses WAL frames from raw data (for future incremental shipping)
func parseWALFrames(data []byte, pageSize int) []walFrame {
	var frames []walFrame
	offset := walHeaderSize // skip WAL header

	for offset+walFrameHeaderSize+pageSize <= len(data) {
		frame := walFrame{
			pageNo:     binary.BigEndian.Uint32(data[offset:]),
			commitSize: binary.BigEndian.Uint32(data[offset+4:]),
			salt1:      binary.BigEndian.Uint32(data[offset+8:]),
			salt2:      binary.BigEndian.Uint32(data[offset+12:]),
			checksum1:  binary.BigEndian.Uint32(data[offset+16:]),
			checksum2:  binary.BigEndian.Uint32(data[offset+20:]),
			pageData:   data[offset+walFrameHeaderSize : offset+walFrameHeaderSize+pageSize],
		}
		frames = append(frames, frame)
		offset += walFrameHeaderSize + pageSize
	}

	return frames
}
