package main

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/gofiber/fiber/v2"
	"github.com/valyala/fasthttp"
)

// WAL header size
const walHeaderSize = 32

// WAL frame header size
const walFrameHeaderSize = 24

func main() {
	mode := flag.String("mode", "", "primary or replica")
	dbPath := flag.String("db", "", "path to SQLite database file")
	replicas := flag.String("replicas", "", "comma-separated replica addresses (primary mode, e.g. host:port)")
	listen := flag.String("listen", ":9090", "gRPC listen address (replica mode)")
	metricsAddr := flag.String("metrics", "", "HTTP metrics listen address (e.g. :9091, empty = disabled)")
	configPath := flag.String("config", "", "path to TOML config file (CLI flags override config)")
	flag.Parse()

	// Load config file if specified
	cfg := loadConfig(*configPath)

	// CLI flags override config file values
	if *mode == "" {
		*mode = cfg.Mode
	}
	if *dbPath == "" {
		*dbPath = cfg.DBPath
	}
	if *replicas == "" {
		*replicas = cfg.Replicas
	}
	if *listen == ":9090" && cfg.Listen != "" {
		*listen = cfg.Listen
	}
	if *metricsAddr == "" {
		*metricsAddr = cfg.Metrics
	}

	if *mode == "" || *dbPath == "" {
		flag.Usage()
		os.Exit(1)
	}

	// Start metrics server if enabled
	if *metricsAddr != "" {
		go startMetricsServer(*metricsAddr)
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

// config holds values parsed from TOML config file
type config struct {
	Mode     string
	DBPath   string
	Replicas string
	Listen   string
	Metrics  string
}

// loadConfig reads a minimal TOML config file (key = "value" format).
// Returns empty config if path is empty or file doesn't exist.
func loadConfig(path string) *config {
	cfg := &config{}
	if path == "" {
		return cfg
	}
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("error reading config file %s: %v", path, err)
	}
	for _, line := range splitLines(string(data)) {
		line = trimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}
		eq := indexOf(line, '=')
		if eq < 0 {
			continue
		}
		key := trimSpace(line[:eq])
		val := trimSpace(line[eq+1:])
		// Strip quotes
		if len(val) >= 2 && (val[0] == '"' && val[len(val)-1] == '"') {
			val = val[1 : len(val)-1]
		}
		switch key {
		case "mode":
			cfg.Mode = val
		case "db":
			cfg.DBPath = val
		case "replicas":
			cfg.Replicas = val
		case "listen":
			cfg.Listen = val
		case "metrics":
			cfg.Metrics = val
		}
	}
	log.Printf("config loaded from %s", path)
	return cfg
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := range s {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\r') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}

func indexOf(s string, c byte) int {
	for i := range s {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// ============================================================
// PRIMARY MODE — HTTP client (fasthttp), ships WAL to replicas
// ============================================================

// replicaConn holds a persistent fasthttp client to one replica
type replicaConn struct {
	addr   string
	client *fasthttp.Client
}

func newReplicaConn(addr string) *replicaConn {
	return &replicaConn{
		addr: addr,
		client: &fasthttp.Client{
			MaxConnsPerHost:     4,
			MaxIdleConnDuration: 30 * time.Second,
			ReadTimeout:         10 * time.Second,
			WriteTimeout:        10 * time.Second,
		},
	}
}

func runPrimary(dbPath string, replicasCSV string) {
	if replicasCSV == "" {
		log.Fatal("primary mode requires -replicas")
	}

	replicaAddrs := splitCSV(replicasCSV)
	log.Printf("walsync primary starting | db=%s | replicas=%v", dbPath, replicaAddrs)

	walPath := dbPath + "-wal"

	// Create persistent HTTP clients for all replicas
	conns := make([]*replicaConn, 0, len(replicaAddrs))
	for _, addr := range replicaAddrs {
		if !hasPort(addr) {
			addr = addr + ":9090"
		}
		conns = append(conns, newReplicaConn(addr))
		log.Printf("replica client ready: %s", addr)
	}

	// Ship initial snapshot to all replicas
	log.Println("shipping initial snapshot...")
	shipSnapshotHTTP(dbPath, walPath, conns)
	log.Println("initial snapshot shipped")

	// Watch WAL file for changes
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("failed to create watcher: %v", err)
	}
	defer watcher.Close()

	dir := filepath.Dir(dbPath)
	if err := watcher.Add(dir); err != nil {
		log.Fatalf("failed to watch directory %s: %v", dir, err)
	}

	// Track last shipped WAL size and DB mtime
	lastShippedSize := fileSize(walPath)
	lastShippedDBMod := fileModTime(dbPath)
	lastSalt1, lastSalt2 := walSalt(walPath)

	// Debounce: batch rapid WAL changes
	debounceMs := 50 * time.Millisecond
	var debounceTimer *time.Timer
	debounceCh := make(chan struct{}, 1)

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

	// Polling fallback: check WAL size every 50ms
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			if event.Name == walPath && (event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create) {
				if fileSize(walPath) > lastShippedSize {
					scheduleShip()
				}
			}
			if event.Name == dbPath && (event.Op&fsnotify.Write == fsnotify.Write) {
				newWalSize := fileSize(walPath)
				if newWalSize == 0 || newWalSize < lastShippedSize {
					log.Println("checkpoint detected, shipping snapshot...")
					shipSnapshotHTTP(dbPath, walPath, conns)
					lastShippedSize = fileSize(walPath)
					lastShippedDBMod = fileModTime(dbPath)
				}
			}

		case <-ticker.C:
			// Polling: check if WAL grew
			if fileSize(walPath) > lastShippedSize {
				scheduleShip()
			}
			// Check if DB file changed (checkpoint without WAL)
			currentDBMod := fileModTime(dbPath)
			if currentDBMod != lastShippedDBMod {
				log.Printf("DB file modified, shipping snapshot...")
				shipSnapshotHTTP(dbPath, walPath, conns)
				lastShippedSize = fileSize(walPath)
				lastShippedDBMod = currentDBMod
				lastSalt1, lastSalt2 = walSalt(walPath)
			}

			// Check if WAL salt changed (WAL recreated with different salt)
			curSalt1, curSalt2 := walSalt(walPath)
			if curSalt1 != lastSalt1 || curSalt2 != lastSalt2 {
				if curSalt1 != 0 { // skip if WAL doesn't exist
					log.Printf("WAL salt changed, shipping snapshot...")
					shipSnapshotHTTP(dbPath, walPath, conns)
					lastShippedSize = fileSize(walPath)
					lastSalt1, lastSalt2 = curSalt1, curSalt2
				}
			}

		case <-debounceCh:
			currentSize := fileSize(walPath)
			if currentSize <= lastShippedSize {
				continue
			}
			shipWALHTTP(walPath, lastShippedSize, currentSize, conns)
			lastShippedSize = currentSize

		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Printf("watcher error: %v", err)
		}
	}
}

// shipSnapshotHTTP ships full DB+WAL snapshot to all replicas via HTTP.
// Body format: 8-byte big-endian db_len + db_data + wal_data, gzip compressed.
func shipSnapshotHTTP(dbPath, walPath string, conns []*replicaConn) {
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

	// Pack: 8-byte big-endian db_len + db_data + wal_data
	packed := make([]byte, 8+len(dbData)+len(walData))
	binary.BigEndian.PutUint64(packed[:8], uint64(len(dbData)))
	copy(packed[8:], dbData)
	copy(packed[8+len(dbData):], walData)

	// Gzip compress
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	gw.Write(packed)
	gw.Close()
	compressed := buf.Bytes()

	var wg sync.WaitGroup
	for _, rc := range conns {
		wg.Add(1)
		go func(rc *replicaConn) {
			defer wg.Done()

			url := "http://" + rc.addr + "/ship-snapshot"
			req := fasthttp.AcquireRequest()
			res := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseRequest(req)
			defer fasthttp.ReleaseResponse(res)

			req.SetRequestURI(url)
			req.Header.SetMethod("POST")
			req.Header.Set("Content-Type", "application/octet-stream")
			req.Header.Set("Content-Encoding", "gzip")
			req.SetBody(compressed)

			if err := rc.client.DoTimeout(req, res, 10*time.Second); err != nil {
				log.Printf("error sending snapshot to %s: %v, retrying...", rc.addr, err)
				// Retry once
				req.Reset()
				res.Reset()
				req.SetRequestURI(url)
				req.Header.SetMethod("POST")
				req.Header.Set("Content-Type", "application/octet-stream")
				req.Header.Set("Content-Encoding", "gzip")
				req.SetBody(compressed)
				if err := rc.client.DoTimeout(req, res, 10*time.Second); err != nil {
					log.Printf("retry snapshot to %s failed: %v", rc.addr, err)
					metricsIncSnapshotError()
					return
				}
			}

			if res.StatusCode() != 200 {
				log.Printf("snapshot to %s failed: HTTP %d", rc.addr, res.StatusCode())
				metricsIncSnapshotError()
				return
			}
			log.Printf("snapshot shipped to %s (%d bytes db, %d bytes wal)", rc.addr, len(dbData), len(walData))
		}(rc)
	}
	wg.Wait()
	metricsIncSnapshot(int64(len(dbData) + len(walData)))
}

// shipWALHTTP ships incremental WAL bytes to all replicas via HTTP.
// Body: gzip-compressed WAL bytes from offset.
func shipWALHTTP(walPath string, offset, size int64, conns []*replicaConn) {
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

	// Gzip compress
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	gw.Write(data)
	gw.Close()
	compressed := buf.Bytes()

	offsetStr := strconv.FormatInt(offset, 10)

	var wg sync.WaitGroup
	for _, rc := range conns {
		wg.Add(1)
		go func(rc *replicaConn) {
			defer wg.Done()

			url := "http://" + rc.addr + "/ship-wal"
			req := fasthttp.AcquireRequest()
			res := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseRequest(req)
			defer fasthttp.ReleaseResponse(res)

			req.SetRequestURI(url)
			req.Header.SetMethod("POST")
			req.Header.Set("Content-Type", "application/octet-stream")
			req.Header.Set("Content-Encoding", "gzip")
			req.Header.Set("X-Walsync-Offset", offsetStr)
			req.SetBody(compressed)

			if err := rc.client.DoTimeout(req, res, 5*time.Second); err != nil {
				log.Printf("error shipping wal to %s: %v, retrying...", rc.addr, err)
				// Retry once
				req.Reset()
				res.Reset()
				req.SetRequestURI(url)
				req.Header.SetMethod("POST")
				req.Header.Set("Content-Type", "application/octet-stream")
				req.Header.Set("Content-Encoding", "gzip")
				req.Header.Set("X-Walsync-Offset", offsetStr)
				req.SetBody(compressed)
				if err := rc.client.DoTimeout(req, res, 5*time.Second); err != nil {
					log.Printf("retry wal ship to %s failed: %v", rc.addr, err)
					metricsIncWalError()
					return
				}
			}

			if res.StatusCode() != 200 {
				log.Printf("wal ship to %s failed: HTTP %d", rc.addr, res.StatusCode())
				metricsIncWalError()
			}
		}(rc)
	}
	wg.Wait()

	log.Printf("WAL shipped: %d bytes (compressed %d) from offset %d to %d replicas", len(data), len(compressed), offset, len(conns))
	metricsIncWalShips(int64(len(data)))
}

// ============================================================
// REPLICA MODE — Fiber HTTP server, receives WAL from primary
// ============================================================

func runReplica(dbPath string, listen string) {
	walPath := dbPath + "-wal"
	log.Printf("walsync replica starting | db=%s | listen=%s", dbPath, listen)

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		BodyLimit:             16 * 1024 * 1024,
	})

	// POST /ship-wal — receive incremental WAL bytes
	app.Post("/ship-wal", func(c *fiber.Ctx) error {
		// Fiber auto-decompresses gzip body based on Content-Encoding header
		data := c.Body()
		if len(data) == 0 {
			return c.JSON(fiber.Map{"ok": true})
		}


		offset, _ := strconv.ParseInt(string(c.Get("X-Walsync-Offset")), 10, 64)

		var f *os.File
		var err error
		if offset == 0 {
			f, err = os.Create(walPath)
		} else {
			f, err = os.OpenFile(walPath, os.O_WRONLY, 0644)
			if err != nil {
				f, err = os.Create(walPath)
			}
			if err == nil {
				if _, err = f.Seek(offset, 0); err != nil {
					f.Close()
					return c.JSON(fiber.Map{"ok": false, "error": err.Error()})
				}
			}
		}
		if err != nil {
			return c.JSON(fiber.Map{"ok": false, "error": err.Error()})
		}

		if _, err := f.Write(data); err != nil {
			f.Close()
			return c.JSON(fiber.Map{"ok": false, "error": err.Error()})
		}
		f.Close()

		log.Printf("WAL received: %d bytes at offset %d", len(data), offset)
		return c.JSON(fiber.Map{"ok": true, "applied_offset": offset + int64(len(data))})
	})

	// POST /ship-snapshot — receive full DB+WAL snapshot
	app.Post("/ship-snapshot", func(c *fiber.Ctx) error {
		// Fiber auto-decompresses gzip body based on Content-Encoding header
		data := c.Body()

		// Unpack: 8-byte big-endian db_len + db_data + wal_data
		if len(data) < 8 {
			return c.JSON(fiber.Map{"ok": false, "error": "snapshot too small"})
		}
		dbLen := binary.BigEndian.Uint64(data[:8])
		if uint64(len(data)) < 8+dbLen {
			return c.JSON(fiber.Map{"ok": false, "error": "snapshot truncated"})
		}
		dbData := data[8 : 8+dbLen]
		walData := data[8+dbLen:]

		// Atomic replace
		tmpPath := dbPath + ".tmp"
		if err := os.WriteFile(tmpPath, dbData, 0644); err != nil {
			return c.JSON(fiber.Map{"ok": false, "error": err.Error()})
		}
		os.Remove(walPath)
		os.Remove(dbPath + "-shm")
		if err := os.Rename(tmpPath, dbPath); err != nil {
			return c.JSON(fiber.Map{"ok": false, "error": err.Error()})
		}
		if len(walData) > 0 {
			if err := os.WriteFile(walPath, walData, 0644); err != nil {
				return c.JSON(fiber.Map{"ok": false, "error": err.Error()})
			}
		}

		log.Printf("snapshot received: %d bytes db, %d bytes wal", len(dbData), len(walData))
		return c.JSON(fiber.Map{"ok": true})
	})

	// GET /health — health check
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"ok":      true,
			"db_size": fileSize(dbPath),
			"wal_size": fileSize(walPath),
		})
	})

	log.Printf("replica listening on %s (HTTP/Fiber)", listen)
	if err := app.Listen(listen); err != nil {
		log.Fatalf("fiber listen: %v", err)
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

// walSalt reads the salt values from the WAL header (bytes 16-23).
// Salt changes when SQLite recreates the WAL file (e.g. after checkpoint).
// Returns (0, 0) if WAL file doesn't exist.
func walSalt(walPath string) (uint32, uint32) {
	f, err := os.Open(walPath)
	if err != nil {
		return 0, 0
	}
	defer f.Close()
	header := make([]byte, walHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return 0, 0
	}
	salt1 := binary.BigEndian.Uint32(header[16:])
	salt2 := binary.BigEndian.Uint32(header[20:])
	return salt1, salt2
}

func hasPort(addr string) bool {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return true
		}
		if addr[i] < '0' || addr[i] > '9' {
			if addr[i] == ']' {
				return true // IPv6 bracket
			}
			return false
		}
	}
	return false
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

func parseWALFrames(data []byte, pageSize int) []walFrame {
	var frames []walFrame
	offset := walHeaderSize

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

// ============================================================
// METRICS — Prometheus-compatible HTTP endpoint
// ============================================================

var (
	metricsMu             sync.Mutex
	metricsWalShips       int64
	metricsWalBytes       int64
	metricsWalErrors      int64
	metricsSnapshots      int64
	metricsSnapshotBytes  int64
	metricsSnapshotErrors int64
	metricsLastShipUnix   int64
)

func metricsIncWalShips(bytes int64) {
	metricsMu.Lock()
	metricsWalShips++
	metricsWalBytes += bytes
	metricsLastShipUnix = time.Now().Unix()
	metricsMu.Unlock()
}

func metricsIncWalError() {
	metricsMu.Lock()
	metricsWalErrors++
	metricsMu.Unlock()
}

func metricsIncSnapshot(bytes int64) {
	metricsMu.Lock()
	metricsSnapshots++
	metricsSnapshotBytes += bytes
	metricsLastShipUnix = time.Now().Unix()
	metricsMu.Unlock()
}

func metricsIncSnapshotError() {
	metricsMu.Lock()
	metricsSnapshotErrors++
	metricsMu.Unlock()
}

func startMetricsServer(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metricsMu.Lock()
		walShips := metricsWalShips
		walBytes := metricsWalBytes
		walErrors := metricsWalErrors
		snaps := metricsSnapshots
		snapBytes := metricsSnapshotBytes
		snapErrors := metricsSnapshotErrors
		lastShip := metricsLastShipUnix
		metricsMu.Unlock()

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# walsync metrics\n")
		fmt.Fprintf(w, "walsync_wal_ships_total %d\n", walShips)
		fmt.Fprintf(w, "walsync_wal_shipped_bytes_total %d\n", walBytes)
		fmt.Fprintf(w, "walsync_wal_ship_errors_total %d\n", walErrors)
		fmt.Fprintf(w, "walsync_snapshot_ships_total %d\n", snaps)
		fmt.Fprintf(w, "walsync_snapshot_shipped_bytes_total %d\n", snapBytes)
		fmt.Fprintf(w, "walsync_snapshot_ship_errors_total %d\n", snapErrors)
		fmt.Fprintf(w, "walsync_last_ship_timestamp_seconds %d\n", lastShip)
	})

	log.Printf("metrics server listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("metrics server error: %v", err)
	}
}
