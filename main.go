package main

import (
	"context"
	"encoding/binary"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/maulanashalihin/walsync/proto/walsyncpb"
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
// PRIMARY MODE — gRPC client, ships WAL to replicas
// ============================================================

// replicaConn holds a persistent gRPC connection to one replica
type replicaConn struct {
	addr string
	conn *grpc.ClientConn
	cli  pb.WalSyncClient
}

func runPrimary(dbPath string, replicasCSV string) {
	if replicasCSV == "" {
		log.Fatal("primary mode requires -replicas")
	}

	replicaAddrs := splitCSV(replicasCSV)
	log.Printf("walsync primary starting | db=%s | replicas=%v", dbPath, replicaAddrs)

	walPath := dbPath + "-wal"

	// Establish persistent gRPC connections to all replicas
	conns := make([]*replicaConn, 0, len(replicaAddrs))
	for _, addr := range replicaAddrs {
		// Ensure addr has port (default 9090)
		if !hasPort(addr) {
			addr = addr + ":9090"
		}
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatalf("failed to connect to replica %s: %v", addr, err)
		}
		cli := pb.NewWalSyncClient(conn)
		conns = append(conns, &replicaConn{addr: addr, conn: conn, cli: cli})
		log.Printf("connected to replica %s", addr)
	}
	defer func() {
		for _, rc := range conns {
			rc.conn.Close()
		}
	}()

	// Ship initial snapshot to all replicas
	log.Println("shipping initial snapshot...")
	shipSnapshotGRPC(dbPath, walPath, conns)
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
					shipSnapshotGRPC(dbPath, walPath, conns)
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
				shipSnapshotGRPC(dbPath, walPath, conns)
				lastShippedSize = fileSize(walPath)
				lastShippedDBMod = currentDBMod
			}

		case <-debounceCh:
			currentSize := fileSize(walPath)
			if currentSize <= lastShippedSize {
				continue
			}
			shipWALGRPC(walPath, lastShippedSize, currentSize, conns)
			lastShippedSize = currentSize

		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Printf("watcher error: %v", err)
		}
	}
}

func shipSnapshotGRPC(dbPath, walPath string, conns []*replicaConn) {
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

	snap := &pb.Snapshot{
		DbData:  dbData,
		WalData: walData,
	}

	var wg sync.WaitGroup
	for _, rc := range conns {
		wg.Add(1)
		go func(rc *replicaConn) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			ack, err := rc.cli.ShipSnapshot(ctx, snap)
			if err != nil {
				log.Printf("error sending snapshot to %s: %v", rc.addr, err)
				return
			}
			if !ack.Ok {
				log.Printf("snapshot to %s failed: %s", rc.addr, ack.Error)
				return
			}
			log.Printf("snapshot shipped to %s (%d bytes db, %d bytes wal)", rc.addr, len(dbData), len(walData))
		}(rc)
	}
	wg.Wait()
}

func shipWALGRPC(walPath string, offset, size int64, conns []*replicaConn) {
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

	chunk := &pb.WalChunk{
		Offset: offset,
		Data:   data,
	}

	var wg sync.WaitGroup
	for _, rc := range conns {
		wg.Add(1)
		go func(rc *replicaConn) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			ack, err := rc.cli.ShipWal(ctx, chunk)
			if err != nil {
				log.Printf("error shipping wal to %s: %v", rc.addr, err)
				return
			}
			if !ack.Ok {
				log.Printf("wal ship to %s failed: %s", rc.addr, ack.Error)
			}
		}(rc)
	}
	wg.Wait()

	log.Printf("WAL shipped: %d bytes from offset %d to %d replicas", len(data), offset, len(conns))
}

// ============================================================
// REPLICA MODE — gRPC server, receives WAL from primary
// ============================================================

type replicaServer struct {
	pb.UnimplementedWalSyncServer
	dbPath  string
	walPath string
}

func runReplica(dbPath string, listen string) {
	walPath := dbPath + "-wal"
	log.Printf("walsync replica starting | db=%s | listen=%s", dbPath, listen)

	lis, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", listen, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWalSyncServer(grpcServer, &replicaServer{
		dbPath:  dbPath,
		walPath: walPath,
	})

	log.Printf("replica listening on %s (gRPC)", listen)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func (s *replicaServer) ShipSnapshot(ctx context.Context, snap *pb.Snapshot) (*pb.Ack, error) {
	// Write DB to temp file, then atomically replace
	tmpPath := s.dbPath + ".tmp"
	if err := os.WriteFile(tmpPath, snap.DbData, 0644); err != nil {
		return &pb.Ack{Ok: false, Error: err.Error()}, nil
	}

	// Remove existing WAL (stale)
	os.Remove(s.walPath)
	os.Remove(s.dbPath + "-shm")

	if err := os.Rename(tmpPath, s.dbPath); err != nil {
		return &pb.Ack{Ok: false, Error: err.Error()}, nil
	}

	// Write WAL if provided
	if len(snap.WalData) > 0 {
		if err := os.WriteFile(s.walPath, snap.WalData, 0644); err != nil {
			return &pb.Ack{Ok: false, Error: err.Error()}, nil
		}
	}

	log.Printf("snapshot received: %d bytes db, %d bytes wal", len(snap.DbData), len(snap.WalData))
	return &pb.Ack{Ok: true}, nil
}

func (s *replicaServer) ShipWal(ctx context.Context, chunk *pb.WalChunk) (*pb.Ack, error) {
	if len(chunk.Data) == 0 {
		return &pb.Ack{Ok: true}, nil
	}

	offset := chunk.Offset

	var f *os.File
	var err error
	if offset == 0 {
		// Full WAL replacement
		f, err = os.Create(s.walPath)
		if err != nil {
			return &pb.Ack{Ok: false, Error: err.Error()}, nil
		}
	} else {
		f, err = os.OpenFile(s.walPath, os.O_WRONLY, 0644)
		if err != nil {
			f, err = os.Create(s.walPath)
			if err != nil {
				return &pb.Ack{Ok: false, Error: err.Error()}, nil
			}
		}
		if _, err := f.Seek(offset, 0); err != nil {
			f.Close()
			return &pb.Ack{Ok: false, Error: err.Error()}, nil
		}
	}

	if _, err := f.Write(chunk.Data); err != nil {
		f.Close()
		return &pb.Ack{Ok: false, Error: err.Error()}, nil
	}
	f.Close()

	log.Printf("WAL received: %d bytes at offset %d", len(chunk.Data), offset)
	return &pb.Ack{Ok: true, AppliedOffset: offset + int64(len(chunk.Data))}, nil
}

func (s *replicaServer) Health(ctx context.Context, _ *pb.Empty) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{
		Ok:      true,
		DbSize:  fileSize(s.dbPath),
		WalSize: fileSize(s.walPath),
	}, nil
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
