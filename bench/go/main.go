// Benchmark: gRPC vs HTTP/Fiber for WAL shipping.
//
// Measures round-trip latency + throughput for shipping raw byte payloads
// (the core operation walsync performs: ShipWal).
//
// Fairness controls:
//   - Same payload data for both transports
//   - Persistent connections for both (gRPC HTTP/2, fasthttp reuse)
//   - Same server-side work (write payload to temp file, return ack)
//   - Warmup before measurement
//   - gzip tested for both (gRPC has it built-in via UseCompressor)
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/valyala/fasthttp"

	pb "github.com/maulanashalihin/walsync/proto/walsyncpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcgzip "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
)

const iterations = 200

type payload struct {
	name string
	size int
	data []byte
}

func makePayloads() []payload {
	sizes := []int{4096, 65536, 1048576, 4194304}
	out := make([]payload, 0, len(sizes)*2)

	for _, sz := range sizes {
		// Incompressible: random bytes (worst case for gzip)
		random := make([]byte, sz)
		rand.Read(random)
		out = append(out, payload{
			name: fmt.Sprintf("random-%dKB", sz/1024),
			size: sz,
		data: random,
		})

		// Compressible: SQLite-page-like (mostly zeros with some structure)
		// SQLite pages are 4096 bytes, mostly structured data with repetition
		sqlite := make([]byte, sz)
		// Fill with page-like pattern: header + mostly zeros
		for i := 0; i < sz; i += 4096 {
			end := i + 4096
			if end > sz {
				end = sz
			}
			// Page header: 100 bytes of structure
			for j := i; j < i+100 && j < end; j++ {
				sqlite[j] = byte(j % 256)
			}
			// Rest is zeros (unallocated pages)
		}
		out = append(out, payload{
			name: fmt.Sprintf("sqlite-%dKB", sz/1024),
			size: sz,
			data: sqlite,
		})
	}

	return out
}

// ============================================================
// gRPC server (minimal — same shape as walsync replica)
// ============================================================

type grpcReplica struct {
	pb.UnimplementedWalSyncServer
	walPath string
}

func (s *grpcReplica) ShipWal(ctx context.Context, chunk *pb.WalChunk) (*pb.Ack, error) {
	if len(chunk.Data) == 0 {
		return &pb.Ack{Ok: true}, nil
	}
	// Write to temp file (same I/O as real replica)
	if err := os.WriteFile(s.walPath, chunk.Data, 0644); err != nil {
		return &pb.Ack{Ok: false, Error: err.Error()}, nil
	}
	return &pb.Ack{Ok: true, AppliedOffset: chunk.Offset + int64(len(chunk.Data))}, nil
}

func (s *grpcReplica) ShipSnapshot(ctx context.Context, snap *pb.Snapshot) (*pb.Ack, error) {
	return &pb.Ack{Ok: true}, nil
}

func (s *grpcReplica) Health(ctx context.Context, _ *pb.Empty) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{Ok: true}, nil
}

// ============================================================
// Fiber HTTP server (equivalent endpoint)
// ============================================================

func startFiberServer(addr, walPath string) *fiber.App {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		// Match fasthttp defaults — no body limit increase needed for 4MB
		BodyLimit: 10 * 1024 * 1024,
	})

	app.Post("/shipwal", func(c *fiber.Ctx) error {
		data := c.Body()
		if len(data) == 0 {
			return c.JSON(fiber.Map{"ok": true})
		}
		if err := os.WriteFile(walPath, data, 0644); err != nil {
			return c.JSON(fiber.Map{"ok": false, "error": err.Error()})
		}
		return c.JSON(fiber.Map{"ok": true})
	})

	app.Post("/shipwal-gzip", func(c *fiber.Ctx) error {
		// Decompress gzip body
		r, err := gzip.NewReader(bytes.NewReader(c.Body()))
		if err != nil {
			return c.JSON(fiber.Map{"ok": false, "error": err.Error()})
		}
		data, err := io.ReadAll(r)
		if err != nil {
			return c.JSON(fiber.Map{"ok": false, "error": err.Error()})
		}
		r.Close()
		if len(data) == 0 {
			return c.JSON(fiber.Map{"ok": true})
		}
		if err := os.WriteFile(walPath, data, 0644); err != nil {
			return c.JSON(fiber.Map{"ok": false, "error": err.Error()})
		}
		return c.JSON(fiber.Map{"ok": true})
	})

	go func() {
		if err := app.Listen(addr); err != nil {
			log.Fatalf("fiber listen: %v", err)
		}
	}()

	return app
}

// ============================================================
// Benchmark helpers
// ============================================================

type result struct {
	transport string
	payload   string
	gzip      bool
	totalMs   float64
	msPerOp   float64
	mbPerSec  float64
}

func (r result) String() string {
	gz := ""
	if r.gzip {
		gz = "+gzip"
	}
	return fmt.Sprintf("%-12s %-16s %6.1f ms total  %6.3f ms/op  %7.1f MB/s",
		r.transport+gz, r.payload, r.totalMs, r.msPerOp, r.mbPerSec)
}

func benchGRPC(addr string, p payload, useGzip bool) result {
	// Connect
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                10 * time.Second,
		Timeout:             5 * time.Second,
		PermitWithoutStream: true,
	}))
	opts = append(opts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(16*1024*1024),
		grpc.MaxCallSendMsgSize(16*1024*1024),
	))
	if useGzip {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor(grpcgzip.Name)))
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		log.Fatalf("grpc dial: %v", err)
	}
	defer conn.Close()
	cli := pb.NewWalSyncClient(conn)

	chunk := &pb.WalChunk{Offset: 0, Data: p.data}

	// Warmup
	for range 10 {
		_, err := cli.ShipWal(context.Background(), chunk)
		if err != nil {
			log.Fatalf("grpc warmup: %v", err)
		}
	}

	// Benchmark
	start := time.Now()
	for range iterations {
		_, err := cli.ShipWal(context.Background(), chunk)
		if err != nil {
			log.Fatalf("grpc ship: %v", err)
		}
	}
	elapsed := time.Since(start)

	totalMs := float64(elapsed.Milliseconds())
	return result{
		transport: "gRPC",
		payload:   p.name,
		gzip:      useGzip,
		totalMs:   totalMs,
		msPerOp:   totalMs / float64(iterations),
		mbPerSec:  float64(iterations*p.size) / totalMs / 1024 / 1024 * 1000,
	}
}

func benchFiber(addr string, p payload, useGzip bool) result {
	client := &fasthttp.Client{
		MaxConnsPerHost: 2, // persistent connection
	}

	url := "http://" + addr + "/shipwal"
	var body []byte
	if useGzip {
		url = "http://" + addr + "/shipwal-gzip"
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		gw.Write(p.data)
		gw.Close()
		body = buf.Bytes()
	} else {
		body = p.data
	}

	req := fasthttp.AcquireRequest()
	res := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(res)

	// Warmup
	for range 10 {
		req.SetRequestURI(url)
		req.Header.SetMethod("POST")
		req.SetBody(body)
		if err := client.DoTimeout(req, res, 10*time.Second); err != nil {
			log.Fatalf("fiber warmup: %v", err)
		}
		res.Reset()
		req.Reset()
	}

	// Benchmark
	start := time.Now()
	for range iterations {
		req.SetRequestURI(url)
		req.Header.SetMethod("POST")
		req.SetBody(body)
		if err := client.DoTimeout(req, res, 10*time.Second); err != nil {
			log.Fatalf("fiber ship: %v", err)
		}
		res.Reset()
		req.Reset()
	}
	elapsed := time.Since(start)

	totalMs := float64(elapsed.Milliseconds())
	return result{
		transport: "Fiber",
		payload:   p.name,
		gzip:      useGzip,
		totalMs:   totalMs,
		msPerOp:   totalMs / float64(iterations),
		mbPerSec:  float64(iterations*p.size) / totalMs / 1024 / 1024 * 1000,
	}
}

func main() {
	payloads := makePayloads()

	// Create temp WAL files
	grpcWal, _ := os.CreateTemp("", "bench-grpc-*.wal")
	grpcWalPath := grpcWal.Name()
	grpcWal.Close()
	defer os.Remove(grpcWalPath)

	fiberWal, _ := os.CreateTemp("", "bench-fiber-*.wal")
	fiberWalPath := fiberWal.Name()
	fiberWal.Close()
	defer os.Remove(fiberWalPath)

	// Start gRPC server
	grpcAddr := "127.0.0.1:9201"
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("grpc listen: %v", err)
	}
	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.MaxRecvMsgSize(16*1024*1024),
		grpc.MaxSendMsgSize(16*1024*1024),
	)
	pb.RegisterWalSyncServer(grpcServer, &grpcReplica{walPath: grpcWalPath})
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	// Start Fiber server
	fiberAddr := "127.0.0.1:9202"
	fiberApp := startFiberServer(fiberAddr, fiberWalPath)
	defer fiberApp.Shutdown()

	// Wait for servers to be ready
	time.Sleep(500 * time.Millisecond)

	fmt.Printf("iterations: %d per test, localhost, payload variants: random + sqlite-like\n\n", iterations)

	var results []result

	for _, p := range payloads {
		// gRPC raw (no gzip)
		r := benchGRPC(grpcAddr, p, false)
		results = append(results, r)
		fmt.Println(r)

		// gRPC + gzip
		r = benchGRPC(grpcAddr, p, true)
		results = append(results, r)
		fmt.Println(r)

		// Fiber raw
		r = benchFiber(fiberAddr, p, false)
		results = append(results, r)
		fmt.Println(r)

		// Fiber + gzip
		r = benchFiber(fiberAddr, p, true)
		results = append(results, r)
		fmt.Println(r)

		fmt.Println()
	}

	// Summary table
	fmt.Println("\n=== Summary ===")
	fmt.Printf("%-16s %-16s %8s %8s %8s %8s\n",
		"payload", "transport", "ms/op", "MB/s", "ms/op", "MB/s")
	fmt.Printf("%-16s %-16s %8s %8s %8s %8s\n",
		"", "", "gRPC", "gRPC", "Fiber", "Fiber")
	fmt.Println()

	for _, p := range payloads {
		var grpcRes, fiberRes result
		for _, r := range results {
			if r.payload == p.name && !r.gzip {
				if r.transport == "gRPC" {
					grpcRes = r
				} else {
					fiberRes = r
				}
			}
		}
		fmt.Printf("%-16s %-16s %8.3f %8.1f %8.3f %8.1f\n",
			p.name, "raw",
			grpcRes.msPerOp, grpcRes.mbPerSec,
			fiberRes.msPerOp, fiberRes.mbPerSec)

		for _, r := range results {
			if r.payload == p.name && r.gzip {
				if r.transport == "gRPC" {
					grpcRes = r
				} else {
					fiberRes = r
				}
			}
		}
		fmt.Printf("%-16s %-16s %8.3f %8.1f %8.3f %8.1f\n",
			p.name, "+gzip",
			grpcRes.msPerOp, grpcRes.mbPerSec,
			fiberRes.msPerOp, fiberRes.mbPerSec)
	}

}
