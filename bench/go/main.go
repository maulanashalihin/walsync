package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"time"
)

// WalChunk protobuf encoding (manual, equivalent to generated code)
// message WalChunk { int64 offset = 1; bytes data = 2; }
func encodeWalChunk(offset int64, data []byte) []byte {
	buf := make([]byte, 0, len(data)+16)
	// field 1: offset (varint tag=0x08, varint value)
	buf = append(buf, 0x08)
	buf = appendVarint(buf, uint64(offset))
	// field 2: data (varint tag=0x12, varint length, raw bytes)
	buf = append(buf, 0x12)
	buf = appendVarint(buf, uint64(len(data)))
	buf = append(buf, data...)
	return buf
}

func appendVarint(buf []byte, v uint64) []byte {
	for v >= 0x80 {
		buf = append(buf, byte(v)|0x80)
		v >>= 7
	}
	return append(buf, byte(v))
}

func gzipCompress(data []byte) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Write(data)
	w.Close()
	return buf.Bytes()
}

func main() {
	walPath := "/tmp/bench-wal.bin"

	// Warmup
	warmupData, _ := os.ReadFile(walPath)
	_ = gzipCompress(warmupData)

	iterations := 1000

	// Benchmark 1: File read
	start := time.Now()
	for i := 0; i < iterations; i++ {
		data, err := os.ReadFile(walPath)
		if err != nil {
			panic(err)
		}
		_ = data
	}
	elapsed := time.Since(start)
	fmt.Printf("GO file_read:    %d iterations, %v total, %.3f ms/op, %.1f MB/s\n",
		iterations, elapsed,
		float64(elapsed.Milliseconds())/float64(iterations),
		float64(iterations)*1.0/elapsed.Seconds())

	// Benchmark 2: File read + gzip compress
	start = time.Now()
	for i := 0; i < iterations; i++ {
		data, err := os.ReadFile(walPath)
		if err != nil {
			panic(err)
		}
		compressed := gzipCompress(data)
		_ = compressed
	}
	elapsed = time.Since(start)
	fmt.Printf("GO read+gzip:    %d iterations, %v total, %.3f ms/op, %.1f MB/s\n",
		iterations, elapsed,
		float64(elapsed.Milliseconds())/float64(iterations),
		float64(iterations)*1.0/elapsed.Seconds())

	// Benchmark 3: File read + protobuf encode + gzip compress (full hot path)
	start = time.Now()
	for i := 0; i < iterations; i++ {
		data, err := os.ReadFile(walPath)
		if err != nil {
			panic(err)
		}
		chunk := encodeWalChunk(0, data)
		compressed := gzipCompress(chunk)
		_ = compressed
	}
	elapsed = time.Since(start)
	fmt.Printf("GO read+proto+gz: %d iterations, %v total, %.3f ms/op, %.1f MB/s\n",
		iterations, elapsed,
		float64(elapsed.Milliseconds())/float64(iterations),
		float64(iterations)*1.0/elapsed.Seconds())

	// Benchmark 4: gzip compress only (data already in memory)
	data, _ := os.ReadFile(walPath)
	start = time.Now()
	for i := 0; i < iterations; i++ {
		compressed := gzipCompress(data)
		_ = compressed
	}
	elapsed = time.Since(start)
	fmt.Printf("GO gzip_only:    %d iterations, %v total, %.3f ms/op, %.1f MB/s\n",
		iterations, elapsed,
		float64(elapsed.Milliseconds())/float64(iterations),
		float64(iterations)*1.0/elapsed.Seconds())

	// Benchmark 5: protobuf encode only
	start = time.Now()
	for i := 0; i < iterations; i++ {
		chunk := encodeWalChunk(0, data)
		_ = chunk
	}
	elapsed = time.Since(start)
	fmt.Printf("GO proto_only:   %d iterations, %v total, %.3f ms/op, %.1f MB/s\n",
		iterations, elapsed,
		float64(elapsed.Milliseconds())/float64(iterations),
		float64(iterations)*1.0/elapsed.Seconds())

	// Report compression ratio
	compressed := gzipCompress(data)
	fmt.Printf("\nCompression: %d → %d bytes (%.1f%% reduction)\n",
		len(data), len(compressed),
		float64(len(compressed))/float64(len(data))*100)
}
