# v0.3.0 Research: Performance Optimizations

## Findings

### 1. gRPC Compression — 94-98% bandwidth reduction

**gRPC built-in gzip**: `grpc.UseCompressor(gzip.Name)` — 1 line per RPC call.

**Compression ratios measured on real SQLite data:**

| Data | Raw | Gzip | Reduction |
|------|----:|-----:|----------:|
| DB file (8KB, 18 rows) | 8,192 | 482 | 94% |
| DB file (4KB, 5 rows) | 4,096 | 102 | 97% |
| WAL file (28KB, 5 writes) | 28,872 | 608 | 98% |
| WAL chunk (4KB, 1 write) | 4,120 | ~200 | 95% |

SQLite pages are mostly null bytes → compress extremely well.

**Implementation:**
```go
import _ "google.golang.org/grpc/encoding/gzip" // register compressor

// Per-call:
resp, err := cli.ShipWal(ctx, chunk, grpc.UseCompressor(gzip.Name))

// Or per-connection (all calls):
// Use grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name))
```

**zstd alternative**: `github.com/mostynb/go-grpc-compression/zstd` — registers zstd as gRPC compressor. zstd is ~2x faster than gzip at similar ratio. But adds dependency. gzip is built-in.

**Recommendation**: Start with gzip (built-in, zero dep). If CPU becomes bottleneck, switch to zstd.

### 2. gRPC Keepalive — fast connection failure detection

**Current problem**: If replica crashes, primary doesn't know until next ship attempt fails (could be minutes if no writes).

**Implementation:**
```go
// Client (primary):
grpc.WithKeepaliveParams(keepalive.ClientParameters{
    Time:                10 * time.Second, // ping every 10s
    Timeout:             5 * time.Second,  // wait 5s for pong
    PermitWithoutStream: true,             // ping even when idle
})

// Server (replica):
grpc.KeepaliveParams(keepalive.ServerParameters{
    Time:    30 * time.Second, // ping every 30s
    Timeout: 10 * time.Second, // wait 10s for pong
})
grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
    MinTime:             5 * time.Second,  // min ping interval
    PermitWithoutStream: true,             // allow pings when idle
})
```

**Recommendation**: Add to both client and server. ~10 lines total.

### 3. Page-Level WAL Shipping — avoid full snapshot on checkpoint

**Current problem**: When SQLite checkpoints (WAL → DB), walsync ships entire DB file. For 1GB DB, that's 1GB per checkpoint.

**Solution**: Parse WAL frames, track which pages changed, ship only changed pages.

**SQLite WAL frame format** (from sqlite.org/fileformat.html):
```
WAL header (32 bytes):
  0-3:   Magic (0x377f0682 or 0x377f0683)
  4-7:   File format version (3007000)
  8-11:  Page size
  12-15: Checkpoint sequence number
  16-19: Salt-1
  20-23: Salt-2
  24-27: Checksum-1
  28-31: Checksum-2

WAL frame (24 bytes header + page_size data):
  0-3:   Page number (big-endian)
  4-7:   New DB size in pages (non-zero = commit frame)
  8-11:  Salt-1 (must match WAL header)
  12-15: Salt-2 (must match WAL header)
  16-19: Checksum-1
  20-23: Checksum-2
  24+:   Page data (page_size bytes)
```

**Implementation plan:**
1. Parse WAL frames to extract (page_number, page_data) pairs
2. Track page versions: `map[uint32]int64` (page_no → last_shipped_frame_index)
3. On checkpoint: instead of shipping full DB, ship only pages that changed
4. Replica: write pages directly to DB file at correct offset (page_no * page_size)

**Challenge**: Applying individual pages to DB file requires:
- Knowing page_size (from WAL header)
- Seeking to `page_no * page_size` in DB file
- Writing page data
- This bypasses SQLite's WAL mechanism — direct file manipulation

**Risk**: If app has DB open (WAL mode), writing to DB file directly may conflict with SQLite's page cache. Need to coordinate:
- Option A: Pause app writes during page apply (lock file)
- Option B: Apply pages to a copy, then atomic rename
- Option C: Use SQLite's `sqlite3_deserialize` (C API, not available in Go without CGo)

**Recommendation**: Defer to v0.3.0 or v0.4.0. Complex but high impact for large DBs. Start with compression + keepalive first (easy wins).

### 4. Linux inotify Optimization — skip redundant polling

**Current**: fsnotify + polling 50ms both run simultaneously. On Linux, fsnotify uses inotify (instant). Polling is redundant.

**Implementation:**
```go
// Only poll on macOS (FSEvents unreliable for file writes)
runtime.GOOS == "darwin"
```

Or: detect if fsnotify event fired recently, skip polling for that cycle.

**Recommendation**: Simple check, ~5 lines. Low priority — polling 50ms uses negligible CPU.

## Priority for v0.3.0

| Optimization | Impact | Effort | Priority |
|---|---|---|---|
| gRPC gzip compression | 🔴 94-98% bandwidth | 2 lines | P0 |
| gRPC keepalive | 🟡 fast failure detection | 10 lines | P0 |
| Page-level shipping | 🔴 avoid full snapshot | medium-hard | P1 |
| Skip polling on Linux | 🟢 negligible CPU | 5 lines | P2 |
