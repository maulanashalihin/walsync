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

---

# v0.6.0 Research: Multi-Writer Support

## Problem

Current walsync is single-writer (primary only, replicas read-only). Can we support multiple writers — two or more nodes writing concurrently?

## Approach 1: Bi-directional WAL shipping — FAILED

**Test**: Both nodes run as primary+replica, ship WAL to each other.

**Result**: Both nodes overwrite each other's DB files via snapshot. Data loss. No conflict detection.

**Root cause**: WAL is page-level, not row-level. Two nodes writing to different rows may modify the same page (index, b-tree, page 1 header). Page-level changes from different nodes cannot be merged — each node's WAL has different salt, different page structure.

**Conclusion**: WAL page-level shipping fundamentally cannot support multi-write. This is the same limitation that caused #3 (page-level shipping corruption) to fail.

## Approach 2: Trigger-based CDC + LWW — WORKS

**Architecture**: Row-level logical replication using SQLite triggers.

### How it works

1. **CDC tables**: Each node has `_cdc_changes` table + `_cdc_sync_flag` table
2. **Triggers**: AFTER INSERT/UPDATE/DELETE triggers capture row-level changes into `_cdc_changes` as JSON
3. **Sync flag**: `WHEN (SELECT value FROM _cdc_sync_flag) = 0` — triggers skip during remote change application (prevents infinite loop)
4. **Ship changes**: Node ships unsynced `_cdc_changes` rows to peers via gRPC
5. **Apply with LWW**: Receiver compares `timestamp` — if remote is newer, apply; else skip
6. **Mark synced**: After shipping, mark changes as `synced = 1`

### Test results

| Scenario | Result |
----------|--------|
| Bi-directional INSERT (UUID PKs) | ✅ Both nodes converge |
| Same row, different timestamps (LWW) | ✅ Newer version wins |
| UPDATE + INSERT concurrent | ✅ Both applied correctly |
| Continuous sync (round 2) | ✅ Converged, no loop |
| Auto-increment PK collision | ❌ Use UUID to avoid |

### Requirements

- **UUID primary keys**: Auto-increment causes PK collision across nodes
- **`updated_at` column**: Required for LWW comparison (timestamp)
- **CDC schema setup**: Triggers + `_cdc_changes` + `_cdc_sync_flag` per table
- **`INSERT OR REPLACE`**: For idempotent apply

### Limitations

- **LWW only**: No CRDT, no custom merge. Last write wins by timestamp.
- **No transaction support**: Each row change is independent (no multi-row atomicity)
- **Schema modification**: User must add triggers + CDC tables
- **Column-level**: Currently captures all columns as JSON blob. No column-level CRDT.
- **Clock skew**: LWW depends on synchronized clocks. HLC (Hybrid Logical Clock) would be more robust.

### Comparison with other tools

| Tool | Approach | Multi-write | Conflict resolution |
------|----------|-------------|-------------------|
| walsync (current) | WAL page-level | ❌ Single-writer | N/A |
| walsync (proposed) | Trigger CDC + LWW | ✅ | LWW by timestamp |
| Marmot | CDC + 2PC (preupdate hook) | ✅ | 2PC + LWW (HLC) |
| cr-sqlite | CRDT extension | ✅ | CRDT (LWW, counters, MV-register) |
| LiteFS | LTX + FUSE | ❌ Single-primary | Failover only |
| dqlite | Raft consensus | ❌ Single-leader | Raft |

### Implementation plan for walsync

1. New mode: `-mode multi` (bi-directional CDC sync)
2. New gRPC RPC: `ShipChanges(ChangeSet)` — ship row-level changes
3. CDC schema auto-setup: walsync creates triggers + CDC tables on startup
4. Bi-directional: each node ships changes to all peers
5. LWW apply: compare `updated_at` timestamp
6. Polling: check `_cdc_changes` for unsynced rows every 50ms (reuse existing debounce)

### What would NOT change

- Existing `-mode primary` / `-mode replica` (WAL shipping) stays as-is
- gRPC transport, compression, keepalive, reconnect — all reused
- Metrics, config file — all reused

### Estimated effort

Medium-high. New proto message + new RPC + CDC schema management + LWW apply logic + bi-directional sync loop. ~300 lines of new code. Existing infrastructure (gRPC, reconnect, metrics) reused.

---

# v0.6.0 Research: gRPC → Fiber (fasthttp) Transport

## Problem

gRPC adds overhead that is pure waste for walsync: protobuf encode/decode, HTTP/2 framing, trailing headers, status codes. walsync ships raw bytes — no streaming, no auth, no interceptors, no type safety needed.

## Benchmark (localhost, 200 iterations, Go)

| Payload | gRPC raw | Fiber raw | gRPC+gzip | Fiber+gzip | Fiber advantage |
|---------|---------|-----------|-----------|------------|-----------------|
| 4KB sqlite | 0.075ms | 0.055ms | 0.133ms | 0.060ms | **2.2x** (gzip) |
| 64KB sqlite | 0.200ms | 0.153ms | 0.295ms | 0.170ms | **1.7x** |
| 1MB sqlite | 1.168ms | 0.793ms | 2.473ms | 1.168ms | **2.1x** |
| 4MB random | 4.180ms | 3.915ms | 42.580ms | 4.865ms | **8.7x** |

**Key finding**: gRPC gzip is pathologically slow for large payloads — 10x slower than raw. gRPC creates a new gzip compressor per call. Fiber uses `compress/gzip` with manual control, no pathology.

## Why Fiber wins

| gRPC overhead | walsync needs it? |
|---|---|
| Protobuf encode/decode | No — ships raw bytes |
| HTTP/2 framing | No — 3 endpoints, no multiplexing |
| gRPC gzip (new compressor per call) | No — pathologically slow |
| Trailing headers + status codes | No — simple `{ok: true}` |
| gRPC interceptors/auth | No — no auth, no middleware |
| Bidirectional streaming | No — unary calls only |

## What changed

- Replica: `grpc.Server` → `fiber.New()` (3 routes: `/ship-wal`, `/ship-snapshot`, `/health`)
- Primary: `grpc.ClientConn` → `fasthttp.Client` (persistent connections)
- Removed: `proto/` directory, gRPC + protobuf dependencies
- Fiber `c.Body()` auto-decompresses gzip request bodies (no manual decompression needed)

## A/B test (2 Singapore VPS, 1000 rows)

| Metric | v0.5.0 (gRPC) | v0.6.0 (Fiber) |
|--------|---------------|----------------|
| Replication | 1001/1001 ✅ | 1001/1001 ✅ |
| Binary size | 15.3MB | 11.7MB (24% smaller) |
| Write time | 9ms | 7ms |
| Sync latency | ~5s | ~5s (polling-bound) |
| Reconnect | ✅ | ✅ (fasthttp auto-reconnect + retry) |

## Conclusion

Fiber (fasthttp) is the correct transport for walsync. Unlike language swaps (Rust, Bun) which don't help because walsync is I/O-bound, transport swap changes the I/O path itself — less overhead per ship, smaller binary, fewer dependencies. All v0.5.0 features preserved.
