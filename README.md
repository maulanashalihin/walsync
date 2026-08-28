# walsync

Live SQLite WAL shipping replication. Write to local SQLite at native speed, sync to replica servers automatically via HTTP.

## Why?

Every existing SQLite replication tool forces a tradeoff:

| Tool | Read | Write | Multi-server | Catch |
|------|-----:|------:|:---:|-------|
| Native SQLite | 221K QPS | 94K QPS | ❌ | No replication |
| Litestream | 221K QPS | 94K QPS | ❌ | Backup to S3 only, not live |
| LiteFS | 220K QPS | 6K QPS | ✅ | FUSE intercepts every write (fsync per write) |
| Marmot | 16K QPS | 17K QPS | ✅ | TCP server, not embedded (14x slower read) |
| dqlite | ~50K QPS | ~20K QPS | ✅ | Custom C API, not drop-in SQLite |

**walsync** takes a different approach: the app uses native embedded SQLite (zero overhead), and a separate background process ships WAL changes to replicas asynchronously.

```
Result: 221K read QPS + 94K write QPS + live multi-server replication
```

The tradeoff: eventual consistency (~1-3s sync delay) and single-writer (primary only).

## How it works

```
Primary server:
  App → SQLite (embedded) → WAL file → disk
                                    ↓
  walsync (background):           (auto-detect)
    1. Watch WAL file changes (fsnotify + polling fallback)
    2. Ship WAL data to replicas via HTTP
    3. On checkpoint: ship full DB snapshot

Replica server:
  walsync (background):
    1. HTTP endpoint: receive WAL + snapshot
    2. Write to local SQLite DB/WAL files
    3. App reads from local SQLite (embedded, native speed)
```

```
┌─────────────────────────────┐       ┌─────────────────────────────┐
│  Primary (Node 1)           │       │  Replica (Node 2)           │
│                             │       │                             │
│  App ──→ SQLite (embedded)  │       │  App ──→ SQLite (embedded)  │
│         │                   │       │         ▲                   │
│         ▼                   │       │         │                   │
│    app.db + app.db-wal      │       │    replica.db + .db-wal     │
│         │                   │       │         ▲                   │
│    ┌────┴────┐              │       │    ┌────┴────┐              │
│    │ walsync │ ── HTTP ─────┼───────┼───→│ walsync │              │
│    │ primary │  WAL ship    │       │    │ replica │              │
│    └─────────┘              │       │    └─────────┘              │
└─────────────────────────────┘       └─────────────────────────────┘
```

## Quick start

### Build

```bash
# Native
go build -o walsync .

# Cross-compile for Linux amd64
GOOS=linux GOARCH=amd64 go build -o walsync-linux-amd64 .
```

### Download pre-built binary

```bash
# Download from GitHub releases
curl -L https://github.com/maulanashalihin/walsync/releases/latest/download/walsync-linux-amd64 -o walsync
chmod +x walsync
```

Available: `walsync-darwin-arm64`, `walsync-darwin-amd64`, `walsync-linux-amd64`, `walsync-linux-arm64`.


### Run

```bash
# Replica (Node 2) — start first
./walsync -mode replica -db /path/to/replica.db -listen :9090

# Primary (Node 1) — start after replica is up
./walsync -mode primary -db /path/to/app.db -replicas http://replica-ip:9090
```

### Production deployment

For production, use systemd to run walsync as a daemon with auto-restart, logging, and security hardening. See **[deploy/README.md](deploy/README.md)** for complete guide.

Quick setup:

```bash
# Install binary
sudo cp walsync-linux-amd64 /usr/local/bin/walsync

# Copy systemd unit (edit paths first!)
sudo cp deploy/walsync-primary.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now walsync-primary

# View logs
sudo journalctl -u walsync-primary -f
```


### App usage

Your app uses native SQLite with WAL mode. No special drivers, no FUSE, no proxies:

```javascript
const { DatabaseSync } = require('node:sqlite');
const db = new DatabaseSync('/path/to/app.db');
db.exec('PRAGMA journal_mode = WAL');
db.exec('PRAGMA synchronous = NORMAL');
db.exec('PRAGMA wal_autocheckpoint = 0'); // let walsync handle checkpointing
```

Works with any SQLite binding in any language: `better-sqlite3` (Node), `sqlite3` (Python), `rusqlite` (Rust), `database/sql` (Go CGo), raw C API.

## Features

- **Embedded SQLite on both sides** — App reads/writes at native SQLite speed (221K read QPS, 94K write QPS)
- **Automatic sync** — walsync detects WAL changes and ships to replicas in background
- **Initial snapshot** — On startup, ships full DB + WAL to new replicas
- **Checkpoint detection** — When SQLite checkpoints (WAL → DB), ships new snapshot automatically
- **Multi-replica** — Ship to multiple replicas simultaneously (comma-separated URLs)
- **Single binary** — Go binary, no runtime dependencies, cross-compile to any platform

## Limitations

This is an MVP. Current constraints:

- **Single-writer** — Only primary accepts writes. Replicas are read-only.
- **No failover** — No automatic primary promotion. Manual failover only.
- **Eventual consistency** — Sync delay ~1-3 seconds depending on network.
- **No auth** — HTTP endpoints are unauthenticated. Use behind VPN/firewall.
- **No WAL frame-level shipping** — Ships WAL file chunks, not individual frames. Checkpoint triggers full snapshot.

### Roadmap

- [ ] WAL frame-level incremental shipping (avoid full snapshot on checkpoint)
- [ ] TLS + token authentication
- [ ] Automatic failover (promote replica on primary failure)
- [ ] Multi-primary with conflict resolution (LWW or CRDT)
- [ ] gRPC transport option (lower overhead than HTTP)
- [ ] Prometheus metrics endpoint

## Benchmark

Multi-server test: 2 Singapore VPS (~20ms network latency), 6 vCPU x86_64.

| Operation | Sync delay | Result |
|-----------|-----------:|--------|
| INSERT | ~3s | ✅ All rows replicated |
| UPDATE | ~3s | ✅ Updates replicated |
| DELETE | ~3s | ✅ Deletes replicated |
| Initial snapshot | ~1s | ✅ Full DB shipped |
| Checkpoint re-sync | ~3s | ✅ Snapshot re-shipped |

Performance comparison (same hardware, localhost):

| Tool | Read QPS | Write QPS | Model | Multi-server |
|------|--------:|--------:|-------|:---:|
| **walsync** | **221K** | **94K** | Embedded + async WAL ship | ✅ |
| Native SQLite | 221K | 94K | Embedded | ❌ |
| LiteFS (host) | 220K | 6K | FUSE + LTX | ✅ |
| Marmot (TCP) | 16K | 17K | Server (CDC) | ✅ |
| PostgreSQL (TCP) | 10K | 4K | Server | ✅ |
| Turso (remote) | 7 | 7 | Serverless HTTP | ✅ |

walsync achieves native SQLite speed because:
1. App uses embedded SQLite directly (no FUSE, no TCP, no interceptor)
2. walsync runs as a separate background process (zero overhead on app path)
3. WAL shipping is async (writes return immediately, sync happens in background)

## Examples

See [`examples/`](examples/) for sample apps:
- `writer-app.js` — Persistent writer (Node.js, keeps DB connection open)
- `reader-app.js` — Read-only app (Node.js, reads from replica)

## Contributing

Contributions welcome! See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT — see [LICENSE](LICENSE).
