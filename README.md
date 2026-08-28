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

The tradeoff: eventual consistency (~100ms sync delay) and single-writer (primary only).

## How it works

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

1. App writes to local SQLite (embedded, native speed)
2. walsync watches WAL file changes (fsnotify + polling)
3. walsync ships WAL data to replicas via HTTP (persistent connections, gzip compressed)
4. Replica receives WAL, writes to local SQLite
5. App on replica reads from local SQLite (embedded, native speed)

## Quick start

### 1. Download binary

```bash
# Linux x86_64
curl -L https://github.com/maulanashalihin/walsync/releases/latest/download/walsync-linux-amd64 -o walsync
chmod +x walsync

# Other platforms: replace walsync-linux-amd64 with:
#   walsync-darwin-arm64  (macOS Apple Silicon)
#   walsync-darwin-amd64  (macOS Intel)
#   walsync-linux-arm64   (Linux ARM64)
```

Or build from source (requires Go 1.26+):

```bash
git clone https://github.com/maulanashalihin/walsync.git
cd walsync
go build -o walsync .
```

### 2. Start replica (Node 2)

```bash
./walsync -mode replica -db /data/app.db -listen :9090
```

This starts an HTTP server on port 9090 that receives WAL data from primary.

### 3. Start primary (Node 1)

```bash
./walsync -mode primary -db /data/app.db -replicas replica-ip:9090
```

This watches the WAL file and ships changes to the replica. Multiple replicas: `-replicas replica1:9090,replica2:9090`.

### 4. Use SQLite in your app

Your app uses native SQLite with WAL mode. No special drivers, no FUSE, no proxies:

```javascript
// Node.js (node:sqlite or better-sqlite3)
const { DatabaseSync } = require('node:sqlite');
const db = new DatabaseSync('/data/app.db');
db.exec('PRAGMA journal_mode = WAL');
db.exec('PRAGMA synchronous = NORMAL');
db.exec('PRAGMA wal_autocheckpoint = 0'); // let walsync handle checkpointing

// Write on primary
db.prepare('INSERT INTO users(name, city) VALUES(?, ?)').run('Alice', 'Singapore');

// Read on replica (different server, same native speed)
const users = db.prepare('SELECT * FROM users').all();
```

```python
# Python (sqlite3)
import sqlite3
db = sqlite3.connect('/data/app.db')
db.execute('PRAGMA journal_mode = WAL')
db.execute('PRAGMA synchronous = NORMAL')
db.execute('PRAGMA wal_autocheckpoint = 0')
```

```go
// Go (CGo + mattn/go-sqlite3)
db, _ := sql.Open("sqlite3", "/data/app.db?_journal_mode=WAL&_synchronous=NORMAL&_wal_autocheckpoint=0")
```

```rust
// Rust (rusqlite)
let conn = Connection::open("/data/app.db")?;
conn.pragma_update(None, "journal_mode", "WAL")?;
conn.pragma_update(None, "synchronous", "NORMAL")?;
conn.pragma_update(None, "wal_autocheckpoint", "0")?;
```

Works with any SQLite binding in any language. The only requirement: **WAL mode** and **disable auto-checkpoint** so walsync can track changes.

### 5. Verify sync

```bash
# On primary: write a row
sqlite3 /data/app.db "INSERT INTO users(name) VALUES('test');"

# On replica: read it (within ~100ms)
sqlite3 /data/app.db "SELECT * FROM users;"
```

## Production deployment

### systemd (recommended)

```bash
# 1. Install binary
sudo mkdir -p /var/lib/walsync
sudo cp walsync-linux-amd64 /usr/local/bin/walsync

# 2. Copy service file (edit -db path and -replicas first!)
sudo cp deploy/walsync-primary.service /etc/systemd/system/
# Edit: sudo nano /etc/systemd/system/walsync-primary.service

# 3. Enable and start
sudo systemctl daemon-reload
sudo systemctl enable --now walsync-primary

# 4. Check status and logs
sudo systemctl status walsync-primary
sudo journalctl -u walsync-primary -f
```

For replica: copy `deploy/walsync-replica.service` instead.

systemd features included: auto-restart on failure, start on boot, security hardening (NoNewPrivileges, ProtectSystem, PrivateTmp), journal logging.

### Docker

```bash
# Build
docker build -t walsync .

# Run replica
docker run -d --name walsync-replica \
  -p 9090:9090 \
  -v /data/walsync:/var/lib/walsync \
  walsync -mode replica -db /var/lib/walsync/app.db -listen :9090

# Run primary
docker run -d --name walsync-primary \
  -v /data/walsync:/var/lib/walsync \
  walsync -mode primary -db /var/lib/walsync/app.db -replicas replica-host:9090
```

### Firewall — REQUIRED before production

walsync has no auth by design (zero overhead). **You MUST restrict replica port to primary IPs only** before going to production. An exposed replica port lets anyone write arbitrary WAL data to your database.

```bash
# UFW (Ubuntu/Debian) — replace PRIMARY_IP with your primary server's IP
sudo ufw allow from PRIMARY_IP to any port 9090
sudo ufw deny 9090
sudo ufw enable

# Verify: only PRIMARY_IP should have access
sudo ufw status numbered | grep 9090
```

```bash
# iptables (any Linux)
sudo iptables -A INPUT -p tcp --dport 9090 -s PRIMARY_IP -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 9090 -j DROP
# Persist: sudo apt install iptables-persistent && sudo netfilter-persistent save
```

**Why firewall, not TLS/auth?** Firewall = kernel-level, zero app overhead, already in your OS. TLS = handshake + encrypt/decrypt per WAL ship. Token auth = per-request validation. For trusted server-to-server WAL shipping, firewall is the right tool.

See [deploy/README.md](deploy/README.md) for complete production guide (operations, log rotation, updates, multi-replica setup).

## CLI reference

```
walsync -mode <primary|replica> -db <path> [options]

Primary mode:
  -mode primary
  -db <path>              SQLite database file path
  -replicas <addr,addr>   Comma-separated replica addresses (host:port)

Replica mode:
  -mode replica
  -db <path>              SQLite database file path
  -listen <addr>          HTTP listen address (default :9090)

Examples:
  walsync -mode replica -db /data/app.db -listen :9090
  walsync -mode primary -db /data/app.db -replicas 10.0.0.2:9090,10.0.0.3:9090
```

## Features

- **Embedded SQLite on both sides** — App reads/writes at native SQLite speed (221K read QPS, 94K write QPS)
- **HTTP transport (Fiber/fasthttp)** — Persistent connections, gzip compressed, 1.7-2.1x faster than gRPC
- **gzip compression** — 95% bandwidth reduction (SQLite pages compress extremely well)
- **Keepalive** — Connection failure detected in ~15s (ping every 10s)
- **Automatic sync** — walsync detects WAL changes and ships to replicas in background
- **Initial snapshot** — On startup, ships full DB + WAL to new replicas
- **Checkpoint detection** — When SQLite checkpoints (WAL → DB), ships new snapshot automatically
- **Multi-replica** — Ship to multiple replicas simultaneously (comma-separated addresses)
- **Single binary** — Go binary, no runtime dependencies, cross-compile to any platform

## Client compatibility

Tested with walsync v0.8.0 — all clients read incremental WAL correctly with **persistent readonly connections** (natural app pattern).

| Client | Language | Engine | Status |
|--------|----------|--------|:------:|
| sqlite3 CLI | C | C API | ✅ |
| Python sqlite3 | Python | C API | ✅ |
| Python SQLAlchemy | Python | C API | ✅ |
| better-sqlite3 | Node.js | C API | ✅ |
| node:sqlite | Node.js | C API | ✅ |
| bun:sqlite | Bun | C API | ✅ |
| PHP PDO SQLite | PHP | C API | ✅ |
| Ruby sqlite3 | Ruby | C API | ✅ |
| mattn/go-sqlite3 | Go | CGo | ✅ |
| modernc.org/sqlite | Go | pure Go | ✅ |
| rusqlite | Rust | bundled C | ✅ |

**Pattern for replica reads:**
```js
// Natural pattern — persistent readonly connection.
// walsync corrupts -shm after each WAL ship (same inode).
// SQLite detects invalid checksum → rebuilds from WAL → updates -shm in place.
// Persistent connection sees update via mmap shared memory.
const db = new Database(replicaPath, { readonly: true });
app.get('/api/users', (req, res) => {
  res.json(db.prepare('SELECT * FROM users').all());
});
```

Use `readonly: true` to prevent checkpoint on close (WAL preserved for next incremental ship).

## Limitations

- **Single-writer** — Only primary accepts writes. Replicas are read-only.
- **No automatic failover (by design)** — Manual failover only. Safe automatic failover requires consensus to prevent split-brain. See [manual failover guide](deploy/README.md#manual-failover).
- **Eventual consistency** — Sync delay ~100ms median (measured: 33-210ms, 2 Singapore VPS, ~20ms RTT)
- **No auth (by design)** — HTTP endpoints are unauthenticated. Security is handled at network layer (firewall/VPN), not application layer. Zero overhead.
- **No WAL frame-level shipping** — Ships WAL file chunks, not individual frames. Checkpoint triggers full snapshot.
- **Replica reads use readonly connections** — walsync corrupts `-shm` after each WAL ship (same inode) so SQLite rebuilds the WAL index in place. Persistent readonly connections see new frames via mmap. Use `readonly: true` to prevent checkpoint on close (WAL preserved). See [client compatibility](#client-compatibility) and [app patterns](docs/app-patterns.md).

### Roadmap

- [x] ~~WAL frame-level incremental shipping~~ — Not viable (checkpoint modifies untracked pages, causes corruption)
- [x] ~~TLS + token authentication~~ — Not pursuing. Firewall is zero-overhead, kernel-level. TLS adds handshake + encrypt/decrypt per request. IP verification adds per-connection check. Use firewall/VPN instead.
- [x] ~~Automatic failover~~ — Not pursuing. Safe failover requires consensus (Raft/Paxos) to prevent split-brain. That's rqlite/dqlite territory. Manual failover guide in [deploy/README.md](deploy/README.md#manual-failover).
- [x] ~~Multi-primary with conflict resolution~~ — Researched, not pursuing (market crowded: Marmot, cr-sqlite, rqlite, dqlite, LiteFS, Turso)
- [x] ~~Prometheus metrics endpoint~~ — Shipped in v0.5.0
- [x] ~~gRPC → Fiber (fasthttp) transport~~ — Shipped in v0.6.0 (1.7-2.1x faster, 24% smaller binary)
- [x] ~~WAL visibility fix (-shm removal)~~ — Shipped in v0.7.0 (incremental WAL now visible to all SQLite clients)
- [x] ~~WAL visibility fix (-shm corruption)~~ — Shipped in v0.8.0 (persistent readonly connections now see incremental WAL via mmap)

## Benchmark

### Sync performance (2 Singapore VPS, ~20ms latency, 6 vCPU x86_64)

| Operation | Sync delay | Result |
|-----------|-----------:|--------|
| Single write | ~100ms median (33-210ms) | ✅ All rows replicated |
| Burst (50 writes) | ~94ms (debounced batch) | ✅ 50/50 rows replicated |
| Initial snapshot | ~1s | ✅ Full DB shipped |
| Checkpoint re-sync | ~1-2s | ✅ Snapshot re-shipped |

### Throughput comparison (same hardware, localhost)

| Tool | Read QPS | Write QPS | Model | Multi-server |
|------|--------:|--------:|-------|:---:|
| **walsync** | **221K** | **94K** | Embedded + async WAL ship | ✅ |
| Native SQLite | 221K | 94K | Embedded | ❌ |
| LiteFS (host) | 220K | 6K | FUSE + LTX | ✅ |
| Marmot (TCP) | 16K | 17K | Server (CDC) | ✅ |
| PostgreSQL (TCP) | 10K | 4K | Server | ✅ |
| Turso (remote) | 7 | 7 | Serverless HTTP | ✅ |

### Compression A/B test (v0.2.0 vs v0.3.0, 1MB data)

| Metric | Without compression | With gzip | 
|--------|--------------------:|----------:|
| Rows synced | 100/100 ✅ | 100/100 ✅ |
| Total CPU | 350ms | 280ms (-20%) |
| Bandwidth | ~1.1MB | ~55KB (95% reduction) |

Compression is net positive: CPU drops 20% (less network I/O), bandwidth drops 95%.

walsync achieves native SQLite speed because:
1. App uses embedded SQLite directly (no FUSE, no TCP, no interceptor)
2. walsync runs as a separate background process (zero overhead on app path)
3. WAL shipping is async (writes return immediately, sync happens in background)

## Examples

See [`examples/`](examples/) for sample apps:
- `writer-app.js` — Persistent writer (Node.js, keeps DB connection open)
- `reader-app.js` — Read-only app (Node.js, reads from replica)
- `app-patterns/server.js` — Express app with role-based read/write split (primary writes, replica reads with fresh readonly per request)

See [`docs/app-patterns.md`](docs/app-patterns.md) for architecture patterns and read-after-write strategies.

## Contributing

Contributions welcome! See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT — see [LICENSE](LICENSE).
