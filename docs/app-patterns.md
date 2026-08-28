# App patterns for single-writer + multi-read

walsync gives you one primary (writes) and N replicas (reads). Your app needs to route writes to primary and distribute reads across replicas. SQLite is embedded — there's no `DATABASE_URL` like PostgreSQL. Routing happens in your app, not the database.

## Architecture

```
Node A (Primary)              Node B (Replica)            Node C (Replica)
┌──────────────────┐         ┌──────────────────┐       ┌──────────────────┐
│ App              │         │ App              │       │ App              │
│  GET  → SQLite   │         │  GET  → SQLite   │       │  GET  → SQLite   │
│  POST → SQLite   │         │  POST → proxy A  │       │  POST → proxy A  │
│     (local file) │         │     (local file) │       │     (local file) │
│                  │         │                  │       │                  │
│ walsync primary  │──WAL───→│ walsync replica  │       │ walsync replica  │
└──────────────────┘         └──────────────────┘       └──────────────────┘
```

Key insight: every node reads from local SQLite (native speed, 221K QPS). Only writes need routing.

## Pattern 1: Role-based (recommended for 2-3 nodes)

Each node knows its role via environment variable. Replicas proxy writes to primary.

```javascript
// server.js — works on both primary and replica
const express = require('express');
const Database = require('better-sqlite3');

const ROLE = process.env.WALSYNC_ROLE || 'primary';
const PRIMARY_URL = process.env.PRIMARY_URL || 'http://localhost:3000';
const DB_PATH = process.env.DB_PATH || '/data/app.db';

// Primary: persistent connection for writes (keeps WAL alive)
let writeDb = null;
function getWriteDb() {
  if (!writeDb) {
    writeDb = new Database(DB_PATH);
    writeDb.pragma('journal_mode = WAL');
    writeDb.pragma('synchronous = NORMAL');
    writeDb.pragma('wal_autocheckpoint = 0');
    writeDb.exec('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, city TEXT)');
  }
  return writeDb;
}

// Replica: persistent readonly connection (natural pattern).
// walsync corrupts -shm after each WAL ship (same inode) → SQLite detects
// invalid checksum → rebuilds WAL index from scan → updates -shm in place.
// Persistent connection sees update via mmap shared memory.
// readonly = no checkpoint on close = WAL preserved for next incremental ship.
const readDb = ROLE === 'replica' ? new Database(DB_PATH, { readonly: true }) : null;

const app = express();
app.use(express.json());

// READ — every node handles locally (native SQLite speed)
// Primary reads from writeDb, replica reads from readDb (persistent readonly)
function getReadDb() {
  return ROLE === 'primary' ? getWriteDb() : readDb;
}

app.get('/api/users', (req, res) => {
  res.json(getReadDb().prepare('SELECT * FROM users ORDER BY id DESC').all());
});

app.get('/api/users/:id', (req, res) => {
  const user = getReadDb().prepare('SELECT * FROM users WHERE id = ?').get(Number(req.params.id));
  if (!user) return res.status(404).json({ error: 'not found' });
  res.json(user);
});

// WRITE — primary handles locally, replica proxies to primary
app.post('/api/users', async (req, res) => {
  if (ROLE === 'primary') {
    const { name, city } = req.body;
    const r = getWriteDb().prepare('INSERT INTO users(name, city) VALUES(?, ?)').run(name, city);
    return res.json({ id: r.lastInsertRowid, name, city });
  }
  try {
    const resp = await fetch(`${PRIMARY_URL}/api/users`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req.body),
    });
    res.status(resp.status).json(await resp.json());
  } catch (err) {
    res.status(503).json({ error: 'primary unavailable', detail: String(err) });
  }
});

app.put('/api/users/:id', async (req, res) => {
  if (ROLE === 'primary') {
    const { name, city } = req.body;
    getWriteDb().prepare('UPDATE users SET name = ?, city = ? WHERE id = ?').run(name, city, Number(req.params.id));
    return res.json({ id: Number(req.params.id), ...req.body });
  }
  try {
    const resp = await fetch(`${PRIMARY_URL}/api/users/${req.params.id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req.body),
    });
    res.status(resp.status).json(await resp.json());
  } catch (err) {
    res.status(503).json({ error: 'primary unavailable', detail: String(err) });
  }
});

app.delete('/api/users/:id', async (req, res) => {
  if (ROLE === 'primary') {
    getWriteDb().prepare('DELETE FROM users WHERE id = ?').run(Number(req.params.id));
    return res.json({ deleted: true, id: Number(req.params.id) });
  }
  try {
    const resp = await fetch(`${PRIMARY_URL}/api/users/${req.params.id}`, { method: 'DELETE' });
    res.status(resp.status).json(await resp.json());
  } catch (err) {
    res.status(503).json({ error: 'primary unavailable', detail: String(err) });
  }
});

app.get('/health', (req, res) => {
  res.json({ role: ROLE, ok: true });
});

app.listen(3000, () => console.log(`[${ROLE}] listening on :3000`));
```

**Deploy:**
```bash
# Node A (primary)
WALSYNC_ROLE=primary DB_PATH=/data/app.db node server.js

# Node B (replica)
WALSYNC_ROLE=replica PRIMARY_URL=http://node-a:3000 DB_PATH=/data/app.db node server.js
```

**Load balancer:**
```
HAProxy round-robin → Node A, Node B, Node C
  GET  → any node (all read local SQLite)
  POST → any node (replicas proxy to primary, primary handles directly)
```

Simplest setup. 10 lines of proxy logic. No service discovery, no consensus.

## Pattern 2: Read/write split middleware (clean separation)

Separate read and write routers. Write router has a guard that proxies for replicas.

```javascript
const express = require('express');
const { DatabaseSync } = require('node:sqlite');
const { Router } = express;

const ROLE = process.env.WALSYNC_ROLE || 'primary';
const PRIMARY_URL = process.env.PRIMARY_URL || 'http://localhost:3000';
const db = new DatabaseSync(process.env.DB_PATH || '/data/app.db');

const app = express();
app.use(express.json());

// Write guard: primary handles locally, replica proxies
function writeGuard(req, res, next) {
  if (ROLE === 'primary') return next();
  const method = req.method;
  fetch(`${PRIMARY_URL}${req.originalUrl}`, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: ['POST', 'PUT', 'PATCH'].includes(method) ? JSON.stringify(req.body) : undefined,
  })
    .then(async r => res.status(r.status).json(await r.json()))
    .catch(err => res.status(503).json({ error: 'primary unavailable', detail: String(err) }));
}

// Read router — direct to local SQLite, any node
const readRouter = Router();
readRouter.get('/api/users', (req, res) => {
  res.json(db.prepare('SELECT * FROM users ORDER BY id DESC').all());
});
readRouter.get('/api/users/:id', (req, res) => {
  const user = db.prepare('SELECT * FROM users WHERE id = ?').get(req.params.id);
  if (!user) return res.status(404).json({ error: 'not found' });
  res.json(user);
});

// Write router — guarded, only primary executes
const writeRouter = Router();
writeRouter.post('/api/users', writeGuard, (req, res) => {
  const { name, city } = req.body;
  const result = db.prepare('INSERT INTO users(name, city) VALUES(?, ?)').run(name, city);
  res.json({ id: result.lastInsertRowid, name, city });
});
writeRouter.put('/api/users/:id', writeGuard, (req, res) => {
  const { name, city } = req.body;
  db.prepare('UPDATE users SET name = ?, city = ? WHERE id = ?').run(name, city, req.params.id);
  res.json({ id: Number(req.params.id), ...req.body });
});
writeRouter.delete('/api/users/:id', writeGuard, (req, res) => {
  db.prepare('DELETE FROM users WHERE id = ?').run(req.params.id);
  res.json({ deleted: true, id: Number(req.params.id) });
});

app.use(readRouter);
app.use(writeRouter);
app.listen(3000, () => console.log(`[${ROLE}] listening on :3000`));
```

Cleaner separation. Easy to test read routes and write routes independently. `writeGuard` is the only proxy logic — one function, all write routes.

## Pattern 3: Smart client (client-side routing)

Client knows the topology. Server stays dumb — primary handles all, replicas reject writes.

```javascript
// client.js
class WalsyncClient {
  constructor({ primary, replicas = [] }) {
    this.primary = primary;
    this.replicas = replicas;
    this.rr = 0; // round-robin index
  }

  async write(method, path, body) {
    return fetch(`${this.primary}${path}`, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: body ? JSON.stringify(body) : undefined,
    });
  }

  async read(path) {
    if (this.replicas.length === 0) return fetch(`${this.primary}${path}`);
    const replica = this.replicas[this.rr % this.replicas.length];
    this.rr++;
    try {
      const resp = await fetch(`${replica}${path}`, { signal: AbortSignal.timeout(2000) });
      if (resp.ok) return resp;
    } catch { /* fall through to primary */ }
    return fetch(`${this.primary}${path}`);
  }
}

// Usage
const client = new WalsyncClient({
  primary: 'http://node-a:3000',
  replicas: ['http://node-b:3000', 'http://node-c:3000'],
});

await client.write('POST', '/api/users', { name: 'Alice', city: 'SG' });
const resp = await client.read('/api/users');
const users = await resp.json();
```

```javascript
// server.js — primary
app.post('/api/users', (req, res) => {
  const { name, city } = req.body;
  const result = db.prepare('INSERT INTO users(name, city) VALUES(?, ?)').run(name, city);
  res.json({ id: result.lastInsertRowid, name, city });
});

// server.js — replica (same code, writes rejected)
app.post('/api/users', (req, res) => {
  res.status(403).json({ error: 'read-only replica', primaryUrl: PRIMARY_URL });
});
```

Best for high-throughput: client distributes reads, no proxy hop on writes. Requires client to know topology (config or service discovery).

## Read-after-write consistency

walsync sync delay is ~100ms. A read immediately after a write may hit a replica that hasn't received the WAL yet.

```
t=0ms:   POST /api/users → primary (committed)
t=5ms:   GET /api/users  → replica (WAL not arrived yet — stale!)
t=100ms: WAL arrives at replica
t=105ms: GET /api/users  → replica (data present)
```

### Strategy 1: Read-your-writes window

Read from primary for N ms after last write. Simple, works for session-based apps.

```javascript
class ReadYourWritesClient {
  constructor({ primary, replicas = [], windowMs = 500 }) {
    this.primary = primary;
    this.replicas = replicas;
    this.windowMs = windowMs;
    this.lastWrite = 0;
    this.rr = 0;
  }

  async write(method, path, body) {
    this.lastWrite = Date.now();
    return fetch(`${this.primary}${path}`, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: body ? JSON.stringify(body) : undefined,
    });
  }

  async read(path) {
    // Within consistency window → read from primary
    if (Date.now() - this.lastWrite < this.windowMs) {
      return fetch(`${this.primary}${path}`);
    }
    // Outside window → read from replica (round-robin)
    if (this.replicas.length === 0) return fetch(`${this.primary}${path}`);
    const replica = this.replicas[this.rr % this.replicas.length];
    this.rr++;
    try {
      const resp = await fetch(`${replica}${path}`, { signal: AbortSignal.timeout(2000) });
      if (resp.ok) return resp;
    } catch {}
    return fetch(`${this.primary}${path}`);
  }
}
```

### Strategy 2: Critical reads → primary, heavy reads → replica

Route specific endpoints to primary, rest to replicas.

```javascript
// Always read from primary — user must see their own data immediately
app.get('/api/me', (req, res) => {
  res.json(db.prepare('SELECT * FROM users WHERE id = ?').get(req.userId));
});

// Replica OK — feed can be ~100ms stale
app.get('/api/feed', (req, res) => {
  res.json(db.prepare('SELECT * FROM posts ORDER BY created_at DESC LIMIT 50').all());
});
```

### Strategy 3: Accept eventual consistency

Document it: "reads may be stale by ~100ms". Fine for analytics, dashboards, search, activity feeds. Not fine for "create order → immediately view order".

## Error handling

```javascript
// Replica gets write accidentally — clear error
app.all('/api/*', (req, res, next) => {
  if (ROLE === 'replica' && req.method !== 'GET') {
    return res.status(403).json({
      error: 'read-only-replica',
      message: 'Send writes to primary',
      primaryUrl: PRIMARY_URL,
    });
  }
  next();
});

// Primary unreachable — replicas still serve reads
app.get('/health', async (req, res) => {
  let primaryReachable = true;
  if (ROLE === 'replica') {
    try {
      await fetch(`${PRIMARY_URL}/health`, { signal: AbortSignal.timeout(1000) });
    } catch {
      primaryReachable = false;
    }
  }
  res.json({
    role: ROLE,
    db: db.prepare('SELECT 1').get() ? 'ok' : 'error',
    primaryReachable,
  });
});
```

## Which pattern?

| Pattern | Complexity | Nodes | Best for |
|---------|-----------|------|----------|
| 1. Role-based | Low | 2-3 | Simple API, quick deploy |
| 2. RW split middleware | Medium | 3+ | Clean separation, testable |
| 3. Smart client | Medium | 3+ | High throughput, client-side control |

**Start with Pattern 1.** It's 10 lines of proxy logic. Upgrade to Pattern 2 when API grows. Use Pattern 3 when you need fine-grained read distribution.

## Load balancer config

```
# HAProxy — all nodes equal, replicas proxy writes internally
frontend api
  bind *:80
  default_backend walsync_nodes

backend walsync_nodes
  balance round-robin
  server node-a 10.0.0.1:3000 check
  server node-b 10.0.0.2:3000 check
  server node-c 10.0.0.3:3000 check
```

All nodes receive all requests. GET hits local SQLite. POST/PUT/DELETE either executes locally (primary) or proxies to primary (replica). Simple, no path-based routing needed.

For Pattern 3 (smart client), no load balancer needed — client routes directly.

## WAL visibility on replica (v0.8.0+)

walsync ships WAL bytes directly to the replica's `-wal` file. After each WAL ship, walsync **corrupts** `-shm` (flips first byte, same inode) so SQLite detects an invalid checksum, rebuilds the WAL index from the WAL file, and writes the valid index back to the same file. Persistent app connections see the update via mmap shared memory.

### How it works

```
walsync writes WAL bytes → -wal file grows
walsync corrupts -shm     → first byte flipped (same inode)
app's persistent conn     → next query reads -shm via mmap
                           → detects invalid checksum
                           → scans -wal for all valid frames
                           → rebuilds -shm in place (same inode)
                           → sees new WAL frames ✅
```

### Correct pattern: persistent readonly

```javascript
// ✅ Natural pattern — persistent readonly connection
const db = new Database(DB_PATH, { readonly: true });
app.get('/api/users', (req, res) => {
  res.json(db.prepare('SELECT * FROM users').all()); // always sees latest WAL
});
```

Use `readonly: true` to prevent checkpoint on close (WAL preserved for next incremental ship).

### Why not delete -shm? (v0.7.0 approach)

v0.7.0 deleted `-shm` after each WAL ship. This created a new file (different inode). Persistent connections had the old file mmap'd (still in memory after deletion) and never saw the new `-shm`. Only fresh connections worked — requiring an unnatural "open+close per read" pattern.

v0.8.0 corrupts `-shm` in place (same inode). SQLite rebuilds it, and mmap coherence ensures all connections see the update.

### Tested SQLite clients

All clients below read incremental WAL correctly with persistent readonly connections (tested v0.8.0):

| Client | Language | Status |
|-------|----------|:------:|
| sqlite3 CLI | C | ✅ |
| Python sqlite3 / SQLAlchemy | Python | ✅ |
| better-sqlite3 / node:sqlite | Node.js | ✅ |
| bun:sqlite | Bun | ✅ |
| PHP PDO SQLite | PHP | ✅ |
| Ruby sqlite3 | Ruby | ✅ |
| mattn/go-sqlite3 / modernc.org/sqlite | Go | ✅ |
| rusqlite | Rust | ✅ |

## Snapshot handling

When the primary's SQLite checkpoints (WAL → DB file), walsync ships a **full snapshot** — the entire DB file is atomically replaced on the replica (`os.Rename`). This changes the DB file's inode. Any persistent app connection that opened the DB before the snapshot now points to the old (deleted) file descriptor and reads **stale data forever**.

### When does checkpoint happen?

- Primary app closes its last database connection (e.g. app restart)
- Primary app explicitly runs `PRAGMA wal_checkpoint`
- SQLite auto-checkpoint reaches threshold (disabled if `wal_autocheckpoint = 0`)
- `sqlite3` CLI is used on the primary DB (each CLI exit = checkpoint)

### How to handle: auto-reconnect on error

```javascript
const Database = require('better-sqlite3');

let readDb = null;

function getReadDb() {
  if (!readDb) {
    readDb = new Database(DB_PATH, { readonly: true });
  }
  return readDb;
}

function readQuery(sql, ...params) {
  try {
    return getReadDb().prepare(sql).all(...params);
  } catch (err) {
    // SQLite error codes that indicate stale file handle:
    // SQLITE_IOERR (10), SQLITE_NOTADB (26), SQLITE_CANTOPEN (14)
    if (err.code === 'SQLITE_IOERR' || err.code === 'SQLITE_NOTADB' || err.code === 'SQLITE_CANTOPEN') {
      console.log('Replica DB replaced (snapshot), reconnecting...');
      readDb = null;  // force reopen on next call
      return readQuery(sql, ...params);  // retry once
    }
    throw err;
  }
}
```

### How to handle: periodic reconnect

Simpler approach — reopen connection every N seconds. No error detection needed, but adds slight overhead.

```javascript
let readDb = new Database(DB_PATH, { readonly: true });

// Reopen every 60 seconds to pick up snapshots
setInterval(() => {
  readDb.close();
  readDb = new Database(DB_PATH, { readonly: true });
}, 60_000);
```

### Which approach?

- **Auto-reconnect on error** — Zero overhead, handles snapshot immediately. Recommended for most apps.
- **Periodic reconnect** — Simpler, but reads may be stale for up to N seconds after snapshot. Fine for analytics/dashboards.

## WAL growth management

Replica readonly connections never checkpoint. The WAL file grows indefinitely as walsync ships more data. Over weeks/months, WAL can reach GB.

### Monitoring

```bash
# Check WAL size via walsync health endpoint
curl -s http://replica:9193/health | jq .wal_size

# Alert if WAL > 100MB
WAL_SIZE=$(curl -s http://replica:9193/health | jq .wal_size)
if [ "$WAL_SIZE" -gt 104857600 ]; then
  echo "ALERT: Replica WAL exceeds 100MB"
fi
```

### Manual checkpoint (safe)

To checkpoint the replica WAL (merge WAL into DB, truncate WAL):

```bash
# 1. Stop walsync replica
systemctl stop walsync-replica

# 2. Checkpoint (requires write connection, so no app should be reading)
sqlite3 /data/app.db "PRAGMA wal_checkpoint(TRUNCATE);"

# 3. Start walsync replica
systemctl start walsync-replica
```

**Important:** Checkpoint requires a brief write window. Apps with readonly connections open will block the checkpoint. Stop apps OR stop walsync and ensure no connections are open before checkpointing.

### Primary-side: prevent unnecessary checkpoints

Every primary checkpoint = full snapshot to all replicas. To minimize:

- Set `PRAGMA wal_autocheckpoint = 0` on primary app
- Keep primary app connection persistent (don't close/reopen)
- Never use `sqlite3` CLI on the primary DB in production
- If WAL grows too large on primary, schedule a maintenance window: stop app, let walsync ship final WAL, checkpoint, restart
