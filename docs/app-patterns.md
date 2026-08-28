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
const { DatabaseSync } = require('node:sqlite');

const ROLE = process.env.WALSYNC_ROLE || 'primary';
const PRIMARY_URL = process.env.PRIMARY_URL || 'http://localhost:3000';
const DB_PATH = process.env.DB_PATH || '/data/app.db';

const db = new DatabaseSync(DB_PATH);
db.exec('PRAGMA journal_mode = WAL');
db.exec('PRAGMA synchronous = NORMAL');
db.exec('PRAGMA wal_autocheckpoint = 0');

const app = express();
app.use(express.json());

// READ — every node handles locally (native SQLite speed)
app.get('/api/users', (req, res) => {
  res.json(db.prepare('SELECT * FROM users ORDER BY id DESC').all());
});

app.get('/api/users/:id', (req, res) => {
  const user = db.prepare('SELECT * FROM users WHERE id = ?').get(req.params.id);
  if (!user) return res.status(404).json({ error: 'not found' });
  res.json(user);
});

// WRITE — primary handles locally, replica proxies to primary
app.post('/api/users', async (req, res) => {
  if (ROLE === 'primary') {
    const { name, city } = req.body;
    const result = db.prepare('INSERT INTO users(name, city) VALUES(?, ?)').run(name, city);
    return res.json({ id: result.lastInsertRowid, name, city });
  }
  // Replica: proxy to primary
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
    db.prepare('UPDATE users SET name = ?, city = ? WHERE id = ?').run(name, city, req.params.id);
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
    db.prepare('DELETE FROM users WHERE id = ?').run(req.params.id);
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
