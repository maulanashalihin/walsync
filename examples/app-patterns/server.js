// Pattern 1: Role-based single-writer + multi-read
// Uses better-sqlite3 (production-grade SQLite binding with proper WAL support).
//
// Run on both primary and replica nodes:
//   Primary:  WALSYNC_ROLE=primary DB_PATH=/data/app.db node server.js
//   Replica:  WALSYNC_ROLE=replica PRIMARY_URL=http://primary:3000 DB_PATH=/data/app.db
//
// v0.8.0+: walsync corrupts -shm after each WAL ship (same inode).
// SQLite detects invalid checksum → rebuilds from WAL → updates -shm in place.
// Persistent readonly connections see new frames via mmap. Natural pattern.
//
// v1.0.0: Auto-reconnect on snapshot. When primary checkpoints, walsync ships
// a full snapshot (atomic DB file replace = new inode). The old persistent
// connection points to a deleted file. We catch the error and reopen.

const express = require('express');
const Database = require('better-sqlite3');

const ROLE = process.env.WALSYNC_ROLE || 'primary';
const PRIMARY_URL = process.env.PRIMARY_URL || 'http://localhost:3000';
const DB_PATH = process.env.DB_PATH || '/tmp/walsync-app.db';

// Primary: persistent connection for writes
let writeDb = null;
function getWriteDb() {
  if (!writeDb) {
    writeDb = new Database(DB_PATH);
    writeDb.pragma('journal_mode = WAL');
    writeDb.pragma('synchronous = NORMAL');
    writeDb.pragma('wal_autocheckpoint = 0');
    writeDb.exec(`CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY,
      name TEXT,
      city TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )`);
  }
  return writeDb;
}

// Replica: persistent readonly connection with auto-reconnect.
// walsync corrupts -shm after each WAL ship → SQLite rebuilds in place →
// this connection sees new WAL frames via mmap. No reconnect needed for
// incremental WAL. BUT when primary checkpoints → snapshot → DB file
// replaced (new inode) → old connection stale → catch error → reopen.
let readDb = null;

function getReadDb() {
  if (ROLE === 'primary') return getWriteDb();
  if (!readDb) {
    readDb = new Database(DB_PATH, { readonly: true });
  }
  return readDb;
}

function readQuery(sql, ...params) {
  try {
    return getReadDb().prepare(sql).all(...params);
  } catch (err) {
    // Snapshot replaced DB file → stale file handle → reopen
    if (err.code === 'SQLITE_IOERR' || err.code === 'SQLITE_NOTADB' || err.code === 'SQLITE_CANTOPEN') {
      console.log('[replica] DB replaced (snapshot), reconnecting...');
      readDb = null;
      return readQuery(sql, ...params);
    }
    throw err;
  }
}

function readOne(sql, ...params) {
  try {
    return getReadDb().prepare(sql).get(...params);
  } catch (err) {
    if (err.code === 'SQLITE_IOERR' || err.code === 'SQLITE_NOTADB' || err.code === 'SQLITE_CANTOPEN') {
      console.log('[replica] DB replaced (snapshot), reconnecting...');
      readDb = null;
      return readOne(sql, ...params);
    }
    throw err;
  }
}

const app = express();
app.use(express.json());

// ── READ: every node handles locally ──
app.get('/api/users', (req, res) => {
  res.json(readQuery('SELECT * FROM users ORDER BY id DESC'));
});

app.get('/api/users/:id', (req, res) => {
  const user = readOne('SELECT * FROM users WHERE id = ?', Number(req.params.id));
  if (!user) return res.status(404).json({ error: 'not found' });
  res.json(user);
});

// ── WRITE: primary handles locally, replica proxies ──
app.post('/api/users', async (req, res) => {
  if (ROLE === 'primary') {
    const { name, city } = req.body;
    const r = getWriteDb().prepare('INSERT INTO users(name, city) VALUES(?, ?)').run(name, city);
    return res.json({ id: r.lastInsertRowid, name, city });
  }
  try {
    const r = await fetch(`${PRIMARY_URL}/api/users`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req.body),
    });
    res.status(r.status).json(await r.json());
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
    const r = await fetch(`${PRIMARY_URL}/api/users/${req.params.id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req.body),
    });
    res.status(r.status).json(await r.json());
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
    const r = await fetch(`${PRIMARY_URL}/api/users/${req.params.id}`, { method: 'DELETE' });
    res.status(r.status).json(await r.json());
  } catch (err) {
    res.status(503).json({ error: 'primary unavailable', detail: String(err) });
  }
});

app.get('/health', (req, res) => {
  res.json({ role: ROLE, ok: true });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`[${ROLE}] listening on :${PORT}`));
