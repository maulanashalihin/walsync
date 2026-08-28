// Test app: writes to SQLite on primary, reads from replica
const { DatabaseSync } = require('node:sqlite');

const mode = process.argv[2] || 'primary';
const dbPath = process.argv[3] || '/tmp/walsync-test.db';

const db = new DatabaseSync(dbPath);
db.exec('PRAGMA journal_mode = WAL');
db.exec('PRAGMA synchronous = NORMAL');
db.exec('PRAGMA wal_autocheckpoint = 0'); // disable auto-checkpoint, let walsync handle

// Create table
db.exec(`CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY,
  name TEXT,
  city TEXT,
  created_at TEXT DEFAULT (datetime('now'))
)`);

if (mode === 'primary') {
  // Write mode
  const name = process.argv[4] || 'TestUser';
  const city = process.argv[5] || 'Singapore';

  const stmt = db.prepare('INSERT INTO users (name, city) VALUES (?, ?)');
  const result = stmt.run(name, city);

  console.log(`[PRIMARY] Inserted: id=${result.lastInsertRowid}, name=${name}, city=${city}`);

  // Show all rows
  const rows = db.prepare('SELECT * FROM users ORDER BY id DESC LIMIT 5').all();
  console.log('[PRIMARY] Latest rows:', JSON.stringify(rows));
} else {
  // Read mode (replica)
  const rows = db.prepare('SELECT * FROM users ORDER BY id DESC LIMIT 10').all();
  console.log(`[REPLICA] ${rows.length} rows:`);
  for (const row of rows) {
    console.log(`  id=${row.id} name=${row.name} city=${row.city} created_at=${row.created_at}`);
  }
}

db.close();
