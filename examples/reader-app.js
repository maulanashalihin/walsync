// Reader app: reads from replica DB
const { DatabaseSync } = require('node:sqlite');

const dbPath = process.argv[2] || '/tmp/walsync-replica.db';

try {
  const db = new DatabaseSync(dbPath);
  db.exec('PRAGMA query_only = 1'); // read-only mode

  const rows = db.prepare('SELECT * FROM users ORDER BY id DESC LIMIT 20').all();
  console.log(`[READER] ${rows.length} rows:`);
  for (const row of rows) {
    console.log(`  id=${row.id} name=${row.name} city=${row.city} created_at=${row.created_at}`);
  }
  db.close();
} catch (e) {
  console.log(`[READER] Error: ${e.message}`);
}
