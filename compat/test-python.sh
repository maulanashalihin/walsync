#!/bin/bash
# test-python.sh — test Python sqlite3 replica read
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Test: Python sqlite3 (persistent readonly) ==="
bash "$SCRIPT_DIR/setup.sh"

python3 -c "
import sqlite3, sys
db_path = sys.argv[1]
conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
conn.row_factory = sqlite3.Row

rows = conn.execute('SELECT * FROM items ORDER BY id').fetchall()
if len(rows) < 2:
    print(f'FAIL: expected >= 2 rows, got {len(rows)}'); sys.exit(1)
if rows[0]['name'] != 'test-row-1' or rows[0]['value'] != 42:
    print(f'FAIL: row 0 mismatch {dict(rows[0])}'); sys.exit(1)
if rows[1]['name'] != 'test-row-2' or rows[1]['value'] != 99:
    print(f'FAIL: row 1 mismatch {dict(rows[1])}'); sys.exit(1)
print(f'  read 1 OK: {len(rows)} rows')

count = conn.execute('SELECT COUNT(*) as c FROM items').fetchone()
print(f'  read 2 OK (persistent conn): count = {count[\"c\"]}')

row = conn.execute('SELECT * FROM items WHERE id = 1').fetchone()
if not row or row['name'] != 'test-row-1':
    print(f'FAIL: point read mismatch {dict(row) if row else None}'); sys.exit(1)
print(f'  read 3 OK (point read): {row[\"name\"]}')

conn.close()
print('PASS: Python sqlite3 — data correct, persistent connection works')
" "$SCRIPT_DIR/test-replica.db"

RESULT=$?
bash "$SCRIPT_DIR/cleanup.sh"
exit $RESULT
