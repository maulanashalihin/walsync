#!/bin/bash
# setup.sh — start walsync primary+replica on localhost, write test data, wait for sync
# Called by individual test scripts. Exports DB_PRIMARY and DB_REPLICA paths.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
WALSYNC="${WALSYNC:-$PROJECT_DIR/walsync}"

# Fallback: check compat dir for binary
if [ ! -f "$WALSYNC" ]; then
  WALSYNC="$SCRIPT_DIR/walsync"
fi

DB_PRIMARY="$SCRIPT_DIR/test-primary.db"
DB_REPLICA="$SCRIPT_DIR/test-replica.db"
PORT_PRIMARY=19090
PORT_REPLICA=19091

# Clean any previous state
bash "$SCRIPT_DIR/cleanup.sh" 2>/dev/null || true
rm -f "$DB_PRIMARY" "$DB_PRIMARY"-* "$DB_REPLICA" "$DB_REPLICA"-*

# Start replica first
"$WALSYNC" -mode replica -db "$DB_REPLICA" -listen ":$PORT_REPLICA" \
  > "$SCRIPT_DIR/replica.log" 2>&1 &
REPLICA_PID=$!

# Wait for replica to be ready
for i in $(seq 1 20); do
  if curl -s "http://localhost:$PORT_REPLICA/health" 2>/dev/null | grep -q '"ok":true'; then
    break
  fi
  sleep 0.1
done

# Start primary
"$WALSYNC" -mode primary -db "$DB_PRIMARY" -replicas "localhost:$PORT_REPLICA" \
  > "$SCRIPT_DIR/primary.log" 2>&1 &
PRIMARY_PID=$!

# Wait for primary to be ready
sleep 0.5

# Create schema + write test data via sqlite3 CLI on primary
sqlite3 "$DB_PRIMARY" "
  PRAGMA journal_mode = WAL;
  PRAGMA synchronous = NORMAL;
  PRAGMA wal_autocheckpoint = 0;
  CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, value INTEGER, created_at INTEGER);
  INSERT INTO items (name, value, created_at) VALUES ('test-row-1', 42, $(date +%s));
  INSERT INTO items (name, value, created_at) VALUES ('test-row-2', 99, $(date +%s));
"

# Wait for sync: poll replica DB file existence + row count
# Use readwrite mode for polling because replica DB may lack -wal/-shm files
# after snapshot — readonly mode would fail with SQLITE_CANTOPEN.
SYNCED=0
for i in $(seq 1 100); do
  if [ -f "$DB_REPLICA" ]; then
    RC=$(sqlite3 "$DB_REPLICA" "SELECT COUNT(*) FROM items" 2>/dev/null || echo "0")
    if [ "$RC" -ge 2 ] 2>/dev/null; then
      SYNCED=1
      sleep 0.5  # give SQLite time to settle -shm
      break
    fi
  fi
  sleep 0.1
done

if [ "$SYNCED" != "1" ]; then
  echo "  ERROR: sync did not complete within timeout"
  cat "$SCRIPT_DIR/primary.log" 2>/dev/null | tail -5
  cat "$SCRIPT_DIR/replica.log" 2>/dev/null | tail -5
  exit 1
fi

# Warm up replica DB: open in readwrite mode to create -wal/-shm files.
# Without this, readonly connections fail with SQLITE_CANTOPEN because
# the DB is in WAL mode but has no -wal/-shm files after snapshot.
sqlite3 "$DB_REPLICA" "SELECT 1" >/dev/null 2>&1 || true

echo "  primary PID=$PRIMARY_PID, replica PID=$REPLICA_PID"
echo "  DB_PRIMARY=$DB_PRIMARY"
echo "  DB_REPLICA=$DB_REPLICA"
echo "  sync confirmed: replica has $RC rows"
