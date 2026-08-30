#!/bin/bash
# test-corruption.sh — test walsync v1.1.0 corruption recovery scenarios
# 1. Checkpoint recovery: primary checkpoint → snapshot → replica sees new data
# 2. Salt mismatch: truncate replica WAL → primary ships snapshot
# 3. CLI access: sqlite3 CLI on replica (readwrite) → checkpoint → recovery
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
WALSYNC="${WALSYNC:-$PROJECT_DIR/walsync}"

if [ ! -f "$WALSYNC" ]; then
  WALSYNC="$SCRIPT_DIR/walsync"
  if [ ! -f "$WALSYNC" ]; then
    curl -fsSL https://github.com/maulanashalihin/walsync/releases/download/v1.1.0/walsync-linux-amd64-fixed -o "$WALSYNC"
    chmod +x "$WALSYNC"
  fi
fi

DB_PRIMARY="$SCRIPT_DIR/corr-primary.db"
DB_REPLICA="$SCRIPT_DIR/corr-replica.db"
PORT_PRIMARY=19092
PORT_REPLICA=19093

cleanup() {
  pkill -f "walsync.*corr-primary" 2>/dev/null || true
  pkill -f "walsync.*corr-replica" 2>/dev/null || true
  sleep 0.2
  rm -f "$DB_PRIMARY"* "$DB_REPLICA"* 2>/dev/null || true
}

FAIL=0
cleanup

echo "=== Corruption Test 1: Checkpoint Recovery ==="
echo "  (primary checkpoint → snapshot → replica sees new data)"

# Start replica
"$WALSYNC" -mode replica -db "$DB_REPLICA" -listen ":$PORT_REPLICA" > "$SCRIPT_DIR/corr-replica.log" 2>&1 &
for i in $(seq 1 20); do curl -s "http://localhost:$PORT_REPLICA/health" 2>/dev/null | grep -q ok && break; sleep 0.1; done

# Start primary
"$WALSYNC" -mode primary -db "$DB_PRIMARY" -replicas "localhost:$PORT_REPLICA" > "$SCRIPT_DIR/corr-primary.log" 2>&1 &
sleep 0.5

# Write initial data
sqlite3 "$DB_PRIMARY" "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE items(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, value INTEGER); INSERT INTO items(name,value) VALUES('before-checkpoint',1);"
sleep 1

# Verify sync
REPLICA_COUNT=$(sqlite3 "$DB_REPLICA" "SELECT COUNT(*) FROM items" 2>/dev/null || echo "0")
echo "  before checkpoint: replica count = $REPLICA_COUNT"
if [ "$REPLICA_COUNT" != "1" ]; then echo "  FAIL: expected 1 row before checkpoint"; FAIL=1; fi

# Force checkpoint on primary (simulates app restart or explicit checkpoint)
sqlite3 "$DB_PRIMARY" "PRAGMA wal_checkpoint(TRUNCATE);"
sleep 2

# Write more data after checkpoint
sqlite3 "$DB_PRIMARY" "INSERT INTO items(name,value) VALUES('after-checkpoint',2);"
sleep 2

# Verify replica sees both rows
REPLICA_COUNT=$(sqlite3 "$DB_REPLICA" "SELECT COUNT(*) FROM items" 2>/dev/null || echo "0")
echo "  after checkpoint: replica count = $REPLICA_COUNT"
if [ "$REPLICA_COUNT" != "2" ]; then echo "  FAIL: expected 2 rows after checkpoint, got $REPLICA_COUNT"; FAIL=1; else echo "  PASS: checkpoint recovery works"; fi

# Check primary log for snapshot ship
if grep -q "snapshot" "$SCRIPT_DIR/corr-primary.log" 2>/dev/null; then
  echo "  PASS: primary shipped snapshot after checkpoint"
else
  echo "  WARN: no snapshot log found (may have used WAL salt detection)"
fi

echo ""
echo "=== Corruption Test 2: Salt Mismatch Recovery ==="
echo "  (truncate replica WAL → primary ships snapshot via salt validation)"

# Truncate replica WAL (simulates WAL recreation with different salt)
echo "" > "$DB_REPLICA-wal" 2>/dev/null || true
rm -f "$DB_REPLICA-shm" 2>/dev/null || true

# Write new data to primary — should trigger salt mismatch → snapshot
sqlite3 "$DB_PRIMARY" "INSERT INTO items(name,value) VALUES('after-salt-mismatch',3);"
sleep 2

REPLICA_COUNT=$(sqlite3 "$DB_REPLICA" "SELECT COUNT(*) FROM items" 2>/dev/null || echo "0")
echo "  after salt mismatch: replica count = $REPLICA_COUNT"
if [ "$REPLICA_COUNT" != "3" ]; then echo "  FAIL: expected 3 rows after salt recovery, got $REPLICA_COUNT"; FAIL=1; else echo "  PASS: salt mismatch recovery works"; fi

# Check logs for salt mismatch detection
if grep -q "salt mismatch" "$SCRIPT_DIR/corr-replica.log" 2>/dev/null; then
  echo "  PASS: replica detected salt mismatch"
elif grep -q "snapshot" "$SCRIPT_DIR/corr-primary.log" 2>/dev/null; then
  echo "  PASS: primary shipped snapshot (salt validation triggered)"
else
  echo "  INFO: no explicit salt mismatch log (snapshot may have been triggered by other detection)"
fi

echo ""
echo "=== Corruption Test 3: CLI Access on Replica ==="
echo "  (sqlite3 CLI readwrite on replica → checkpoint → recovery)"

# Simulate the exact bug we found: someone runs sqlite3 CLI on replica DB
# This opens a readwrite connection, which checkpoints on close
sqlite3 "$DB_REPLICA" "SELECT COUNT(*) FROM items;" 2>/dev/null || true

# Write new data to primary
sqlite3 "$DB_PRIMARY" "INSERT INTO items(name,value) VALUES('after-cli-access',4);"
sleep 2

REPLICA_COUNT=$(sqlite3 "$DB_REPLICA" "SELECT COUNT(*) FROM items" 2>/dev/null || echo "0")
echo "  after CLI access: replica count = $REPLICA_COUNT"
if [ "$REPLICA_COUNT" != "4" ]; then echo "  FAIL: expected 4 rows after CLI recovery, got $REPLICA_COUNT"; FAIL=1; else echo "  PASS: CLI access recovery works"; fi

# Verify all 4 rows are correct
REPLICA_ROWS=$(sqlite3 "$DB_REPLICA" "SELECT name FROM items ORDER BY id" 2>/dev/null)
echo "  replica rows:"
echo "$REPLICA_ROWS" | while read -r row; do echo "    - $row"; done

echo ""
if [ "$FAIL" = "0" ]; then
  echo "=== ALL CORRUPTION TESTS PASSED ==="
else
  echo "=== SOME CORRUPTION TESTS FAILED ==="
fi

cleanup
exit $FAIL
