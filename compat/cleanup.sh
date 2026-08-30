#!/bin/bash
# cleanup.sh — kill walsync processes and remove test DB files
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Kill walsync processes for our test ports
pkill -f "walsync.*test-primary" 2>/dev/null || true
pkill -f "walsync.*test-replica" 2>/dev/null || true

# Wait briefly for processes to exit
sleep 0.2

# Remove test DB files
rm -f "$SCRIPT_DIR"/test-primary.db* "$SCRIPT_DIR"/test-replica.db* 2>/dev/null || true
