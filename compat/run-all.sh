#!/bin/bash
# run-all.sh — run all walsync compatibility tests
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

PASS=0
FAIL=0
FAILED_TESTS=""

run_test() {
  local name="$1"
  local script="$2"
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  if bash "$script" 2>&1; then
    PASS=$((PASS + 1))
  else
    FAIL=$((FAIL + 1))
    FAILED_TESTS="$FAILED_TESTS $name"
  fi
}

echo "╔══════════════════════════════════════════════╗"
echo "║  walsync v1.1.0 Compatibility Test Suite     ║"
echo "╚══════════════════════════════════════════════╝"

# Library compatibility tests
run_test "bun:sqlite"        "$SCRIPT_DIR/test-bun.sh"
run_test "better-sqlite3"    "$SCRIPT_DIR/test-better-sqlite3.sh"
run_test "node:sqlite"       "$SCRIPT_DIR/test-node-sqlite.sh"
run_test "python-sqlite3"    "$SCRIPT_DIR/test-python.sh"
run_test "php-pdo-sqlite"    "$SCRIPT_DIR/test-php.sh"
run_test "ruby-sqlite3"      "$SCRIPT_DIR/test-ruby.sh"
run_test "go-modernc-sqlite" "$SCRIPT_DIR/test-go.sh"

# Corruption recovery tests
run_test "corruption-recovery" "$SCRIPT_DIR/test-corruption.sh"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  RESULTS: $PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then
  echo "  FAILED:$FAILED_TESTS"
  exit 1
else
  echo "  ALL TESTS PASSED ✅"
  exit 0
fi
