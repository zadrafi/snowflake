#!/usr/bin/env bash
# run_cross_cloud_tests.sh — Run the full test suite against AWS, Azure, and GCP
#
# Usage:
#   ./poc/scripts/run_cross_cloud_tests.sh              # all 3 clouds
#   ./poc/scripts/run_cross_cloud_tests.sh aws azure     # specific clouds
#   ./poc/scripts/run_cross_cloud_tests.sh --e2e-only    # only E2E tests
#   ./poc/scripts/run_cross_cloud_tests.sh --sql-only    # only SQL tests
#
# Prerequisites:
#   - Snowflake connections configured: aws_spcs, azure_spcs, gcp_spcs
#   - POC venv at poc/.venv with all dependencies installed
#   - Playwright browsers installed (for E2E)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
POC_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV="$POC_DIR/.venv/bin"
STREAMLIT_PORT=8504

# Default clouds and test modes
CLOUDS=()
RUN_SQL=true
RUN_E2E=true

# Parse arguments
for arg in "$@"; do
  case "$arg" in
    --e2e-only) RUN_SQL=false ;;
    --sql-only) RUN_E2E=false ;;
    aws|azure|gcp) CLOUDS+=("$arg") ;;
    *) echo "Unknown argument: $arg"; exit 1 ;;
  esac
done

# Default to all 3 clouds if none specified
if [ ${#CLOUDS[@]} -eq 0 ]; then
  CLOUDS=(aws azure gcp)
fi

# Connection name mapping
declare -A CONN_MAP=(
  [aws]=aws_spcs
  [azure]=azure_spcs
  [gcp]=gcp_spcs
)

# Results tracking
declare -A SQL_RESULTS
declare -A E2E_RESULTS

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

kill_streamlit() {
  local pids
  pids=$(lsof -t -i ":$STREAMLIT_PORT" 2>/dev/null || true)
  if [ -n "$pids" ]; then
    kill $pids 2>/dev/null || true
    sleep 2
    # Force kill if still running
    pids=$(lsof -t -i ":$STREAMLIT_PORT" 2>/dev/null || true)
    if [ -n "$pids" ]; then
      kill -9 $pids 2>/dev/null || true
      sleep 1
    fi
  fi
}

start_streamlit() {
  local conn="$1"
  echo "  Starting Streamlit with POC_CONNECTION=$conn ..."
  cd "$POC_DIR/streamlit"
  POC_CONNECTION="$conn" \
    POC_DB=AI_EXTRACT_POC \
    POC_SCHEMA=DOCUMENTS \
    POC_WH=AI_EXTRACT_WH \
    "$VENV/python" -m streamlit run streamlit_app.py \
      --server.port "$STREAMLIT_PORT" \
      --server.headless true \
      --browser.gatherUsageStats false &>/dev/null &
  cd "$POC_DIR"

  # Wait for server
  for i in $(seq 1 30); do
    if curl -s -o /dev/null -w "%{http_code}" "http://localhost:$STREAMLIT_PORT" 2>/dev/null | grep -q 200; then
      echo "  Streamlit ready after ${i}s"
      return 0
    fi
    sleep 1
  done
  echo "  ERROR: Streamlit failed to start after 30s"
  return 1
}

run_sql_tests() {
  local conn="$1"
  echo "  Running SQL integration tests ..."
  cd "$POC_DIR"
  POC_CONNECTION="$conn" "$VENV/python" -m pytest tests/ \
    --ignore=tests/test_e2e \
    --timeout=120 \
    -v --tb=short \
    -W ignore::DeprecationWarning \
    2>&1 | tail -5
  return "${PIPESTATUS[0]}"
}

run_e2e_tests() {
  echo "  Running E2E browser tests ..."
  cd "$POC_DIR"
  "$VENV/python" -m pytest tests/test_e2e/ \
    --timeout=30 \
    -v --tb=short \
    -W ignore::DeprecationWarning \
    2>&1 | tail -5
  return "${PIPESTATUS[0]}"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

echo "============================================"
echo "Cross-Cloud Test Runner"
echo "Clouds: ${CLOUDS[*]}"
echo "SQL tests: $RUN_SQL | E2E tests: $RUN_E2E"
echo "============================================"
echo ""

for cloud in "${CLOUDS[@]}"; do
  conn="${CONN_MAP[$cloud]}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Cloud: $cloud  |  Connection: $conn"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # SQL tests
  if [ "$RUN_SQL" = true ]; then
    if run_sql_tests "$conn"; then
      SQL_RESULTS[$cloud]="PASS"
    else
      SQL_RESULTS[$cloud]="FAIL"
    fi
    echo ""
  fi

  # E2E tests
  if [ "$RUN_E2E" = true ]; then
    kill_streamlit
    if start_streamlit "$conn"; then
      if run_e2e_tests; then
        E2E_RESULTS[$cloud]="PASS"
      else
        E2E_RESULTS[$cloud]="FAIL"
      fi
    else
      E2E_RESULTS[$cloud]="SKIP (server failed)"
    fi
    kill_streamlit
    echo ""
  fi
done

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

echo ""
echo "============================================"
echo "  CROSS-CLOUD TEST SUMMARY"
echo "============================================"
printf "%-8s | %-15s | %-15s\n" "Cloud" "SQL Tests" "E2E Tests"
printf "%-8s-+-%-15s-+-%-15s\n" "--------" "---------------" "---------------"
for cloud in "${CLOUDS[@]}"; do
  sql_result="${SQL_RESULTS[$cloud]:-N/A}"
  e2e_result="${E2E_RESULTS[$cloud]:-N/A}"
  printf "%-8s | %-15s | %-15s\n" "$cloud" "$sql_result" "$e2e_result"
done
echo "============================================"

# Exit with failure if any test failed
for cloud in "${CLOUDS[@]}"; do
  if [ "${SQL_RESULTS[$cloud]:-}" = "FAIL" ] || [ "${E2E_RESULTS[$cloud]:-}" = "FAIL" ]; then
    exit 1
  fi
done
