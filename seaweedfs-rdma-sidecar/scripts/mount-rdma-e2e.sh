#!/bin/bash

# Mount RDMA Integration E2E Test
# Tests the weed mount RDMA client paths (read + write) against
# rdma-engine + demo-server.
#
# Usage:
#   ./scripts/mount-rdma-e2e.sh              # full e2e (mock RDMA)
#   ./scripts/mount-rdma-e2e.sh --real-ucx   # full e2e (real UCX RDMA via InfiniBand)
#   ./scripts/mount-rdma-e2e.sh --skip-build # skip build step
#   ./scripts/mount-rdma-e2e.sh --unit-only  # run Go unit tests only

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

RDMA_ENGINE_SOCKET="/tmp/rdma-engine.sock"
DEMO_SERVER_PORT=8092
RUST_ENGINE_PID=""
DEMO_SERVER_PID=""
SKIP_BUILD=false
UNIT_ONLY=false
REAL_UCX=false

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m'

print_header() { echo -e "\n${PURPLE}=== $1 ===${NC}\n"; }
print_step()   { echo -e "${CYAN}-> $1${NC}"; }
print_ok()     { echo -e "${GREEN}OK: $1${NC}"; }
print_warn()   { echo -e "${YELLOW}WARN: $1${NC}"; }
print_fail()   { echo -e "${RED}FAIL: $1${NC}"; }

for arg in "$@"; do
    case "$arg" in
        --skip-build) SKIP_BUILD=true ;;
        --unit-only)  UNIT_ONLY=true ;;
        --real-ucx)   REAL_UCX=true ;;
    esac
done

cleanup() {
    print_header "CLEANUP"
    [[ -n "$DEMO_SERVER_PID" ]] && { kill $DEMO_SERVER_PID 2>/dev/null || true; wait $DEMO_SERVER_PID 2>/dev/null || true; }
    [[ -n "$RUST_ENGINE_PID" ]] && { kill $RUST_ENGINE_PID 2>/dev/null || true; wait $RUST_ENGINE_PID 2>/dev/null || true; }
    rm -f "$RDMA_ENGINE_SOCKET"
    lsof -ti :"$DEMO_SERVER_PORT" 2>/dev/null | xargs kill -9 2>/dev/null || true
    print_ok "Cleanup complete"
}
trap cleanup EXIT

# Pre-flight: kill stale processes from previous runs
print_step "Pre-flight cleanup..."
lsof -ti :"$DEMO_SERVER_PORT" 2>/dev/null | xargs kill -9 2>/dev/null || true
rm -f "$RDMA_ENGINE_SOCKET"

# ── Step 1: Go Unit Tests ──────────────────────────────────────────

run_unit_tests() {
    print_header "GO UNIT TESTS (RDMAMountClient)"
    print_step "Running weed/mount RDMA unit tests..."

    cd "$PROJECT_DIR/.."
    if go test ./weed/mount/ -run TestRDMAMountClient -v -count=1; then
        print_ok "All unit tests passed"
    else
        print_fail "Unit tests failed"
        exit 1
    fi
    cd "$PROJECT_DIR"
}

run_unit_tests

if $UNIT_ONLY; then
    echo ""
    print_ok "Unit-only mode: done."
    exit 0
fi

# ── Step 2: Build ──────────────────────────────────────────────────

if ! $SKIP_BUILD; then
    print_header "BUILD"

    print_step "Building Go test binaries..."
    go build -o bin/demo-server ./cmd/demo-server
    go build -o bin/test-mount-rdma ./cmd/test-mount-rdma
    print_ok "Go binaries built"

    print_step "Building Rust RDMA engine..."
    CARGO_FEATURES=""
    if $REAL_UCX; then
        CARGO_FEATURES="--features real-ucx"
        print_step "Building with REAL UCX RDMA support (InfiniBand)"
    fi
    cd rdma-engine && cargo build --release $CARGO_FEATURES 2>&1 | tail -1 && cd ..
    print_ok "RDMA engine built"
else
    print_header "BUILD (skipped)"
fi

# ── Step 3: Start rdma-engine ──────────────────────────────────────

print_header "START RDMA ENGINE"
rm -f "$RDMA_ENGINE_SOCKET"

if $REAL_UCX; then
    print_step "Starting with REAL UCX RDMA (InfiniBand hardware)"
else
    print_step "Starting with mock RDMA"
fi

VOLUME_SERVER_URL="http://localhost:$DEMO_SERVER_PORT" \
    ./rdma-engine/target/release/rdma-engine-server \
    --ipc-socket "$RDMA_ENGINE_SOCKET" --port 0 --debug &
RUST_ENGINE_PID=$!

print_step "Waiting for RDMA engine socket..."
for i in {1..10}; do
    if [[ -S "$RDMA_ENGINE_SOCKET" ]]; then
        print_ok "RDMA engine ready (PID: $RUST_ENGINE_PID)"
        break
    fi
    sleep 1
done
if [[ ! -S "$RDMA_ENGINE_SOCKET" ]]; then
    print_fail "RDMA engine failed to start"
    exit 1
fi

# ── Step 4: Start demo-server (acts as sidecar) ───────────────────

print_header "START DEMO SERVER"

./bin/demo-server \
    --port $DEMO_SERVER_PORT \
    --rdma-socket "$RDMA_ENGINE_SOCKET" \
    --enable-rdma \
    --debug &
DEMO_SERVER_PID=$!

print_step "Waiting for demo server..."
for i in {1..10}; do
    if curl -s "http://localhost:$DEMO_SERVER_PORT/health" > /dev/null 2>&1; then
        print_ok "Demo server ready (PID: $DEMO_SERVER_PID)"
        break
    fi
    if ! kill -0 $DEMO_SERVER_PID 2>/dev/null; then
        print_fail "Demo server process died"
        exit 1
    fi
    sleep 1
done

# ── Step 5: Integration Tests ─────────────────────────────────────

print_header "MOUNT RDMA INTEGRATION TESTS"

PASSED=0
FAILED=0

run_test() {
    local name="$1"
    shift
    echo -e "\n${BLUE}--- TEST: $name ---${NC}"
    if "$@"; then
        print_ok "$name"
        PASSED=$((PASSED + 1))
    else
        print_fail "$name"
        FAILED=$((FAILED + 1))
    fi
}

# Test: Health
test_health() {
    local resp
    resp=$(curl -s "http://localhost:$DEMO_SERVER_PORT/health")
    local status
    status=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "")
    if [[ "$status" == "healthy" ]]; then
        echo "  Status: $status"
        return 0
    fi
    echo "  Response: $resp"
    return 1
}

# Test: Read via sidecar HTTP (mount client path)
test_read() {
    local file_id="$1"
    local size="${2:-4096}"
    local volume_server="http://localhost:$DEMO_SERVER_PORT"
    local url="http://localhost:$DEMO_SERVER_PORT/read?file_id=${file_id}&offset=0&size=${size}&volume_server=${volume_server}"

    local http_code body headers
    headers=$(mktemp)
    body=$(curl -s -D "$headers" -o - -w '\n%{http_code}' "$url")
    http_code=$(echo "$body" | tail -1)
    body=$(echo "$body" | head -n -1)

    local source rdma_used
    source=$(grep -i "X-Source:" "$headers" | tr -d '\r' | awk '{print $2}')
    rdma_used=$(grep -i "X-RDMA-Used:" "$headers" | tr -d '\r' | awk '{print $2}')
    rm -f "$headers"

    echo "  file_id=$file_id, size=$size"
    echo "  HTTP status: $http_code"
    echo "  X-Source: $source"
    echo "  X-RDMA-Used: $rdma_used"
    echo "  Body length: ${#body}"

    [[ "$http_code" == "200" ]] || return 1
    [[ -n "$source" ]] || return 1
    return 0
}

# Test: Write via sidecar HTTP (mount client path)
test_write() {
    local file_id="$1"
    local data_size="${2:-1024}"
    local volume_server="http://localhost:$DEMO_SERVER_PORT"
    local url="http://localhost:$DEMO_SERVER_PORT/write?file_id=${file_id}&volume_server=${volume_server}"

    local payload
    payload=$(head -c "$data_size" /dev/urandom | base64 | head -c "$data_size")

    local resp
    resp=$(curl -s -X POST -H "Content-Type: application/octet-stream" -d "$payload" "$url")

    local success is_rdma source resp_size
    success=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('success',False))" 2>/dev/null || echo "")
    is_rdma=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('is_rdma',False))" 2>/dev/null || echo "")
    source=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('source',''))" 2>/dev/null || echo "")
    resp_size=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('size',0))" 2>/dev/null || echo "")

    echo "  file_id=$file_id, data_size=$data_size"
    echo "  success=$success, is_rdma=$is_rdma"
    echo "  source=$source, size=$resp_size"

    [[ "$success" == "True" ]] || return 1
    [[ "$is_rdma" == "True" ]] || return 1
    return 0
}

# Test: test-mount-rdma binary (all-in-one)
test_mount_binary() {
    if [[ -x bin/test-mount-rdma ]]; then
        ./bin/test-mount-rdma --sidecar "localhost:$DEMO_SERVER_PORT" all
    else
        echo "  bin/test-mount-rdma not found, skipping"
        return 0
    fi
}

# Run tests
run_test "Health Check" test_health
run_test "Read (small, 1KB)" test_read "3,01637037d6" 1024
run_test "Read (medium, 64KB)" test_read "3,01637037d6" 65536
run_test "Read (large, 1MB)" test_read "3,01637037d6" 1048576
run_test "Write (small, 1KB)" test_write "5,0a1b2c3d4e" 1024
run_test "Write (medium, 64KB)" test_write "5,0a1b2c3d4e" 65536
run_test "test-mount-rdma binary" test_mount_binary

# ── Results ────────────────────────────────────────────────────────

print_header "RESULTS"

if $REAL_UCX; then
    echo -e "  Mode:   ${CYAN}REAL UCX RDMA (InfiniBand)${NC}"
else
    echo -e "  Mode:   ${YELLOW}Mock RDMA${NC}"
fi
echo -e "  Passed: ${GREEN}$PASSED${NC}"
echo -e "  Failed: ${RED}$FAILED${NC}"
echo ""

if [[ $FAILED -gt 0 ]]; then
    print_fail "$FAILED test(s) failed"
    exit 1
fi

print_ok "All $PASSED tests passed!"
exit 0
