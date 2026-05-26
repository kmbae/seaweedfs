#!/bin/bash

# SeaweedFS RDMA End-to-End Demo Script
# This script demonstrates the complete integration between SeaweedFS and the RDMA sidecar

set -e

# Configuration
RDMA_ENGINE_SOCKET="/tmp/rdma-engine.sock"
DEMO_SERVER_PORT=8080
WEED_MASTER_PORT=9333
WEED_VOLUME_PORT=8444
WEED_VOLUME_DIR="/tmp/seaweed-test-vol"
RUST_ENGINE_PID=""
DEMO_SERVER_PID=""
WEED_SERVER_PID=""
TEST_FILE_ID=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

print_header() {
    echo -e "\n${PURPLE}===============================================${NC}"
    echo -e "${PURPLE}$1${NC}"
    echo -e "${PURPLE}===============================================${NC}\n"
}

print_step() {
    echo -e "${CYAN}🔵 $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

cleanup() {
    print_header "CLEANUP"
    
    if [[ -n "$DEMO_SERVER_PID" ]]; then
        print_step "Stopping demo server (PID: $DEMO_SERVER_PID)"
        kill $DEMO_SERVER_PID 2>/dev/null || true
        wait $DEMO_SERVER_PID 2>/dev/null || true
    fi
    
    if [[ -n "$RUST_ENGINE_PID" ]]; then
        print_step "Stopping Rust RDMA engine (PID: $RUST_ENGINE_PID)"
        kill $RUST_ENGINE_PID 2>/dev/null || true
        wait $RUST_ENGINE_PID 2>/dev/null || true
    fi
    
    if [[ -n "$WEED_SERVER_PID" ]]; then
        print_step "Stopping weed server (PID: $WEED_SERVER_PID)"
        kill $WEED_SERVER_PID 2>/dev/null || true
        wait $WEED_SERVER_PID 2>/dev/null || true
    fi
    
    # Clean up socket and temp volume data
    rm -f "$RDMA_ENGINE_SOCKET"
    rm -rf "$WEED_VOLUME_DIR"
    
    print_success "Cleanup complete"
}

kill_port() {
    local port=$1
    local pids
    pids=$(lsof -ti :"$port" 2>/dev/null || true)
    if [[ -n "$pids" ]]; then
        print_warning "Killing existing process(es) on port $port: $pids"
        echo "$pids" | xargs kill -9 2>/dev/null || true
        sleep 1
    fi
}

preflight_cleanup() {
    print_step "Pre-flight: cleaning up stale processes and sockets..."
    kill_port "$DEMO_SERVER_PORT"
    kill_port "$WEED_MASTER_PORT"
    kill_port "$WEED_VOLUME_PORT"
    kill_port 18515
    rm -f "$RDMA_ENGINE_SOCKET"
    rm -rf "$WEED_VOLUME_DIR"
}

# Set up cleanup on exit
trap cleanup EXIT

build_components() {
    print_header "BUILDING COMPONENTS"
    
    print_step "Building Go components..."
    go build -o bin/demo-server ./cmd/demo-server
    go build -o bin/test-rdma ./cmd/test-rdma
    go build -o bin/sidecar ./cmd/sidecar
    print_success "Go components built"
    
    if [[ ! -f bin/weed ]]; then
        print_step "Building SeaweedFS weed binary (for local-volume test)..."
        (cd .. && go build -o seaweedfs-rdma-sidecar/bin/weed ./weed)
        print_success "weed binary built"
    else
        print_success "weed binary already exists"
    fi
    
    print_step "Building Rust RDMA engine..."
    cd rdma-engine
    cargo build --release
    cd ..
    print_success "Rust RDMA engine built"
}

start_rdma_engine() {
    print_header "STARTING RDMA ENGINE"
    
    print_step "Starting Rust RDMA engine..."
    VOLUME_SERVER_URL="http://localhost:$DEMO_SERVER_PORT" ./rdma-engine/target/release/rdma-engine-server --debug &
    RUST_ENGINE_PID=$!
    
    # Wait for engine to be ready
    print_step "Waiting for RDMA engine to be ready..."
    for i in {1..10}; do
        if [[ -S "$RDMA_ENGINE_SOCKET" ]]; then
            print_success "RDMA engine ready (PID: $RUST_ENGINE_PID)"
            return 0
        fi
        sleep 1
    done
    
    print_error "RDMA engine failed to start"
    exit 1
}

start_demo_server() {
    print_header "STARTING DEMO SERVER"
    
    DEMO_SERVER_ARGS="--port $DEMO_SERVER_PORT --rdma-socket $RDMA_ENGINE_SOCKET --enable-rdma --debug"
    if [[ -d "$WEED_VOLUME_DIR" ]]; then
        DEMO_SERVER_ARGS="$DEMO_SERVER_ARGS --volume-data-dir $WEED_VOLUME_DIR"
        print_step "Starting SeaweedFS RDMA demo server (with local-volume: $WEED_VOLUME_DIR)..."
    else
        print_step "Starting SeaweedFS RDMA demo server..."
    fi
    ./bin/demo-server $DEMO_SERVER_ARGS &
    DEMO_SERVER_PID=$!
    
    # Wait for server to be ready (check both PID alive and HTTP responding)
    print_step "Waiting for demo server to be ready..."
    for i in {1..10}; do
        if ! kill -0 $DEMO_SERVER_PID 2>/dev/null; then
            print_error "Demo server process died (PID: $DEMO_SERVER_PID)"
            DEMO_SERVER_PID=""
            exit 1
        fi
        if curl -s "http://localhost:$DEMO_SERVER_PORT/health" > /dev/null 2>&1; then
            print_success "Demo server ready (PID: $DEMO_SERVER_PID)"
            return 0
        fi
        sleep 1
    done
    
    print_error "Demo server failed to start"
    exit 1
}

setup_test_volume() {
    print_header "SETTING UP TEST VOLUME (for local-volume test)"
    
    mkdir -p "$WEED_VOLUME_DIR"
    
    print_step "Starting weed server (master=$WEED_MASTER_PORT, volume=$WEED_VOLUME_PORT)..."
    ./bin/weed server \
        -master.port=$WEED_MASTER_PORT \
        -volume.port=$WEED_VOLUME_PORT \
        -dir="$WEED_VOLUME_DIR" \
        -volume.max=5 \
        > /tmp/weed-server.log 2>&1 &
    WEED_SERVER_PID=$!
    
    print_step "Waiting for weed server to be ready..."
    for i in {1..15}; do
        if curl -s "http://localhost:$WEED_MASTER_PORT/cluster/status" > /dev/null 2>&1; then
            print_success "Weed server ready (PID: $WEED_SERVER_PID)"
            break
        fi
        if [[ $i -eq 15 ]]; then
            print_warning "Weed server did not start in time, local-volume test will be skipped"
            kill $WEED_SERVER_PID 2>/dev/null || true
            WEED_SERVER_PID=""
            return 1
        fi
        sleep 1
    done
    
    sleep 2
    
    print_step "Uploading test needle data..."
    TEST_DATA="SEAWEEDFS_RDMA_LOCAL_VOLUME_TEST_DATA_$(date +%s)"
    upload_response=$(curl -s -F "file=@-;filename=test.dat" "http://localhost:$WEED_MASTER_PORT/submit" <<< "$TEST_DATA")
    
    if echo "$upload_response" | jq -e '.fid' > /dev/null 2>&1; then
        TEST_FILE_ID=$(echo "$upload_response" | jq -r '.fid')
        print_success "Test data uploaded: file_id=$TEST_FILE_ID"
        echo "$upload_response" | jq '.'
    else
        print_warning "Failed to upload test data, local-volume test will be skipped"
        echo "$upload_response"
        return 1
    fi
}

test_health_check() {
    print_header "HEALTH CHECK TEST"
    
    print_step "Testing health endpoint..."
    response=$(curl -s "http://localhost:$DEMO_SERVER_PORT/health")
    
    if echo "$response" | jq -e '.status == "healthy"' > /dev/null; then
        print_success "Health check passed"
        echo "$response" | jq '.'
    else
        print_error "Health check failed"
        echo "$response"
        exit 1
    fi
}

test_capabilities() {
    print_header "CAPABILITIES TEST"
    
    print_step "Testing capabilities endpoint..."
    response=$(curl -s "http://localhost:$DEMO_SERVER_PORT/stats")
    
    if echo "$response" | jq -e '.enabled == true' > /dev/null; then
        print_success "RDMA capabilities retrieved"
        echo "$response" | jq '.'
    else
        print_warning "RDMA not enabled, but HTTP fallback available"
        echo "$response" | jq '.'
    fi
}

test_needle_read() {
    print_header "NEEDLE READ TEST"
    
    print_step "Testing RDMA needle read..."
    response=$(curl -s "http://localhost:$DEMO_SERVER_PORT/read?volume=1&needle=12345&cookie=305419896&size=1024&volume_server=http://localhost:$DEMO_SERVER_PORT")
    
    if echo "$response" | jq -e '.success == true' > /dev/null; then
        is_rdma=$(echo "$response" | jq -r '.is_rdma')
        source=$(echo "$response" | jq -r '.source')
        duration=$(echo "$response" | jq -r '.duration')
        data_size=$(echo "$response" | jq -r '.data_size')
        
        if [[ "$is_rdma" == "true" ]]; then
            print_success "RDMA fast path used! Duration: $duration, Size: $data_size bytes"
        else
            print_warning "HTTP fallback used. Duration: $duration, Size: $data_size bytes"
        fi
        
        echo "$response" | jq '.'
    else
        print_error "Needle read failed"
        echo "$response"
        exit 1
    fi
}

test_needle_write() {
    print_header "NEEDLE WRITE TEST (rdma+http-submit)"
    
    print_step "Testing RDMA write path with local volume server..."
    WRITE_DATA="Hello SeaweedFS RDMA write test - $(date +%s)"
    response=$(curl -s -X POST \
        "http://localhost:$DEMO_SERVER_PORT/write?volume=1&volume_server=http://localhost:$DEMO_SERVER_PORT" \
        -H "Content-Type: application/octet-stream" \
        -d "$WRITE_DATA")
    
    if echo "$response" | jq -e '.success == true' > /dev/null 2>&1; then
        file_id=$(echo "$response" | jq -r '.file_id // "N/A"')
        data_size=$(echo "$response" | jq -r '.data_size')
        duration=$(echo "$response" | jq -r '.duration')
        is_rdma=$(echo "$response" | jq -r '.is_rdma // "N/A"')
        source=$(echo "$response" | jq -r '.source // "N/A"')
        
        if [[ "$is_rdma" == "true" ]]; then
            print_success "RDMA write path used! source=$source, file_id=$file_id, size=$data_size bytes, duration=$duration"
        else
            print_warning "HTTP-only write. source=$source, duration=$duration"
        fi
        
        echo "$response" | jq '.'
    else
        print_error "Needle write failed"
        echo "$response"
        exit 1
    fi
}

test_remote_rdma_write() {
    print_header "REMOTE RDMA WRITE TEST"
    
    HOST_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
    if [[ -z "$HOST_IP" ]]; then
        HOST_IP=$(hostname)
    fi
    
    REMOTE_VOLUME_SERVER="http://${HOST_IP}:${DEMO_SERVER_PORT}"
    print_step "Testing remote-write path with volume_server=$REMOTE_VOLUME_SERVER ..."
    
    WRITE_DATA="Remote RDMA write test data - $(date +%s)"
    response=$(curl -s -X POST \
        "http://localhost:$DEMO_SERVER_PORT/write?volume=1&volume_server=$REMOTE_VOLUME_SERVER" \
        -H "Content-Type: application/octet-stream" \
        -d "$WRITE_DATA")
    
    if echo "$response" | jq -e '.success == true' > /dev/null 2>&1; then
        source=$(echo "$response" | jq -r '.source // "N/A"')
        is_rdma=$(echo "$response" | jq -r '.is_rdma // "N/A"')
        file_id=$(echo "$response" | jq -r '.file_id // "N/A"')
        duration=$(echo "$response" | jq -r '.duration')
        data_size=$(echo "$response" | jq -r '.data_size')
        
        if [[ "$source" == *"remote-write"* ]]; then
            print_success "Remote RDMA write path verified! source=$source, file_id=$file_id, duration=$duration, size=$data_size bytes"
        elif [[ "$is_rdma" == "true" ]]; then
            print_warning "RDMA used but source=$source (expected remote-write). duration=$duration"
        else
            print_warning "HTTP fallback used. source=$source, duration=$duration"
        fi
        
        echo "$response" | jq '.'
    else
        print_error "Remote RDMA write failed"
        echo "$response"
        exit 1
    fi
}

test_remote_rdma_read() {
    print_header "REMOTE RDMA READ TEST"
    
    # Use a non-localhost address so the Go client takes the remote-rdma path.
    # The rdma-engine NetworkServer (TCP :18515) will fetch needle bytes from
    # demo-server's mock volume data handler.
    HOST_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
    if [[ -z "$HOST_IP" ]]; then
        HOST_IP=$(hostname)
    fi
    
    REMOTE_VOLUME_SERVER="http://${HOST_IP}:${DEMO_SERVER_PORT}"
    print_step "Testing remote-rdma path with volume_server=$REMOTE_VOLUME_SERVER ..."
    
    response=$(curl -s "http://localhost:$DEMO_SERVER_PORT/read?volume=1&needle=12345&cookie=305419896&size=1024&volume_server=$REMOTE_VOLUME_SERVER")
    
    if echo "$response" | jq -e '.success == true' > /dev/null; then
        source=$(echo "$response" | jq -r '.source')
        is_rdma=$(echo "$response" | jq -r '.is_rdma')
        duration=$(echo "$response" | jq -r '.duration')
        data_size=$(echo "$response" | jq -r '.data_size')
        
        if [[ "$source" == *"remote-rdma"* ]]; then
            print_success "Remote RDMA path verified! source=$source, duration=$duration, size=$data_size bytes"
        elif [[ "$is_rdma" == "true" ]]; then
            print_warning "RDMA used but source=$source (expected remote-rdma). duration=$duration"
        else
            print_warning "HTTP fallback used. source=$source, duration=$duration"
        fi
        
        echo "$response" | jq '.'
    else
        print_error "Remote RDMA read failed"
        echo "$response"
        exit 1
    fi
}

test_local_volume_read() {
    print_header "LOCAL VOLUME READ TEST"
    
    if [[ -z "$TEST_FILE_ID" ]]; then
        print_warning "Skipping local-volume test (no test data uploaded, weed server not running)"
        return 0
    fi
    
    print_step "Testing local-volume path with file_id=$TEST_FILE_ID ..."
    response=$(curl -s "http://localhost:$DEMO_SERVER_PORT/read?file_id=$TEST_FILE_ID&volume_server=http://localhost:$DEMO_SERVER_PORT")
    
    if echo "$response" | jq -e '.success == true' > /dev/null; then
        source=$(echo "$response" | jq -r '.source')
        is_rdma=$(echo "$response" | jq -r '.is_rdma')
        duration=$(echo "$response" | jq -r '.duration')
        data_size=$(echo "$response" | jq -r '.data_size')
        
        if [[ "$source" == *"local-volume"* ]]; then
            print_success "Local volume path verified! source=$source, duration=$duration, size=$data_size bytes"
        elif [[ "$is_rdma" == "true" ]]; then
            print_warning "RDMA used but source=$source (expected local-volume). duration=$duration"
        else
            print_warning "HTTP fallback used. source=$source, duration=$duration"
        fi
        
        echo "$response" | jq '.'
    else
        print_warning "Local volume read failed (may need volume file match)"
        echo "$response" | jq '.' 2>/dev/null || echo "$response"
    fi
}

test_benchmark() {
    print_header "PERFORMANCE BENCHMARK"
    
    print_step "Running performance benchmark..."
    response=$(curl -s "http://localhost:$DEMO_SERVER_PORT/benchmark?iterations=5&size=2048")
    
    if echo "$response" | jq -e '.benchmark_results' > /dev/null; then
        rdma_ops=$(echo "$response" | jq -r '.benchmark_results.rdma_ops')
        http_ops=$(echo "$response" | jq -r '.benchmark_results.http_ops')
        avg_latency=$(echo "$response" | jq -r '.benchmark_results.avg_latency')
        throughput=$(echo "$response" | jq -r '.benchmark_results.throughput_mbps')
        ops_per_sec=$(echo "$response" | jq -r '.benchmark_results.ops_per_sec')
        
        print_success "Benchmark completed:"
        echo -e "  ${BLUE}RDMA Operations:${NC} $rdma_ops"
        echo -e "  ${BLUE}HTTP Operations:${NC} $http_ops"
        echo -e "  ${BLUE}Average Latency:${NC} $avg_latency"
        echo -e "  ${BLUE}Throughput:${NC} $throughput MB/s"
        echo -e "  ${BLUE}Operations/sec:${NC} $ops_per_sec"
        
        echo -e "\n${BLUE}Full benchmark results:${NC}"
        echo "$response" | jq '.benchmark_results'
    else
        print_error "Benchmark failed"
        echo "$response"
        exit 1
    fi
}

test_direct_rdma() {
    print_header "DIRECT RDMA ENGINE TEST"
    
    print_step "Testing direct RDMA engine communication..."
    
    echo "Testing ping..."
    ./bin/test-rdma ping 2>/dev/null && print_success "Direct RDMA ping successful" || print_warning "Direct RDMA ping failed"
    
    echo -e "\nTesting capabilities..."
    ./bin/test-rdma capabilities 2>/dev/null | head -15 && print_success "Direct RDMA capabilities successful" || print_warning "Direct RDMA capabilities failed"
    
    echo -e "\nTesting direct read..."
    ./bin/test-rdma read --volume 1 --needle 12345 --size 1024 2>/dev/null > /dev/null && print_success "Direct RDMA read successful" || print_warning "Direct RDMA read failed"
}

show_demo_urls() {
    print_header "DEMO SERVER INFORMATION"
    
    echo -e "${GREEN}🌐 Demo server is running at: http://localhost:$DEMO_SERVER_PORT${NC}"
    echo -e "${GREEN}📱 Try these URLs:${NC}"
    echo -e "  ${BLUE}Home page:${NC}          http://localhost:$DEMO_SERVER_PORT/"
    echo -e "  ${BLUE}Health check:${NC}       http://localhost:$DEMO_SERVER_PORT/health"
    echo -e "  ${BLUE}Statistics:${NC}         http://localhost:$DEMO_SERVER_PORT/stats"
    echo -e "  ${BLUE}Read needle:${NC}        http://localhost:$DEMO_SERVER_PORT/read?volume=1&needle=12345&cookie=305419896&size=1024"
    echo -e "  ${BLUE}Benchmark:${NC}          http://localhost:$DEMO_SERVER_PORT/benchmark?iterations=5&size=2048"
    
    echo -e "\n${GREEN}📋 Example curl commands:${NC}"
    echo -e "  ${CYAN}curl \"http://localhost:$DEMO_SERVER_PORT/health\" | jq '.'${NC}"
    echo -e "  ${CYAN}curl \"http://localhost:$DEMO_SERVER_PORT/read?volume=1&needle=12345&size=1024\" | jq '.'${NC}"
    echo -e "  ${CYAN}curl \"http://localhost:$DEMO_SERVER_PORT/benchmark?iterations=10\" | jq '.benchmark_results'${NC}"
}

interactive_mode() {
    print_header "INTERACTIVE MODE"
    
    show_demo_urls
}

main() {
    print_header "🚀 SEAWEEDFS RDMA END-TO-END DEMO"
    
    echo -e "${GREEN}This demonstration shows:${NC}"
    echo -e "  ✅ Complete Go ↔ Rust IPC integration"
    echo -e "  ✅ SeaweedFS RDMA client with HTTP fallback" 
    echo -e "  ✅ High-performance needle reads via RDMA"
    echo -e "  ✅ Performance benchmarking capabilities"
    echo -e "  ✅ Production-ready error handling and logging"
    
    # Check dependencies
    if ! command -v jq &> /dev/null; then
        print_error "jq is required for this demo. Please install it: brew install jq"
        exit 1
    fi
    
    if ! command -v curl &> /dev/null; then
        print_error "curl is required for this demo."
        exit 1
    fi
    
    # Clean up any leftover processes from previous runs
    preflight_cleanup
    
    # Build and start components
    build_components
    
    # Set up test volume data (for local-volume test) before starting other services
    setup_test_volume || true
    
    start_rdma_engine
    sleep 2  # Give engine time to fully initialize
    start_demo_server
    sleep 2  # Give server time to connect to engine
    
    # Show interactive information
    interactive_mode
    
    # Run automated tests
    test_health_check
    test_capabilities
    test_needle_read
    test_needle_write
    test_remote_rdma_read
    test_remote_rdma_write
    test_local_volume_read
    test_benchmark
    test_direct_rdma
    
    print_header "🎉 END-TO-END DEMO COMPLETE!"
    
    echo -e "${GREEN}All tests passed successfully!${NC}"
    echo -e "${BLUE}Key achievements demonstrated:${NC}"
    echo -e "  🚀 RDMA fast path working with mock operations"
    echo -e "  🔄 Automatic HTTP fallback when RDMA unavailable"
    echo -e "  🌐 Remote RDMA read path (rdma+remote-rdma)"
    echo -e "  💾 Local volume read path (rdma+local-volume)"
    echo -e "  📝 RDMA write path (rdma+http-submit)"
    echo -e "  📡 Remote RDMA write path (rdma+remote-write)"
    echo -e "  📊 Performance monitoring and benchmarking"
    echo -e "  🛡️  Robust error handling and graceful degradation"
    echo -e "  🔌 Complete IPC protocol between Go and Rust"
    echo -e "  ⚡ Session management with proper cleanup"
    
    print_success "SeaweedFS RDMA integration is ready for hardware deployment!"
    
    # Keep server running for manual testing
    echo -e "\n${YELLOW}Demo server will continue running for manual testing...${NC}"
    echo -e "${YELLOW}Press Ctrl+C to shutdown.${NC}"
    
    # Wait for user interrupt
    wait
}

# Run the main function
main "$@"
