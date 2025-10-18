#!/bin/bash

# Query Finalization Test Runner
# Usage: ./run_test.sh <scenario> [clients]
# Scenarios: 1map-1reduce, 1map-manyreduce, manymap-1reduce, manymap-manyreduce

set -e

SCENARIO=${1:-"1map-1reduce"}
CLIENTS=${2:-"CLI1,CLI2,CLI3"}

echo "=========================================="
echo "Query Finalization Test: $SCENARIO"
echo "Clients: $CLIENTS"
echo "=========================================="

# Change to the test directory
cd "$(dirname "$0")/.."

# Clean up any existing containers
echo "Cleaning up existing containers..."
docker-compose -f docker-compose/$SCENARIO.yaml down -v 2>/dev/null || true

# Start the test environment
echo "Starting test environment..."
docker-compose -f docker-compose/$SCENARIO.yaml up -d

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 30

# Send test data for each client
echo "Sending test data to query gateway..."
cd "$(dirname "$0")"
./send_test_data.sh "$CLIENTS" "localhost" "5673" || echo "⚠️  Test data sending failed, but continuing with test..."

# Change back to the test directory for monitoring
cd ".."

# Monitor logs for completion signals
echo "Monitoring completion signals..."
echo "Watching for completion signals and finalization messages..."

# Function to check for completion signals
check_completion() {
    local completed_clients=()
    local total_clients=$(echo "$CLIENTS" | tr ',' ' ' | wc -w)
    
    for container in $(docker-compose -f docker-compose/$SCENARIO.yaml ps -q | grep reduce); do
        local container_name=$(docker inspect --format='{{.Name}}' $container | sed 's/\///')
        local logs=$(docker logs $container 2>&1)
        
        # Check for completion signals for each client
        for client in $(echo "$CLIENTS" | tr ',' ' '); do
            if echo "$logs" | grep -q "\[CLI$client.*Q3.*✅ All.*map workers completed"; then
                if [[ ! " ${completed_clients[@]} " =~ " $client " ]]; then
                    completed_clients+=("$client")
                    echo "✅ [CLI$client] Found completion signal in $container_name"
                fi
            fi
        done
    done
    
    echo "Completed clients: ${#completed_clients[@]}/$total_clients (${completed_clients[*]})"
    return ${#completed_clients[@]}
}

# Monitor for 120 seconds with progress updates
echo "Starting 120-second monitoring period..."
for i in {1..60}; do
    echo -n "Progress: $((i*2))s/120s - "
    if check_completion; then
        echo "✅ All clients completed!"
        break
    fi
    sleep 2
done

# Final check
echo "Final completion check..."
if check_completion; then
    echo "✅ Test PASSED: All clients completed successfully!"
else
    echo "❌ Test FAILED: Not all clients completed"
fi

# Show final logs
echo "=========================================="
echo "Final Logs Summary"
echo "=========================================="

for container in $(docker-compose -f docker-compose/$SCENARIO.yaml ps -q); do
    container_name=$(docker inspect --format='{{.Name}}' $container | sed 's/\///')
    echo "--- $container_name ---"
    
    # Show key events for each component type
    if [[ $container_name == *"orchestrator"* ]]; then
        echo "ORCHESTRATOR EVENTS:"
        docker logs $container 2>&1
    elif [[ $container_name == *"map-worker"* ]]; then
        echo "MAP WORKER EVENTS:"
        docker logs $container 2>&1
    elif [[ $container_name == *"reduce"* ]]; then
        echo "REDUCE WORKER EVENTS:"
        docker logs $container 2>&1
    else
        echo "GENERAL EVENTS:"
        docker logs $container 2>&1
    fi
    echo
done

# Show completion summary
echo "=========================================="
echo "Completion Summary"
echo "=========================================="
for client in $(echo "$CLIENTS" | tr ',' ' '); do
    echo "CLI$client completion status:"
    for container in $(docker-compose -f docker-compose/$SCENARIO.yaml ps -q | grep reduce); do
        local container_name=$(docker inspect --format='{{.Name}}' $container | sed 's/\///')
        if docker logs $container 2>&1 | grep -q "\[CLI$client.*Q3.*✅ All.*map workers completed"; then
            echo "  ✅ $container_name: COMPLETED"
        else
            echo "  ❌ $container_name: NOT COMPLETED"
        fi
    done
    echo
done

# Clean up
echo "Cleaning up..."
docker-compose -f docker-compose/$SCENARIO.yaml down -v

echo "Test completed!"
