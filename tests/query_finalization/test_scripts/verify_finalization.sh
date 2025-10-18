#!/bin/bash

# Query Finalization Verification Script
# Verifies that all reduce workers received completion signals from all map workers

set -e

SCENARIO=${1:-"1map-1reduce"}
CLIENTS=${2:-"CLI1,CLI2,CLI3"}

echo "=========================================="
echo "Verifying Query Finalization: $SCENARIO"
echo "Clients: $CLIENTS"
echo "=========================================="

# Change to the test directory
cd "$(dirname "$0")/.."

# Get all reduce worker containers
REDUCE_CONTAINERS=$(docker-compose -f docker-compose/$SCENARIO.yaml ps -q | grep reduce || true)

if [ -z "$REDUCE_CONTAINERS" ]; then
    echo "❌ No reduce worker containers found!"
    exit 1
fi

# Get all map worker containers
MAP_CONTAINERS=$(docker-compose -f docker-compose/$SCENARIO.yaml ps -q | grep map || true)

if [ -z "$MAP_CONTAINERS" ]; then
    echo "❌ No map worker containers found!"
    exit 1
fi

# Count expected map workers
EXPECTED_MAP_WORKERS=$(echo "$MAP_CONTAINERS" | wc -l)
echo "Expected map workers: $EXPECTED_MAP_WORKERS"

# Check each reduce worker
SUCCESS=true
IFS=',' read -ra CLIENT_ARRAY <<< "$CLIENTS"

for reduce_container in $REDUCE_CONTAINERS; do
    container_name=$(docker inspect --format='{{.Name}}' $reduce_container | sed 's/\///')
    echo "--- Checking $container_name ---"
    
    for client in "${CLIENT_ARRAY[@]}"; do
        echo "  Checking client: $client"
        
        # Count completion signals for this client using new log format
        completion_count=$(docker logs $reduce_container 2>&1 | grep -c "\[CLI$client.*Q3.*Map worker.*completed" || true)
        finalization_count=$(docker logs $reduce_container 2>&1 | grep -c "\[CLI$client.*Q3.*✅ All.*map workers completed" || true)
        
        if [ "$completion_count" -eq "$EXPECTED_MAP_WORKERS" ] && [ "$finalization_count" -gt 0 ]; then
            echo "    ✅ Client $client: $completion_count/$EXPECTED_MAP_WORKERS map workers completed + finalization"
        else
            echo "    ❌ Client $client: $completion_count/$EXPECTED_MAP_WORKERS map workers completed, finalization: $finalization_count"
            SUCCESS=false
        fi
        
        # Additional check for finalization (already checked above, but keeping for clarity)
        if [ "$finalization_count" -gt 0 ]; then
            echo "    ✅ Client $client: Finalization completed"
        else
            echo "    ❌ Client $client: Finalization not completed"
            SUCCESS=false
        fi
    done
    echo
done

# Summary
echo "=========================================="
if [ "$SUCCESS" = true ]; then
    echo "✅ All tests PASSED!"
    echo "All reduce workers received completion signals from all map workers"
    echo "All clients were properly finalized"
else
    echo "❌ Some tests FAILED!"
    echo "Not all reduce workers received completion signals from all map workers"
    exit 1
fi
echo "=========================================="
