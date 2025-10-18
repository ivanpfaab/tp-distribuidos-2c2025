#!/bin/bash

# Comprehensive Query Finalization Test Suite
# Runs all test scenarios and verifies proper finalization

set -e

echo "=========================================="
echo "Query Finalization Test Suite"
echo "Testing N-to-M synchronization protocol"
echo "=========================================="

# Change to the test directory
cd "$(dirname "$0")/.."

# Test scenarios
SCENARIOS=("1map-1reduce" "1map-manyreduce" "manymap-1reduce" "manymap-manyreduce")
CLIENTS="CLI1,CLI2,CLI3"

# Results tracking
PASSED=0
FAILED=0
TOTAL=0

for scenario in "${SCENARIOS[@]}"; do
    echo ""
    echo "=========================================="
    echo "Running Test: $scenario"
    echo "=========================================="
    
    TOTAL=$((TOTAL + 1))
    
    # Run the test
    if ./test_scripts/run_test.sh "$scenario" "$CLIENTS"; then
        echo "✅ Test $scenario: PASSED"
        PASSED=$((PASSED + 1))
    else
        echo "❌ Test $scenario: FAILED"
        FAILED=$((FAILED + 1))
    fi
    
    # Wait between tests
    echo "Waiting 10 seconds before next test..."
    sleep 10
done

# Final summary
echo ""
echo "=========================================="
echo "Test Suite Summary"
echo "=========================================="
echo "Total Tests: $TOTAL"
echo "Passed: $PASSED"
echo "Failed: $FAILED"
echo "Success Rate: $((PASSED * 100 / TOTAL))%"

if [ $FAILED -eq 0 ]; then
    echo ""
    echo "🎉 All tests PASSED!"
    echo "Query finalization protocol is working correctly for N-to-M synchronization"
    exit 0
else
    echo ""
    echo "💥 Some tests FAILED!"
    echo "Query finalization protocol needs attention"
    exit 1
fi
