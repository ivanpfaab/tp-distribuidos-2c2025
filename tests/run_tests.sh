#!/bin/bash

# Test runner script for protocol tests
# This script runs all protocol tests and provides detailed output

set -e  # Exit on any error

echo "Running Protocol Tests..."
echo "========================="

# Change to the tests directory
cd "$(dirname "$0")"

# Check if protocol directory exists
if [ ! -d "./protocol" ]; then
    echo "Error: protocol directory not found!"
    echo "Make sure the protocol source code is mounted or copied to ./protocol"
    exit 1
fi

# Run tests with verbose output
echo "Running Batch Protocol Tests..."
go test -v ./protocol -run TestBatch

echo ""
echo "Running Chunk Protocol Tests..."
go test -v ./protocol -run TestChunk

echo ""
echo "Running All Protocol Tests..."
go test -v ./protocol

echo ""
echo "Test Coverage Report..."
go test -v -cover ./protocol

echo ""
echo "All tests completed successfully!"
