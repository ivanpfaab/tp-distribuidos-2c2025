#!/bin/bash

# Multi-client integration test runner
# This script runs the multi-client connection tests

set -e  # Exit on any error

echo "Running Multi-Client Integration Tests..."
echo "========================================"

# Change to the tests directory
cd "$(dirname "$0")"

# Check if required directories exist
if [ ! -d "../shared/rabbitmq" ]; then
    echo "Error: shared/rabbitmq directory not found!"
    echo "Make sure the shared source code is available"
    exit 1
fi

# Install dependencies
echo "Installing dependencies..."
go mod tidy

# Run the unit tests first (no RabbitMQ required)
echo "Running Multi-Client Unit Tests..."
go test -v -run TestMultiClientUnitTests -timeout 30s

echo ""
echo "Running Multi-Client Connection Integration Test..."
echo "Note: This test requires RabbitMQ to be running"
go test -v -run TestMultiClientConnection -timeout 60s

echo ""
echo "Running Message Queue Simulation Tests..."
go test -v -run TestMessageQueueSimulation -timeout 30s

echo ""
echo "Running Concurrent Client Connection Tests..."
go test -v -run TestConcurrentClientConnections -timeout 30s

echo ""
echo "Multi-client tests completed successfully!"
