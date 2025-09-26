#!/bin/bash

# Docker Integration Test Runner
# This script runs the real Docker integration tests with actual containers
# communicating via Docker network through RabbitMQ

set -e  # Exit on any error

echo "Running Docker Integration Tests..."
echo "=================================="
echo "This will test real Docker containers communicating via RabbitMQ"
echo ""

# Change to the tests directory
cd "$(dirname "$0")"

# Check if Docker and Docker Compose are available
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed or not in PATH"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "Error: docker-compose is not installed or not in PATH"
    exit 1
fi

# Set environment variable to enable Docker tests
export DOCKER_TEST=true

echo "Building and starting Docker containers..."
echo "This includes:"
echo "  - RabbitMQ container (message broker)"
echo "  - Echo Server container (processes messages)"
echo "  - Multiple Echo Client containers (send messages)"
echo "  - Test Runner container (validates communication)"
echo ""

# Run the Docker integration test
echo "Starting Docker Compose integration test..."
docker-compose -f docker-compose.integration-test.yml up --build --abort-on-container-exit

# Get the exit code
EXIT_CODE=$?

echo ""
echo "Docker Compose finished with exit code: $EXIT_CODE"

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Docker integration tests PASSED!"
    echo ""
    echo "This means:"
    echo "  - All containers started successfully"
    echo "  - RabbitMQ is running and accessible"
    echo "  - Server container can receive messages via RabbitMQ"
    echo "  - Client containers can send messages via RabbitMQ"
    echo "  - Network communication between containers works"
    echo "  - Message queuing and routing works correctly"
else
    echo "❌ Docker integration tests FAILED!"
    echo ""
    echo "Check the logs above for details on what went wrong."
fi

echo ""
echo "Cleaning up containers..."
docker-compose -f docker-compose.integration-test.yml down -v

echo ""
echo "Docker integration test completed!"

