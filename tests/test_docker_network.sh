#!/bin/bash

# Simple Docker Network Test
# This script tests that containers can communicate via Docker network

set -e

echo "Testing Docker Network Communication..."
echo "====================================="

# Change to project root
cd "$(dirname "$0")/.."

echo "1. Starting RabbitMQ container..."
docker-compose -f tests/docker-compose.test.yml up -d rabbitmq

echo "2. Waiting for RabbitMQ to be ready..."
sleep 10

echo "3. Starting Server container..."
docker-compose -f tests/docker-compose.test.yml up -d server

echo "4. Waiting for Server to be ready..."
sleep 5

echo "5. Testing Client container communication..."
echo "   This will run a client container that connects to the server via RabbitMQ"

# Run a single client container
docker-compose -f tests/docker-compose.test.yml run --rm client-1

echo ""
echo "6. Checking container logs..."

echo "Server logs:"
docker-compose -f tests/docker-compose.test.yml logs server | tail -10

echo ""
echo "Client logs:"
docker-compose -f tests/docker-compose.test.yml logs client-1 | tail -10

echo ""
echo "7. Cleaning up..."
docker-compose -f tests/docker-compose.test.yml down

echo ""
echo "Docker network communication test completed!"
echo "If you see 'Server response:' messages above, the communication worked!"
