#!/bin/bash

# Test Data Sender Script
# Sends actual test data to the streaming exchange via RabbitMQ

set -e

CLIENTS=${1:-"CLI1,CLI2,CLI3"}
RABBITMQ_HOST=${2:-"localhost"}
RABBITMQ_PORT=${3:-"5673"}

echo "=========================================="
echo "Sending Test Data via RabbitMQ"
echo "Clients: $CLIENTS"
echo "RabbitMQ: $RABBITMQ_HOST:$RABBITMQ_PORT"
echo "=========================================="

# Wait for RabbitMQ to be ready
echo "Waiting for RabbitMQ to be ready..."
for i in {1..30}; do
    if nc -z "$RABBITMQ_HOST" "$RABBITMQ_PORT" 2>/dev/null; then
        echo "✅ RabbitMQ is ready!"
        break
    fi
    echo "Waiting... ($i/30)"
    sleep 2
done

# Send test data using Go (direct to map worker queue)
echo "Sending test data..."
cd "$(dirname "$0")"
go run send_test_data_direct.go "$RABBITMQ_HOST" "$RABBITMQ_PORT" "$CLIENTS"

echo "=========================================="
echo "Test data sending completed!"
echo "=========================================="