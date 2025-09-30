#!/bin/bash

# Real Dataset Test Runner
# This script sets up the real dataset and runs comprehensive tests

set -e

echo "=========================================="
echo "Real Dataset Processing Test Runner"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the right directory
if [ ! -f "docker-compose.test.yaml" ]; then
    print_error "Please run this script from the tests directory"
    exit 1
fi

# Step 1: Setup the real dataset
print_status "Step 1: Setting up real dataset..."
./setup_real_dataset.sh

if [ ! -f "../data/coffee_shop_transactions.csv" ]; then
    print_error "Failed to create dataset"
    exit 1
fi

# Show dataset info
record_count=$(wc -l < ../data/coffee_shop_transactions.csv)
file_size=$(du -h ../data/coffee_shop_transactions.csv | cut -f1)
print_success "Dataset ready: $((record_count - 1)) records, $file_size"

# Step 2: Build and start the test environment
print_status "Step 2: Building and starting test environment..."
docker compose -f docker-compose.test.yaml down --remove-orphans
docker compose -f docker-compose.test.yaml build --no-cache
docker compose -f docker-compose.test.yaml up -d

# Wait for services to be ready
print_status "Step 3: Waiting for services to be ready..."
sleep 10

# Check if services are running
print_status "Step 4: Checking service health..."
if ! docker compose -f docker-compose.test.yaml ps | grep -q "Up"; then
    print_error "Services failed to start"
    docker compose -f docker-compose.test.yaml logs
    exit 1
fi

print_success "All services are running"

# Step 5: Run the real dataset tests
print_status "Step 5: Running real dataset processing tests..."
echo "=========================================="

# Run the specific test
docker compose -f docker-compose.test.yaml exec test-runner go test -v ./client_request_handler/ -run TestKaggleDatasetProcessing

# Check test results
if [ $? -eq 0 ]; then
    print_success "Real dataset tests completed successfully!"
else
    print_error "Real dataset tests failed!"
    print_status "Showing test logs..."
    docker compose -f docker-compose.test.yaml logs test-runner
fi

# Step 6: Show final statistics
print_status "Step 6: Test Summary:"
echo "=========================================="
docker compose -f docker-compose.test.yaml exec test-runner go test -v ./client_request_handler/ -run TestKaggleDatasetProcessing -count=1 | grep -E "(PASS|FAIL|LogStep|LogSuccess)" || true

# Step 7: Show dataset processing performance
print_status "Step 7: Performance Analysis:"
echo "=========================================="
print_status "Dataset: $(basename ../data/coffee_shop_transactions.csv)"
print_status "Records: $((record_count - 1))"
print_status "File Size: $file_size"
print_status "Expected Batches: $((($record_count - 1 + 999) / 1000)) (1000 records per batch)"

# Cleanup
print_status "Step 8: Cleaning up..."
docker compose -f docker-compose.test.yaml down

print_success "Real dataset test completed!"
print_status "Dataset file preserved at: ../data/coffee_shop_transactions.csv"
