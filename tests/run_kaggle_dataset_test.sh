#!/bin/bash

# Kaggle Dataset Test Runner
# This script runs the comprehensive dataset processing tests

set -e

echo "=========================================="
echo "Kaggle Dataset Processing Test Runner"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
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

# Create data directory
mkdir -p ../data

# Check if dataset exists, if not create a sample dataset
if [ ! -f "../data/coffee_shop_transactions.csv" ]; then
    print_status "Dataset not found, creating sample dataset..."
    
    # Create a comprehensive sample dataset with 10,000 records
    print_status "Generating 10,000 sample coffee shop transactions..."
    
    # Create the header
    cat > ../data/coffee_shop_transactions.csv << 'EOF'
TransactionID,Timestamp,CustomerID,ProductID,ProductName,Category,Quantity,UnitPrice,TotalAmount,PaymentMethod,StoreID,StoreName,City,Country
EOF

    # Generate sample data using a simple script
    python3 -c "
import random
import datetime
import csv

# Sample data
products = ['Espresso', 'Latte', 'Cappuccino', 'Americano', 'Mocha', 'Frappuccino', 'Tea', 'Pastry', 'Sandwich', 'Salad']
categories = ['Beverage', 'Food', 'Dessert']
payment_methods = ['Credit Card', 'Cash', 'Mobile Payment', 'Gift Card']
stores = ['Downtown Store', 'Mall Location', 'Airport Store', 'University Store', 'Suburban Store']
cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose']
countries = ['USA', 'Canada', 'Mexico']

# Generate 10,000 records
with open('../data/coffee_shop_transactions.csv', 'a', newline='') as file:
    writer = csv.writer(file)
    
    for i in range(10000):
        product = random.choice(products)
        category = random.choice(categories)
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(2.0, 15.0), 2)
        total_amount = round(unit_price * quantity, 2)
        
        # Generate timestamp between 2023-07-01 and 2025-06-30
        start_date = datetime.datetime(2023, 7, 1)
        end_date = datetime.datetime(2025, 6, 30)
        random_days = random.randint(0, (end_date - start_date).days)
        timestamp = start_date + datetime.timedelta(days=random_days, hours=random.randint(0, 23), minutes=random.randint(0, 59))
        
        writer.writerow([
            f'TXN_{i+1:06d}',
            timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            f'CUST_{random.randint(1, 1000):05d}',
            f'PROD_{random.randint(1, 100):03d}',
            product,
            category,
            quantity,
            unit_price,
            total_amount,
            random.choice(payment_methods),
            f'STORE_{random.randint(1, 10):02d}',
            random.choice(stores),
            random.choice(cities),
            random.choice(countries)
        ])
        
        if (i + 1) % 1000 == 0:
            print(f'Generated {i + 1} records...')

print('Sample dataset created with 10,000 records')
"
    
    print_success "Sample dataset created: ../data/coffee_shop_transactions.csv"
else
    print_success "Dataset found: ../data/coffee_shop_transactions.csv"
fi

# Build and start the test environment
print_status "Building and starting test environment..."
docker compose -f docker-compose.test.yaml down --remove-orphans
docker compose -f docker-compose.test.yaml build --no-cache
docker compose -f docker-compose.test.yaml up -d

# Wait for services to be ready
print_status "Waiting for services to be ready..."
sleep 10

# Check if services are running
print_status "Checking service health..."
if ! docker compose -f docker-compose.test.yaml ps | grep -q "Up"; then
    print_error "Services failed to start"
    docker compose -f docker-compose.test.yaml logs
    exit 1
fi

print_success "All services are running"

# Run the Kaggle dataset tests
print_status "Running Kaggle dataset processing tests..."
echo "=========================================="

# Run the specific test
docker compose -f docker-compose.test.yaml exec test-runner go test -v ./client_request_handler/ -run TestKaggleDatasetProcessing

# Check test results
if [ $? -eq 0 ]; then
    print_success "Kaggle dataset tests completed successfully!"
else
    print_error "Kaggle dataset tests failed!"
    print_status "Showing test logs..."
    docker compose -f docker-compose.test.yaml logs test-runner
fi

# Run performance tests
print_status "Running performance tests..."
echo "=========================================="

docker compose -f docker-compose.test.yaml exec test-runner go test -v ./client_request_handler/ -run TestDatasetPerformance

if [ $? -eq 0 ]; then
    print_success "Performance tests completed successfully!"
else
    print_error "Performance tests failed!"
fi

# Show final statistics
print_status "Test Summary:"
echo "=========================================="
docker compose -f docker-compose.test.yaml exec test-runner go test -v ./client_request_handler/ -run TestKaggleDatasetProcessing -count=1 | grep -E "(PASS|FAIL|LogStep|LogSuccess)"

# Cleanup
print_status "Cleaning up..."
docker compose -f docker-compose.test.yaml down

print_success "All tests completed!"
