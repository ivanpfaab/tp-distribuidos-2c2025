#!/bin/bash

# Script to run the data handler flow tests
# This script builds and runs the tests using docker-compose

echo "Starting Data Handler Flow Tests"
echo "================================="

# Build and run the tests
echo "Building and running tests with docker-compose..."
docker-compose -f docker-compose.test.yaml up --build --abort-on-container-exit

echo "Tests completed!"
