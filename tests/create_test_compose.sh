#!/bin/bash

# Create test docker compose file
echo "Creating test docker compose file..."

# Copy the test compose file to the root directory
cp docker-compose.test.yaml ../docker-compose.test.yaml

echo "Test docker compose file created successfully!"
echo "You can now run: docker-compose -f docker-compose.test.yaml up --build"
