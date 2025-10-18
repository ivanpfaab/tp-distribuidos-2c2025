#!/bin/bash

# Test script to verify environment variables are working
echo "Testing environment variable functionality..."

# Test Query 3 reduce worker with environment variable
echo "Testing Query 3 reduce worker with QUERY3_EXPECTED_MAP_WORKERS=3..."

docker run --rm \
  -e QUERY3_EXPECTED_MAP_WORKERS=3 \
  -e SEMESTER=S1-2024 \
  docker-compose_test-query3-reduce-s1-2024 \
  /bin/sh -c 'echo "Environment variable QUERY3_EXPECTED_MAP_WORKERS: $QUERY3_EXPECTED_MAP_WORKERS"'

echo "Test completed!"
