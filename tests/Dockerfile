# Use official Go image as base
FROM golang:1.21-alpine

# Install git and make
RUN apk add --no-cache git make bash

# Set working directory
WORKDIR /app

# Copy the entire project structure
COPY . .

# Change to tests directory
WORKDIR /app/tests

# Download dependencies and generate go.sum
RUN go mod tidy

# Make the test runners executable
RUN chmod +x run_tests.sh
RUN chmod +x run_multi_client_test.sh

# Default command to run tests
CMD ["bash", "./run_tests.sh"]
