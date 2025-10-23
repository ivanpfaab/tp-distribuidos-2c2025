# Test runner Dockerfile
FROM golang:1.23-alpine

WORKDIR /app

# Install dependencies including docker CLI
RUN apk add --no-cache git docker-cli

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire project (needed for imports)
COPY . .

# Set the working directory for tests
WORKDIR /app

# Default command will be overridden by docker-compose
CMD ["go", "test", "-v", "./tests/infile_join_blackbox_test", "-timeout", "5m"]
