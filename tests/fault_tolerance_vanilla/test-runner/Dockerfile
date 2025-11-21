FROM golang:1.23-alpine

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire project (needed for imports)
COPY . .

# Build the test runner
WORKDIR /app/tests/fault_tolerance/test-runner
RUN go build -o test-runner .

# Run the test runner
CMD ["./test-runner"]

