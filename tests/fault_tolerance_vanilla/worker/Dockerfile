FROM golang:1.23-alpine

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire project (needed for imports)
COPY . .

# Build the worker
WORKDIR /app/tests/fault_tolerance/worker
RUN go build -o worker .

# Run the worker
CMD ["./worker"]

