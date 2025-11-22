FROM golang:1.23-alpine

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire project (needed for imports)
COPY . .

# Build the final consumer
WORKDIR /app/tests/fault_tolerance/final-consumer
RUN go build -o final-consumer .

# Run the final consumer
CMD ["./final-consumer"]

