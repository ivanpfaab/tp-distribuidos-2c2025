# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
WORKDIR /app/workers/join/in-file/user-id/user-partition-writer
RUN CGO_ENABLED=0 GOOS=linux go build -o /user-partition-writer .

# Runtime stage
FROM alpine:latest

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /user-partition-writer .

# Create shared data directory
RUN mkdir -p /shared-data

# Run the application
CMD ["./user-partition-writer"]

