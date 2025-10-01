# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the streaming service
RUN CGO_ENABLED=0 GOOS=linux go build -o streaming-service ./stream

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/streaming-service .

# Expose port (if needed for health checks)
EXPOSE 8080

# Run the streaming service
CMD ["./streaming-service"]
