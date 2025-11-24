# Build stage
FROM golang:1.21-alpine AS builder

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o user-join-worker ./workers/join/in-file/user-id/user-join

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

# Create shared data directory
RUN mkdir -p /shared-data

# Create worker data directory for state files
RUN mkdir -p /app/worker-data

# Set working directory
WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/user-join-worker .

# Create a non-root user
RUN adduser -D -s /bin/sh appuser && \
    chown -R appuser:appuser /root && \
    chown -R appuser:appuser /shared-data && \
    chown -R appuser:appuser /app/worker-data

# Switch to non-root user
USER appuser

# Expose port (if needed)
EXPOSE 8080

# Command to run the application
CMD ["./user-join-worker"]
