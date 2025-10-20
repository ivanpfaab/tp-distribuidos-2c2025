# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the orchestrator
RUN CGO_ENABLED=0 GOOS=linux go build -o orchestrator ./workers/join/in-memory/orchestrator

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/orchestrator .

# Copy shared data directory (if needed)
RUN mkdir -p /shared-data

# Set environment variables
ENV RABBITMQ_HOST=rabbitmq
ENV RABBITMQ_PORT=5672
ENV RABBITMQ_USER=admin
ENV RABBITMQ_PASS=password

# Expose port (if needed)
# EXPOSE 8080

# Run the orchestrator
CMD ["./orchestrator"]
