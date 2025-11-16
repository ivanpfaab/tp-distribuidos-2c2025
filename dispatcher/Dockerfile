# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the results dispatcher
RUN CGO_ENABLED=0 GOOS=linux go build -o results-dispatcher ./dispatcher

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/results-dispatcher .

# Expose port (if needed for health checks)
EXPOSE 8080

# Run the results dispatcher
CMD ["./results-dispatcher"]
