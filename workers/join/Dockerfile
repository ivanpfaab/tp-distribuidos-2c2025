# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod ./
COPY workers/join/go.mod ./workers/join/
COPY shared ./shared
COPY protocol ./protocol

# Download dependencies
RUN go mod download

# Copy source code
COPY workers/join/ ./workers/join/

# Build the application
WORKDIR /app/workers/join
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .
RUN chmod +x ./main

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/workers/join/ .
RUN chmod +x ./main

# Run the application
CMD ["./main"]
