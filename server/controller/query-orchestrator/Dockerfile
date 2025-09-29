# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod and go sum files from root
COPY go.mod go.sum ./

# Copy shared module first (needed for go mod download)
COPY shared ./shared
COPY protocol ./protocol

# Download dependencies
RUN go mod download

# Copy source code
COPY server/controller/query-orchestrator/ ./controller/query-orchestrator/

# Build the application
WORKDIR /app/controller/query-orchestrator
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .
RUN chmod +x ./main

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/controller/query-orchestrator/ .
RUN chmod +x ./main

# Run the application
CMD ["./main"]
