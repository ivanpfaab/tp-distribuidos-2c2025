# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod and go sum files from root
COPY go.mod go.sum ./
COPY shared ./shared
COPY protocol ./protocol

# Download dependencies
RUN go mod download

# Copy source code
COPY workers/group_by/ ./workers/group_by/

# Build the application
WORKDIR /app/workers/group_by
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .
RUN chmod +x ./main

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/workers/group_by/ .
RUN chmod +x ./main

# Run the application
CMD ["./main"]
