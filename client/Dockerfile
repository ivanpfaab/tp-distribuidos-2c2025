# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY client/go.mod ./

# Copy shared module first (needed for go mod download)
COPY shared ./shared

# Download dependencies
RUN go mod download

# Copy source code
COPY client/ .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/main .

# Run the application
CMD ["./main"]
