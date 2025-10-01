# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod and go sum files from root
COPY go.mod go.sum ./

# Copy all necessary directories
COPY protocol/ ./protocol/
COPY shared/ ./shared/
COPY server/ ./server/

# Download dependencies
RUN go mod download

# Change to server directory and build
WORKDIR /app/server

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/server/main .

# Expose port
EXPOSE 8080

# Run the application
CMD ["./main"]