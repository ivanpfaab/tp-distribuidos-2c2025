FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o itemid-join-worker ./workers/join/in-memory/itemid

FROM alpine:latest

RUN apk --no-cache add ca-certificates
WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/itemid-join-worker .

# Expose port (if needed)
EXPOSE 8080

# Run the application
CMD ["./itemid-join-worker"]
