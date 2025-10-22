FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o groupby-worker ./workers/group_by/shared/worker

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/groupby-worker .

# Set execute permissions
RUN chmod +x groupby-worker

# Set environment variables (defaults, can be overridden)
ENV QUERY_TYPE=2
ENV WORKER_ID=0
ENV RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672/
ENV NUM_PARTITIONS=10
ENV NUM_WORKERS=3

# Run the application
CMD ["./groupby-worker"]
