FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o partitioner ./workers/group_by/shared/partitioner

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/partitioner .

# Set execute permissions
RUN chmod +x partitioner

# Set environment variables
ENV QUERY_TYPE=2
ENV RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672/
ENV NUM_PARTITIONS=10
ENV MAX_BUFFER_SIZE=200

# Run the application
CMD ["./partitioner"]