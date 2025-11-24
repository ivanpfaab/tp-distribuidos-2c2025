FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY tests/fault_tolerance_partition_writes/worker-1 ./tests/fault_tolerance_partition_writes/worker-1
COPY shared ./shared
COPY protocol ./protocol

# Build
WORKDIR /app/tests/fault_tolerance_partition_writes/worker-1
RUN go build -o worker-1 .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/tests/fault_tolerance_partition_writes/worker-1/worker-1 .
CMD ["./worker-1"]

