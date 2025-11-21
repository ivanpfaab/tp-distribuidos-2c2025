FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY tests/fault_tolerance_partition_writes/worker-3 ./tests/fault_tolerance_partition_writes/worker-3
COPY shared ./shared
COPY protocol ./protocol

# Build
WORKDIR /app/tests/fault_tolerance_partition_writes/worker-3
RUN go build -o worker-3 .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/tests/fault_tolerance_partition_writes/worker-3/worker-3 .
CMD ["./worker-3"]

