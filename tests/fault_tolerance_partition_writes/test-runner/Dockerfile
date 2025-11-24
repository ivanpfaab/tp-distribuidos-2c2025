FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY tests/fault_tolerance_partition_writes/test-runner ./tests/fault_tolerance_partition_writes/test-runner
COPY shared ./shared
COPY protocol ./protocol

# Build
WORKDIR /app/tests/fault_tolerance_partition_writes/test-runner
RUN go build -o test-runner .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/tests/fault_tolerance_partition_writes/test-runner/test-runner .
CMD ["./test-runner"]

