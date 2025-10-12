# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
COPY protocol/ ./protocol/
COPY shared/ ./shared/

# Download dependencies
RUN go mod download

# Copy reduce worker source and semester utils
COPY workers/group_by/query2_mapreduce/reduce_workers/ ./workers/group_by/query2_mapreduce/reduce_workers/
COPY workers/group_by/query2_mapreduce/reduce_workers/semester_utils.go ./workers/group_by/query2_mapreduce/reduce_workers/

# Build the reduce workers (all executables)
WORKDIR /app/workers/group_by/query2_mapreduce/reduce_workers
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o reduce-worker-s2-2023 main_s2_2023.go reduce_worker.go semester_utils.go && \
    CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o reduce-worker-s1-2024 main_s1_2024.go reduce_worker.go semester_utils.go && \
    CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o reduce-worker-s2-2024 main_s2_2024.go reduce_worker.go semester_utils.go && \
    CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o reduce-worker-s1-2025 main_s1_2025.go reduce_worker.go semester_utils.go && \
    CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o reduce-worker-s2-2025 main_s2_2025.go reduce_worker.go semester_utils.go

# Run stage
FROM alpine:latest
RUN apk --no-cache add ca-certificates

WORKDIR /root/
COPY --from=builder /app/workers/group_by/query2_mapreduce/reduce_workers/reduce-worker-* ./

# Default command (will be overridden in docker-compose)
CMD ["./reduce-worker-s2-2023"]
