# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
COPY protocol/ ./protocol/
COPY shared/ ./shared/

# Download dependencies
RUN go mod download

# Copy reduce worker source
COPY workers/group_by/query4_mapreduce/reduce_worker/ ./workers/group_by/query4_mapreduce/reduce_worker/

# Build the reduce worker
WORKDIR /app/workers/group_by/query4_mapreduce/reduce_worker
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o reduce-worker .

# Run stage
FROM alpine:latest
RUN apk --no-cache add ca-certificates

WORKDIR /root/
COPY --from=builder /app/workers/group_by/query4_mapreduce/reduce_worker/reduce-worker .

CMD ["./reduce-worker"]
