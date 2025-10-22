# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
COPY protocol/ ./protocol/
COPY shared/ ./shared/

# Download dependencies
RUN go mod download

# Copy map worker source
COPY workers/group_by/query4_mapreduce/map_worker/ ./workers/group_by/query4_mapreduce/map_worker/

# Build the map worker
WORKDIR /app/workers/group_by/query4_mapreduce/map_worker
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o map-worker .

# Run stage
FROM alpine:latest
RUN apk --no-cache add ca-certificates

WORKDIR /root/
COPY --from=builder /app/workers/group_by/query4_mapreduce/map_worker/map-worker .

CMD ["./map-worker"]
