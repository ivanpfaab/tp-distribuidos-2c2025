FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the time filter worker
RUN CGO_ENABLED=0 GOOS=linux go build -o time-filter-worker ./workers/filter/time-filter/

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/time-filter-worker .

# Run the time filter worker
CMD ["./time-filter-worker"]
