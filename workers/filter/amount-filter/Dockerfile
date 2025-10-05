FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the amount filter worker
RUN CGO_ENABLED=0 GOOS=linux go build -o amount-filter-worker ./workers/filter/amount-filter/

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/amount-filter-worker .

# Run the amount filter worker
CMD ["./amount-filter-worker"]
