FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the year filter worker
RUN CGO_ENABLED=0 GOOS=linux go build -o year-filter-worker ./workers/filter/year-filter/

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/year-filter-worker .

# Run the year filter worker
CMD ["./year-filter-worker"]
