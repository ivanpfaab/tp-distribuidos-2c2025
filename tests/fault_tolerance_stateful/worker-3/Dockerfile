FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build worker-3
WORKDIR /app/tests/fault_tolerance_stateful/worker-3
RUN CGO_ENABLED=0 GOOS=linux go build -o worker-3 .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /app/tests/fault_tolerance_stateful/worker-3/worker-3 .

CMD ["./worker-3"]

