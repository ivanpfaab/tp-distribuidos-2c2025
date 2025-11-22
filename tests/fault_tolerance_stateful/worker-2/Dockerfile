FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build worker
WORKDIR /app/tests/fault_tolerance_stateful/worker-2
RUN CGO_ENABLED=0 GOOS=linux go build -o worker-2 .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /app/tests/fault_tolerance_stateful/worker-2/worker-2 .

CMD ["./worker-2"]

