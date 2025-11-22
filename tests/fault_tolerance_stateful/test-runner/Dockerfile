FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build test-runner
WORKDIR /app/tests/fault_tolerance_stateful/test-runner
RUN CGO_ENABLED=0 GOOS=linux go build -o test-runner .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /app/tests/fault_tolerance_stateful/test-runner/test-runner .

CMD ["./test-runner"]

