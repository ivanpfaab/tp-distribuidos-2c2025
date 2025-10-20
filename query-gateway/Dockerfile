FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the query gateway
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o query-gateway ./query-gateway/

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/query-gateway .

# Set execute permissions
RUN chmod +x query-gateway

# Run the query gateway
CMD ["./query-gateway"]
