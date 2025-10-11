FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application for each semester
RUN CGO_ENABLED=0 GOOS=linux go build -o reduce_s2_2023 main_s2_2023.go reduce_worker.go
RUN CGO_ENABLED=0 GOOS=linux go build -o reduce_s1_2024 main_s1_2024.go reduce_worker.go
RUN CGO_ENABLED=0 GOOS=linux go build -o reduce_s2_2024 main_s2_2024.go reduce_worker.go
RUN CGO_ENABLED=0 GOOS=linux go build -o reduce_s1_2025 main_s1_2025.go reduce_worker.go
RUN CGO_ENABLED=0 GOOS=linux go build -o reduce_s2_2025 main_s2_2025.go reduce_worker.go

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binaries
COPY --from=builder /app/reduce_s2_2023 .
COPY --from=builder /app/reduce_s1_2024 .
COPY --from=builder /app/reduce_s2_2024 .
COPY --from=builder /app/reduce_s1_2025 .
COPY --from=builder /app/reduce_s2_2025 .

# Copy semester utils
COPY --from=builder /app/semester_utils.go .

# Default command (can be overridden)
CMD ["./reduce_s2_2023"]
