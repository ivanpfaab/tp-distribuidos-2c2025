FROM golang:1.21 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o supervisor ./supervisor/

FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y ca-certificates docker.io && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/supervisor .

CMD ["./supervisor"]

