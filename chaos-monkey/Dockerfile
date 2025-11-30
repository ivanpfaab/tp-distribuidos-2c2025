FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY chaos-monkey/ ./chaos-monkey/

RUN CGO_ENABLED=0 GOOS=linux go build -o chaos-monkey ./chaos-monkey/

FROM docker:cli AS docker-source

FROM alpine:latest

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=docker-source /usr/local/bin/docker /usr/local/bin/docker
COPY --from=builder /app/chaos-monkey .

CMD ["./chaos-monkey"]

