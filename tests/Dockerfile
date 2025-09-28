FROM golang:1.21-alpine

# Install dependencies
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go mod and go sum files from root
COPY go.mod go.sum ./

# Copy source code
COPY . .

# Change to tests directory first
WORKDIR /app/tests

# Download dependencies
RUN go mod download

# Set environment variables for unbuffered output
ENV PYTHONUNBUFFERED=1
ENV GOCACHE=/tmp/go-cache

CMD ["go", "test", "-v", "./..."]
