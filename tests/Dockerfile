FROM golang:1.21-alpine

# Install dependencies
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod ./
COPY shared/middleware/exchange/go.mod ./shared/middleware/exchange/
COPY shared/middleware/workerqueue/go.mod ./shared/middleware/workerqueue/
COPY protocol/batch/go.mod ./protocol/batch/
COPY protocol/chunk/go.mod ./protocol/chunk/

# Copy source code
COPY . .

# Download dependencies and tidy
RUN go mod download
RUN go mod tidy

# Change to tests directory and run tests
WORKDIR /app/tests
RUN go mod tidy

# Set environment variables for unbuffered output
ENV PYTHONUNBUFFERED=1
ENV GOCACHE=/tmp/go-cache

CMD ["go", "test", "-v", "./..."]
