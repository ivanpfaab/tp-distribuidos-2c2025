FROM golang:1.21-alpine

# Install dependencies
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go mod files first for better caching
COPY tests/go.mod ./
COPY shared/middleware/go.mod ./shared/middleware/
COPY shared/middleware/exchange/go.mod ./shared/middleware/exchange/
COPY shared/middleware/workerqueue/go.mod ./shared/middleware/workerqueue/
COPY protocol/batch/go.mod ./protocol/batch/
COPY protocol/chunk/go.mod ./protocol/chunk/
COPY protocol/common/go.mod ./protocol/common/
COPY protocol/deserializer/go.mod ./protocol/deserializer/
COPY shared/testing/go.mod ./shared/testing/

# Copy source code
COPY . .

# Change to tests directory first
WORKDIR /app/tests

# Download dependencies and tidy
RUN go mod download
RUN go mod tidy

# Set environment variables for unbuffered output
ENV PYTHONUNBUFFERED=1
ENV GOCACHE=/tmp/go-cache

CMD ["go", "test", "-v", "./..."]
