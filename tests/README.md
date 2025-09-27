# Protocol Tests

This directory contains comprehensive tests for the batch and chunk protocol implementations.

## Test Structure

- `batch_test.go` - Tests for batch message protocol
- `chunk_test.go` - Tests for chunk message protocol
- `run_tests.sh` - Test runner script
- `Dockerfile` - Docker image for running tests
- `docker-compose.test.yml` - Docker Compose configuration
- `Makefile` - Convenient make targets

## Running Tests

### Local Testing

```bash
# Run all tests
make test

# Run tests with verbose output
make test-verbose

# Run tests with coverage
make test-coverage

# Run specific test suites
make test-batch
make test-chunk
```

### Docker Testing

```bash
# Build Docker image
make docker-build

# Run all tests in Docker
make docker-test

# Run tests with coverage in Docker
make docker-coverage

# Run specific test suites in Docker
make docker-test-batch
make docker-test-chunk
```

### Using Docker Compose

```bash
# Run all tests
docker-compose -f docker-compose.test.yml run --rm protocol-tests

# Run tests with coverage
docker-compose -f docker-compose.test.yml run --rm test-coverage

# Run specific test command
docker-compose -f docker-compose.test.yml run --rm test-runner go test -v ./protocol -run TestBatch
```

## Test Coverage

The tests cover:
- Message creation and validation
- Serialization and deserialization
- Error handling for invalid inputs
- Byte-level structure verification
- Edge cases and boundary conditions
- All protocol constants and types

## Requirements

- Go 1.21+
- Docker (for containerized testing)
- Make (for convenience commands)
