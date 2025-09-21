# Test Makefile for Protocol Tests

.PHONY: help test test-batch test-chunk clean

# Default target
help:
	@echo "Available targets:"
	@echo "  test           - Run all tests using Docker (default)"
	@echo "  test-batch     - Run only batch protocol tests using Docker"
	@echo "  test-chunk     - Run only chunk protocol tests using Docker"
	@echo "  clean          - Clean build artifacts and Docker images"


# Default test target - uses Docker
test:
	@echo "Building Docker image and running all tests..."
	cd .. && docker build -f tests/Dockerfile -t protocol-tests .
	docker run --rm protocol-tests

# Run only batch tests using Docker
test-batch:
	@echo "Building Docker image and running batch protocol tests..."
	cd .. && docker build -f tests/Dockerfile -t protocol-tests .
	docker run --rm protocol-tests bash -c "go test -v ./protocol -run TestBatch"

# Run only chunk tests using Docker
test-chunk:
	@echo "Building Docker image and running chunk protocol tests..."
	cd .. && docker build -f tests/Dockerfile -t protocol-tests .
	docker run --rm protocol-tests bash -c "go test -v ./protocol -run TestChunk"

# Clean build artifacts and Docker images
clean:
	rm -f test-runner
	docker rmi protocol-tests 2>/dev/null || true
