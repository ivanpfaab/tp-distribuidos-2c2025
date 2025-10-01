# Test Makefile

.PHONY: test test-build test-run test-clean test-setup

# Run all tests (with real-time logs)
test: test-setup
	@echo "Running all tests with real-time logs..."
	docker compose -f docker-compose.test.yaml up --build

# Build test containers
test-build:
	@echo "Building test containers..."
	docker compose -f docker-compose.test.yaml build

# Run tests without rebuilding (buffered output)
test-buffered:
	@echo "Running tests (buffered output)..."
	docker compose -f docker-compose.test.yaml up

# Run tests without rebuilding (real-time logs)
test-run:
	@echo "Running tests with real-time logs..."
	docker compose -f docker-compose.test.yaml up

# Setup test environment
test-setup:
	@echo "Setting up test environment..."
	./create_test_compose.sh

# Run specific test package
test-middleware:
	@echo "Running middleware tests..."
	docker compose -f docker-compose.test.yaml run --rm test-runner go test -v ./tests/middleware/...

test-protocol:
	@echo "Running protocol tests..."
	docker compose -f docker-compose.test.yaml run --rm test-runner go test -v ./tests/protocol/...

test-filter:
	@echo "Running filter worker tests..."
	docker compose -f docker-compose.test.yaml run --rm test-runner go test -v ./tests/workers/filter_test.go

# Clean up
clean:
	@echo "Cleaning up..."
	docker compose down -v
	docker system prune -f


# Help
help:
	@echo "Available targets:"
	@echo "  test          - Run all tests with real-time logs (default)"
	@echo "  test-build    - Build test containers"
	@echo "  test-run      - Run tests without rebuilding (real-time logs)"
	@echo "  test-buffered - Run tests with buffered output"
	@echo "  test-clean    - Clean up test containers"
	@echo "  test-setup    - Setup test environment"
	@echo "  test-middleware - Run middleware tests only"
	@echo "  test-protocol   - Run protocol tests only"
	@echo "  help          - Show this help"
