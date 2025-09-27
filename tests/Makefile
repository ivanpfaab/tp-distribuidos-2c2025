# Test Makefile

.PHONY: test test-build test-run test-clean test-setup

# Run all tests
test: test-setup
	@echo "Running all tests..."
	docker compose -f docker-compose.test.yaml up --build --abort-on-container-exit --quiet-pull

# Build test containers
test-build:
	@echo "Building test containers..."
	docker compose -f docker-compose.test.yaml build

# Run tests without rebuilding
test-run:
	@echo "Running tests..."
	docker compose -f docker-compose.test.yaml up --abort-on-container-exit --quiet-pull

# Clean up test containers and images
test-clean:
	@echo "Cleaning up test containers..."
	docker compose -f docker-compose.test.yaml down --rmi all --volumes --remove-orphans

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

# Help
help:
	@echo "Available targets:"
	@echo "  test          - Run all tests"
	@echo "  test-build    - Build test containers"
	@echo "  test-run      - Run tests without rebuilding"
	@echo "  test-clean    - Clean up test containers"
	@echo "  test-setup    - Setup test environment"
	@echo "  test-middleware - Run middleware tests only"
	@echo "  test-protocol   - Run protocol tests only"
	@echo "  help          - Show this help"
