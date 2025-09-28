# Query Orchestrator Makefile

.PHONY: help build test clean up down logs

# Default target
help:
	@echo "Available targets:"
	@echo "  build     - Build the Query Orchestrator Docker image"
	@echo "  up        - Start all services (RabbitMQ + Query Orchestrator + Mock Consumers)"
	@echo "  down      - Stop all services"
	@echo "  logs      - Show logs from all services"
	@echo "  test      - Run a simple test"
	@echo "  clean     - Clean up Docker images and containers"
	@echo "  rabbitmq  - Start only RabbitMQ for testing"

# Build the Query Orchestrator
build:
	@echo "Building Query Orchestrator..."
	docker compose build query-orchestrator

# Start all services
up:
	@echo "Starting Query Orchestrator test environment..."
	docker compose up -d
	@echo "Services started! Check RabbitMQ Management UI at http://localhost:15672"
	@echo "Username: admin, Password: password"

# Stop all services
down:
	@echo "Stopping all services..."
	docker compose down

# Show logs
logs:
	docker compose logs -f

# Show logs for specific service
logs-orchestrator:
	docker compose logs -f query-orchestrator

logs-rabbitmq:
	docker compose logs -f rabbitmq

# Start only RabbitMQ for manual testing
rabbitmq:
	@echo "Starting only RabbitMQ..."
	docker compose up -d rabbitmq
	@echo "RabbitMQ started! Management UI at http://localhost:15672"

# Clean up
clean:
	@echo "Cleaning up..."
	docker compose down -v
	docker system prune -f

# Test the setup
test:
	@echo "Testing Query Orchestrator setup..."
	@echo "1. Starting services..."
	docker compose up -d
	@echo "2. Waiting for services to be ready..."
	sleep 15
	@echo "3. Checking if Query Orchestrator is running..."
	docker compose ps
	@echo "4. Check RabbitMQ Management UI at http://localhost:15672"
	@echo "   Look for exchanges: filter-exchange, aggregator-exchange, join-exchange, groupby-exchange"

# Quick test - build and run
quick-test: build up
	@echo "Quick test completed. Check logs with 'make logs'"
