# Query Orchestrator Makefile

.PHONY: help build build-workers test clean up down logs

# Default target
help:
	@echo "Available targets:"
	@echo "  build         - Build the Query Orchestrator Docker image"
	@echo "  build-workers - Build all worker Docker images"
	@echo "  up            - Start all services (RabbitMQ + Query Orchestrator + Workers)"
	@echo "  down          - Stop all services"
	@echo "  logs          - Show logs from all services"
	@echo "  test          - Run a complete test with all services"
	@echo "  clean         - Clean up Docker images and containers"
	@echo "  rabbitmq      - Start only RabbitMQ for testing"

# Build the Query Orchestrator
build:
	@echo "Building Query Orchestrator..."
	docker compose build query-orchestrator

# Build all worker images
build-workers:
	@echo "Building all worker images..."
	docker compose build filter-worker aggregator-worker join-worker groupby-worker

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

logs-workers:
	docker compose logs -f filter-worker aggregator-worker join-worker groupby-worker

logs-filter:
	docker compose logs -f filter-worker

logs-aggregator:
	docker compose logs -f aggregator-worker

logs-join:
	docker compose logs -f join-worker

logs-groupby:
	docker compose logs -f groupby-worker

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
	@echo "Testing Query Orchestrator setup with workers..."
	@echo "1. Building all services..."
	docker compose build
	@echo "2. Starting all services..."
	docker compose up -d
	@echo "3. Waiting for services to be ready..."
	sleep 20
	@echo "4. Checking if all services are running..."
	docker compose ps
	@echo "5. Check RabbitMQ Management UI at http://localhost:15672"
	@echo "   Look for exchanges: filter-exchange, aggregator-exchange, join-exchange, groupby-exchange"
	@echo "6. Check worker logs with: make logs-workers"

# Quick test - build and run
quick-test: build up
	@echo "Quick test completed. Check logs with 'make logs'"

