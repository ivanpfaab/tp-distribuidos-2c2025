# Simple Makefile for Docker Compose Management

.PHONY: help docker-compose-up docker-compose-down docker-compose-logs docker-compose-build docker-compose-test

# Default target
help: ## Show this help message
	@echo "Available commands:"
	@echo "  docker-compose-up     - Start all services"
	@echo "  docker-compose-down   - Stop all services"
	@echo "  docker-compose-logs   - Show logs from all services"
	@echo "  docker-compose-build  - Build all Docker images"
	@echo "  docker-compose-test   - Run tests"
	@echo "  help                  - Show this help message"

# Start all services
docker-compose-up: ## Start all services
	docker compose up

# Stop all services
docker-compose-down: ## Stop all services
	docker compose down

# Show logs from all services or specific service
docker-compose-logs: ## Show logs from all services (usage: make docker-compose-logs SERVICE=query-orchestrator)
	@if [ -n "$(SERVICE)" ]; then \
		docker compose logs -f $(SERVICE); \
	else \
		docker compose logs -f; \
	fi

# Build all Docker images
docker-compose-build: ## Build all Docker images
	docker compose build

# Run tests
docker-compose-test: ## Run tests
	docker compose --profile test up --build