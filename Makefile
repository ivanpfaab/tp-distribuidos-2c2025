# Simple Makefile for Docker Compose Management

.PHONY: help docker-compose-up docker-compose-up-quick docker-compose-down docker-compose-down-force docker-compose-logs docker-compose-logs-orchestration docker-compose-logs-data-flow docker-compose-build docker-compose-test docker-compose-rebuild docker-compose-generate docker-compose-restore

# Default target
help: ## Show this help message
	@echo "Available commands:"
	@echo "  docker-compose-up                - Start all services in orchestrated order"
	@echo "  docker-compose-up-quick          - Start all services quickly (alternative)"
	@echo "  docker-compose-down              - Stop all services"
	@echo "  docker-compose-down-force        - Force stop all services (handles restart policies)"
	@echo "  docker-compose-logs              - Show logs from all services"
	@echo "  docker-compose-logs-orchestration - Show logs for orchestration services only"
	@echo "  docker-compose-logs-data-flow    - Show logs for data-flow services only"
	@echo "  docker-compose-build             - Build all Docker images"
	@echo "  docker-compose-rebuild           - Rebuild everything from scratch (no cache)"
	@echo "  docker-compose-test              - Run tests"
	@echo "  docker-compose-generate          - Generate docker-compose.yaml (scale: filters, gateways, join workers, clients)"
	@echo "  docker-compose-restore           - Restore original docker-compose.yaml from backup"
	@echo "  help                             - Show this help message"

# Start all services in proper order
docker-compose-up: ## Start all services in proper order
	@echo "Starting services in orchestrated order..."
	@echo "1. Starting RabbitMQ..."
	docker compose up -d rabbitmq
	@echo "Waiting for RabbitMQ to be healthy..."
	@bash -c 'for i in {1..30}; do if docker compose ps rabbitmq | grep -q "healthy"; then break; fi; sleep 2; done'
	@echo "2. Starting all orchestration services (workers, gateways, etc.)..."
	docker compose --profile orchestration up -d
	@echo "3. Starting Server..."
	docker compose --profile orchestration --profile data-flow up -d server
	@echo "4. Starting Clients..."
	docker compose --profile orchestration --profile data-flow up -d
	@echo "All services started successfully!"

docker-compose-up-build: ## Start all services in proper order with build
	@echo "Starting services in orchestrated order..."
	@echo "1. Starting RabbitMQ..."
	docker compose up -d --build rabbitmq
	@echo "Waiting for RabbitMQ to be healthy..."
	@bash -c 'for i in {1..30}; do if docker compose ps rabbitmq | grep -q "healthy"; then break; fi; sleep 2; done'
	@echo "2. Starting all orchestration services (workers, gateways, etc.)..."
	docker compose --profile orchestration up -d --build
	@echo "3. Starting Server..."
	docker compose --profile orchestration --profile data-flow up -d --build server
	@echo "4. Starting Clients..."
	docker compose --profile orchestration --profile data-flow up -d --build
	@echo "All services started successfully!"

# Quick start (alternative - starts all at once with dependencies)
docker-compose-up-quick: ## Start all services quickly (alternative method)
	docker compose --profile orchestration --profile data-flow up

# Stop all services
docker-compose-down: ## Stop all services
	docker compose down

# Force stop all services (stops containers with restart policies)
docker-compose-down-force: ## Force stop all services
	@echo "Force stopping all containers..."
	docker stop $(docker ps -q) 2>/dev/null || true
	@echo "Removing containers and networks..."
	docker compose down --remove-orphans --volumes
	@echo "Cleanup complete!"

# Show logs from all services or specific service
docker-compose-logs: ## Show logs from all services (usage: make docker-compose-logs SERVICE=year-filter-worker)
	@if [ -n "$(SERVICE)" ]; then \
		docker compose --profile orchestration --profile data-flow logs -f $(SERVICE); \
	else \
		docker compose --profile orchestration --profile data-flow logs -f; \
	fi

# Show logs for orchestration services only
docker-compose-logs-orchestration: ## Show logs for orchestration services only
	docker compose --profile orchestration logs -f

# Show logs for data-flow services only  
docker-compose-logs-data-flow: ## Show logs for data-flow services only
	docker compose --profile data-flow logs -f

# Build all Docker images
docker-compose-build: ## Build all Docker images
	docker compose build

# Rebuild everything from scratch (no cache)
docker-compose-rebuild: ## Rebuild everything from scratch (no cache)
	@echo "Rebuilding everything from scratch..."
	@echo "1. Stopping all services..."
	docker compose down --remove-orphans --volumes
	@echo "2. Removing all containers and images..."
	docker system prune -a -f --volumes
	@echo "3. Building all images from scratch..."
	docker compose build --no-cache --pull
	@echo "4. Starting services in orchestrated order..."
	@echo "   Starting RabbitMQ..."
	docker compose up -d rabbitmq
	@echo "   Waiting for RabbitMQ to be healthy..."
	@bash -c 'for i in {1..30}; do if docker compose ps rabbitmq | grep -q "healthy"; then break; fi; sleep 2; done'
	@echo "   Starting Workers..."
	docker compose --profile orchestration up -d year-filter-worker-1 year-filter-worker-2 year-filter-worker-3 time-filter-worker-1 time-filter-worker-2 amount-filter-worker-1 join-data-handler-1 itemid-join-worker-1 itemid-join-worker-2 storeid-join-worker-1 storeid-join-worker-2 user-partition-splitter user-partition-writer-1 user-partition-writer-2 user-partition-writer-3 user-partition-writer-4 user-partition-writer-5 user-join-reader-1 user-join-reader-2 query2-map-worker query2-reduce-s2-2023 query2-reduce-s1-2024 query2-reduce-s2-2024 query2-reduce-s1-2025 query2-reduce-s2-2025 query2-top-items-worker query3-map-worker query3-reduce-s2-2023 query3-reduce-s1-2024 query3-reduce-s2-2024 query3-reduce-s1-2025 query3-reduce-s2-2025 query4-map-worker query4-reduce-worker query4-top-users-worker streaming-service query-gateway-1
	@echo "   Starting Server..."
	docker compose --profile orchestration --profile data-flow up -d server
	@echo "   Starting Clients..."
	docker compose --profile orchestration --profile data-flow up -d client-1
	@echo "Rebuild complete! All services started successfully!"

# Run tests
docker-compose-test: ## Run tests
	docker compose --profile test up --build

# Generate docker-compose.yaml with custom worker scaling
docker-compose-generate: ## Generate docker-compose.yaml with custom worker scaling
	@echo "Running interactive docker-compose generator..."
	./generate-compose.sh

# Restore original docker-compose.yaml
docker-compose-restore: ## Restore original docker-compose.yaml from backup
	@echo "Restoring docker-compose.yaml from backup..."
	@if [ -f docker-compose.yaml.backup ]; then \
		cp docker-compose.yaml.backup docker-compose.yaml; \
		echo "✓ docker-compose.yaml restored from backup"; \
	else \
		echo "✗ No backup file found (docker-compose.yaml.backup)"; \
		exit 1; \
	fi