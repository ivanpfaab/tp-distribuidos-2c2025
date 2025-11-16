# Fault Tolerance Test Makefile

.PHONY: help up down build logs kill-worker restart-worker clean test

COMPOSE_FILE := docker-compose.yaml

# Default target
help:
	@echo "Fault Tolerance Test System - Available commands:"
	@echo ""
	@echo "  make up              - Start all services (build + run)"
	@echo "  make start           - Start all services (build + run)"
	@echo "  make up-detached     - Start all services in detached mode"
	@echo "  make down            - Stop all services"
	@echo "  make stop            - Stop all services"
	@echo "  make build           - Build all containers"
	@echo "  make logs            - Show logs from all services"
	@echo "  make logs-worker-1   - Show logs from worker-1"
	@echo "  make logs-worker-2   - Show logs from worker-2"
	@echo "  make logs-worker-3   - Show logs from worker-3"
	@echo "  make logs-test       - Show logs from test-runner"
	@echo "  make kill-worker-1   - Kill worker-1 (fault injection)"
	@echo "  make kill-worker-2   - Kill worker-2 (fault injection)"
	@echo "  make kill-worker-3   - Kill worker-3 (fault injection)"
	@echo "  make restart-worker-1 - Restart worker-1"
	@echo "  make restart-worker-2 - Restart worker-2"
	@echo "  make restart-worker-3 - Restart worker-3"
	@echo "  make clean            - Stop and remove all containers and volumes"
	@echo "  make test             - Run the test system"
	@echo ""

# Start all services (build + run)
up: build
	@echo "Starting fault tolerance test system..."
	docker compose -f $(COMPOSE_FILE) up

start: up

# Start all services in detached mode
up-detached: build
	@echo "Starting fault tolerance test system in detached mode..."
	docker compose -f $(COMPOSE_FILE) up -d

# Stop all services
down:
	@echo "Stopping fault tolerance test system..."
	docker compose -f $(COMPOSE_FILE) down

stop: down

# Build all containers
build:
	@echo "Building fault tolerance test containers..."
	docker compose -f $(COMPOSE_FILE) build

# Show logs from all services
logs:
	docker compose -f $(COMPOSE_FILE) logs -f

# Show logs from specific services
logs-worker-1:
	docker compose -f $(COMPOSE_FILE) logs -f worker-1

logs-worker-2:
	docker compose -f $(COMPOSE_FILE) logs -f worker-2

logs-worker-3:
	docker compose -f $(COMPOSE_FILE) logs -f worker-3

logs-test:
	docker compose -f $(COMPOSE_FILE) logs -f test-runner

# Kill workers (fault injection)
kill-worker-1:
	@echo "Killing worker-1 (fault injection test)..."
	docker kill fault-tolerance-worker-1 || echo "Worker-1 not running"

kill-worker-2:
	@echo "Killing worker-2 (fault injection test)..."
	docker kill fault-tolerance-worker-2 || echo "Worker-2 not running"

kill-worker-3:
	@echo "Killing worker-3 (fault injection test)..."
	docker kill fault-tolerance-worker-3 || echo "Worker-3 not running"

# Restart workers
restart-worker-1:
	@echo "Restarting worker-1..."
	docker start fault-tolerance-worker-1 || echo "Worker-1 not found, starting new container..."
	docker compose -f $(COMPOSE_FILE) up -d worker-1

restart-worker-2:
	@echo "Restarting worker-2..."
	docker start fault-tolerance-worker-2 || echo "Worker-2 not found, starting new container..."
	docker compose -f $(COMPOSE_FILE) up -d worker-2

restart-worker-3:
	@echo "Restarting worker-3..."
	docker start fault-tolerance-worker-3 || echo "Worker-3 not found, starting new container..."
	docker compose -f $(COMPOSE_FILE) up -d worker-3

# Clean up (stop and remove containers, volumes)
clean:
	@echo "Cleaning up fault tolerance test system..."
	docker compose -f $(COMPOSE_FILE) down -v
	@echo "Cleanup complete!"

# Run the test system (alias for up)
test: up

