.PHONY: help up up-detached down logs logs-worker-1 logs-worker-2 logs-worker-3 logs-test-runner logs-rabbitmq build clean kill-worker-2 restart-worker-2 ps shell-worker-2 shell-worker-3 check-partitions check-state view-partition test

COMPOSE_FILE := docker-compose.yaml
COMPOSE_CMD := docker compose -f $(COMPOSE_FILE)

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}'

up: ## Start all services
	$(COMPOSE_CMD) up --build

up-detached: ## Start all services in detached mode
	$(COMPOSE_CMD) up --build -d

down: ## Stop all services
	$(COMPOSE_CMD) down

logs: ## View logs from all services
	$(COMPOSE_CMD) logs -f

logs-worker-1: ## View logs from worker-1
	$(COMPOSE_CMD) logs -f worker-1

logs-worker-2: ## View logs from worker-2
	$(COMPOSE_CMD) logs -f worker-2

logs-worker-3: ## View logs from worker-3
	$(COMPOSE_CMD) logs -f worker-3

logs-test-runner: ## View logs from test-runner
	$(COMPOSE_CMD) logs -f test-runner

logs-rabbitmq: ## View logs from RabbitMQ
	$(COMPOSE_CMD) logs -f rabbitmq

build: ## Build all services without starting
	$(COMPOSE_CMD) build

clean: ## Stop services and remove volumes
	$(COMPOSE_CMD) down -v

kill-worker-2: ## Kill worker-2 (for testing fault tolerance)
	docker kill partition-writes-worker-2 || true

restart-worker-2: ## Restart worker-2
	docker start partition-writes-worker-2 || $(COMPOSE_CMD) up -d worker-2

ps: ## Show running containers
	$(COMPOSE_CMD) ps

shell-worker-2: ## Open shell in worker-2 container
	docker exec -it partition-writes-worker-2 /bin/sh

shell-worker-3: ## Open shell in worker-3 container
	docker exec -it partition-writes-worker-3 /bin/sh

check-partitions: ## List partition files in worker-2
	docker exec partition-writes-worker-2 ls -la /app/worker-data/partitions/ || echo "No partitions directory found"

check-state: ## Show state files in worker-2
	@echo "=== Processed Chunks ==="
	docker exec partition-writes-worker-2 cat /app/worker-data/state/processed-chunks.txt 2>/dev/null || echo "File not found or empty"
	@echo ""
	@echo "=== Written Partition-Chunk Pairs ==="
	docker exec partition-writes-worker-2 cat /app/worker-data/state/written-partition-chunks.txt 2>/dev/null || echo "File not found or empty"

view-partition: ## View a specific partition file (usage: make view-partition PARTITION=0)
	@if [ -z "$(PARTITION)" ]; then \
		echo "Usage: make view-partition PARTITION=0"; \
	else \
		docker exec partition-writes-worker-2 cat /app/worker-data/partitions/TEST-users-partition-$(shell printf "%03d" $(PARTITION)).csv 2>/dev/null || echo "Partition file not found"; \
	fi

test: ## Run a quick test (start, wait, check results)
	@echo "Starting services..."
	$(COMPOSE_CMD) up -d --build
	@echo "Waiting for services to be ready..."
	@sleep 5
	@echo "Services started. Check logs with: make logs"
	@echo "Check results with: make check-partitions make check-state"

