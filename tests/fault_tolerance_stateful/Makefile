.PHONY: help up up-detached down logs logs-worker-2 logs-worker-3 logs-test-runner logs-rabbitmq build clean kill-worker-2 restart-worker-2 ps shell-worker-2 check-metadata view-metadata test

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
	docker kill stateful-test-worker-2 || true

restart-worker-2: ## Restart worker-2
	docker start stateful-test-worker-2 || $(COMPOSE_CMD) up -d worker-2

ps: ## Show running containers
	$(COMPOSE_CMD) ps

shell-worker-2: ## Open shell in worker-2 container
	docker exec -it stateful-test-worker-2 /bin/sh

check-metadata: ## List metadata files in worker-2
	docker exec stateful-test-worker-2 ls -la /app/worker-data/metadata/ || echo "No metadata directory found"

view-metadata: ## View a specific client's metadata file (usage: make view-metadata CLIENT=CLI1)
	@if [ -z "$(CLIENT)" ]; then \
		echo "Usage: make view-metadata CLIENT=CLI1"; \
	else \
		docker exec stateful-test-worker-2 cat /app/worker-data/metadata/$(CLIENT).csv 2>/dev/null || echo "Metadata file not found"; \
	fi

test: ## Run a quick test (start, wait, check results)
	@echo "Starting services..."
	$(COMPOSE_CMD) up -d --build
	@echo "Waiting for services to be ready..."
	@sleep 5
	@echo "Services started. Check logs with: make logs"
	@echo "Check metadata with: make check-metadata"
	@echo "View metadata with: make view-metadata CLIENT=CLI1"

