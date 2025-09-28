# Go Echo Server and Client Makefile

# Variables
SERVER_IMAGE = echo-server
CLIENT_IMAGE = echo-client
SERVER_CONTAINER = echo-server
CLIENT_CONTAINER = echo-client
SERVER_PORT = 8080

# Default target
.PHONY: help
help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Docker Compose commands
.PHONY: docker-compose-up
docker-compose-up: ## Start both server and client with docker-compose
	docker compose up --build

.PHONY: up-detached
up-detached: ## Start both services in detached mode
	docker compose up --build -d

.PHONY: docker-compose-down
docker-compose-down: ## Stop and remove all containers
	docker compose down

.PHONY: docker-compose-logs
docker-compose-logs: ## Show logs from all services
	docker compose logs -f

.PHONY: logs-server
logs-server: ## Show server logs
	docker compose logs -f server

.PHONY: logs-client
logs-client: ## Show client logs
	docker compose logs -f client

.PHONY: restart
restart: ## Restart all services
	docker compose restart

.PHONY: rebuild
rebuild: ## Rebuild and start services
	docker compose up --build --force-recreate

# Individual Docker commands
.PHONY: build-server
build-server: ## Build server Docker image
	docker build -t $(SERVER_IMAGE) ./server

.PHONY: build-client
build-client: ## Build client Docker image
	docker build -t $(CLIENT_IMAGE) ./client

.PHONY: build-all
build-all: build-server build-client ## Build both Docker images

.PHONY: run-server
run-server: build-server ## Run server container
	docker run -d -p $(SERVER_PORT):$(SERVER_PORT) --name $(SERVER_CONTAINER) $(SERVER_IMAGE)

.PHONY: run-client
run-client: build-client ## Run client container with default input file
	docker run -it --rm --network host -v $(PWD)/input.txt:/app/input.txt $(CLIENT_IMAGE) /app/input.txt

.PHONY: run-client-file
run-client-file: build-client ## Run client with custom input file (usage: make run-client-file FILE=input.txt)
	docker run -it --rm --network host -v $(PWD)/$(FILE):/app/input.txt $(CLIENT_IMAGE) /app/input.txt

.PHONY: run-client-docker-network
run-client-docker-network: build-client ## Run client container with Docker network
	docker run -it --rm --network tp-distribuidos-2c2025_echo-network -v $(PWD)/input.txt:/app/input.txt $(CLIENT_IMAGE) /app/input.txt

.PHONY: stop-server
stop-server: ## Stop server container
	docker stop $(SERVER_CONTAINER) || true

.PHONY: stop-client
stop-client: ## Stop client container
	docker stop $(CLIENT_CONTAINER) || true

.PHONY: stop-all
stop-all: stop-server stop-client ## Stop all containers

.PHONY: remove-server
remove-server: stop-server ## Remove server container
	docker rm $(SERVER_CONTAINER) || true

.PHONY: remove-client
remove-client: stop-client ## Remove client container
	docker rm $(CLIENT_CONTAINER) || true

.PHONY: remove-all
remove-all: remove-server remove-client ## Remove all containers

.PHONY: clean
clean: remove-all ## Remove containers and images
	docker rmi $(SERVER_IMAGE) $(CLIENT_IMAGE) || true

.PHONY: clean-all
clean-all: clean ## Remove all containers, images, and volumes
	docker-compose down -v --rmi all

# Comprehensive cleanup commands
.PHONY: cleanup
cleanup: ## Clean up project-specific containers, images, and networks
	@echo "Cleaning up project-specific resources..."
	@echo "Stopping and removing containers..."
	@docker stop echo-server echo-client rabbitmq-server 2>/dev/null || true
	@docker rm echo-server echo-client rabbitmq-server 2>/dev/null || true
	@echo "Removing project images..."
	@docker rmi tp-distribuidos-2c2025_server tp-distribuidos-2c2025_client 2>/dev/null || true
	@echo "Removing project networks..."
	@docker network rm tp-distribuidos-2c2025_echo-network 2>/dev/null || true
	@echo "Project cleanup completed!"

.PHONY: cleanup-ports
cleanup-ports: ## Kill processes using project ports (8080, 8081, 5672, 15672)
	@echo "Cleaning up ports..."
	@echo "Checking port 8080..."
	@lsof -ti:8080 | xargs -r kill -9 2>/dev/null || echo "Port 8080 is free"
	@echo "Checking port 8081..."
	@lsof -ti:8081 | xargs -r kill -9 2>/dev/null || echo "Port 8081 is free"
	@echo "Checking port 5672 (RabbitMQ)..."
	@lsof -ti:5672 | xargs -r kill -9 2>/dev/null || echo "Port 5672 is free"
	@echo "Checking port 15672 (RabbitMQ Management)..."
	@lsof -ti:15672 | xargs -r kill -9 2>/dev/null || echo "Port 15672 is free"
	@echo "Port cleanup completed!"

.PHONY: deep-cleanup
deep-cleanup: ## Clean up project-specific containers, images, and networks
	@echo "Cleaning up project-specific resources..."
	@echo "Stopping and removing containers..."
	@docker stop echo-server echo-client rabbitmq-server test-echo-server test-runner 2>/dev/null || true
	@docker rm echo-server echo-client rabbitmq-server test-echo-server test-runner 2>/dev/null || true
	@echo "Removing project images..."
	@docker rmi tp-distribuidos-2c2025_server tp-distribuidos-2c2025_client test-echo-server test-runner 2>/dev/null || true
	@echo "Removing project networks..."
	@docker network rm tp-distribuidos-2c2025_echo-network 2>/dev/null || true
	@echo "Project cleanup completed!"

# Development commands
.PHONY: dev-server
dev-server: ## Run server locally for development
	cd server && go run main.go

.PHONY: dev-client
dev-client: ## Run client locally for development
	cd client && go run main.go

.PHONY: test-connection
test-connection: ## Test server connection with netcat
	@echo "Testing server connection on port $(SERVER_PORT)..."
	@echo "test message" | nc localhost $(SERVER_PORT) || echo "Server not running or connection failed"

# Status commands
.PHONY: status
status: ## Show status of containers
	@echo "=== Docker Containers ==="
	@docker ps -a --filter "name=$(SERVER_CONTAINER)" --filter "name=$(CLIENT_CONTAINER)"
	@echo ""
	@echo "=== Docker Images ==="
	@docker images | grep -E "$(SERVER_IMAGE)|$(CLIENT_IMAGE)"

.PHONY: ps
ps: ## Show running containers
	docker ps

# Quick start commands
.PHONY: start
start: up ## Alias for 'up' command

.PHONY: stop
stop: down ## Alias for 'down' command
