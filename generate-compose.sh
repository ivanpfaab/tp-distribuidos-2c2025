#!/bin/bash

# Color codes for pretty output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Docker Compose Generator for Distributed System${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Function to prompt for worker count with validation
prompt_worker_count() {
    local worker_name=$1
    local default_count=$2
    local count
    
    while true; do
        echo -e "${BLUE}How many ${YELLOW}${worker_name}${BLUE} workers do you want? [default: ${default_count}]${NC}" >&2
        read -p "> " count
        
        # Use default if empty
        if [ -z "$count" ]; then
            count=$default_count
        fi
        
        # Validate input is a positive integer
        if [[ "$count" =~ ^[1-9][0-9]*$ ]]; then
            echo -e "${GREEN}✓ Setting ${worker_name}: ${count} replica(s)${NC}" >&2
            echo "" >&2
            echo "$count"
            return
        else
            echo -e "${YELLOW}⚠ Please enter a valid positive number${NC}" >&2
            echo "" >&2
        fi
    done
}

# Collect worker counts
echo -e "${BLUE}Configure Filter Workers (stateless, fully scalable)${NC}"
echo ""

YEAR_FILTER_COUNT=$(prompt_worker_count "year-filter" 1)
TIME_FILTER_COUNT=$(prompt_worker_count "time-filter" 1)
AMOUNT_FILTER_COUNT=$(prompt_worker_count "amount-filter" 1)

echo ""
echo -e "${BLUE}Configure Clients${NC}"
echo ""

CLIENT_COUNT=$(prompt_worker_count "client" 1)

# Summary
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Configuration Summary:${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "Year Filter Workers:   ${YELLOW}${YEAR_FILTER_COUNT}${NC}"
echo -e "Time Filter Workers:   ${YELLOW}${TIME_FILTER_COUNT}${NC}"
echo -e "Amount Filter Workers: ${YELLOW}${AMOUNT_FILTER_COUNT}${NC}"
echo -e "Clients:               ${YELLOW}${CLIENT_COUNT}${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Confirm before generating
echo -e "${BLUE}Generate docker-compose.yaml with this configuration? (y/n)${NC}"
read -p "> " confirm

if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Aborted. No files were generated.${NC}"
    exit 0
fi

echo ""
echo -e "${GREEN}Generating docker-compose.yaml...${NC}"

# Generate docker-compose.yaml
cat > docker-compose.yaml << 'EOF_HEADER'
services:
  # Infrastructure
  rabbitmq:
    image: rabbitmq:3-management
    container_name: rabbitmq-orchestrator
    ports:
      - "5672:5672"   # AMQP port
      - "15672:15672" # Management UI port
    environment:
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: password
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s

EOF_HEADER

# Function to generate year-filter workers
generate_year_filter_workers() {
    local count=$1
    for i in $(seq 1 $count); do
        cat >> docker-compose.yaml << EOF
  # Year Filter Worker ${i}
  year-filter-worker-${i}:
    build:
      context: .
      dockerfile: ./workers/filter/year-filter/Dockerfile
    container_name: year-filter-worker-${i}
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
    profiles: ["orchestration"]

EOF
    done
}

# Function to generate time-filter workers
generate_time_filter_workers() {
    local count=$1
    for i in $(seq 1 $count); do
        cat >> docker-compose.yaml << EOF
  # Time Filter Worker ${i}
  time-filter-worker-${i}:
    build:
      context: .
      dockerfile: ./workers/filter/time-filter/Dockerfile
    container_name: time-filter-worker-${i}
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
    profiles: ["orchestration"]

EOF
    done
}

# Function to generate amount-filter workers
generate_amount_filter_workers() {
    local count=$1
    for i in $(seq 1 $count); do
        cat >> docker-compose.yaml << EOF
  # Amount Filter Worker ${i}
  amount-filter-worker-${i}:
    build:
      context: .
      dockerfile: ./workers/filter/amount-filter/Dockerfile
    container_name: amount-filter-worker-${i}
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
    profiles: ["orchestration"]

EOF
    done
}

# Generate filter workers
echo -e "${BLUE}Generating filter workers...${NC}"
generate_year_filter_workers $YEAR_FILTER_COUNT
generate_time_filter_workers $TIME_FILTER_COUNT
generate_amount_filter_workers $AMOUNT_FILTER_COUNT

# Add remaining non-scalable services
cat >> docker-compose.yaml << 'EOF_REMAINING'
  # Join Data Handler (single instance for now)
  join-data-handler:
    build:
      context: .
      dockerfile: ./workers/join/data-handler/Dockerfile
    container_name: join-data-handler
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
    profiles: ["orchestration"]

  # ItemID Join Worker (single instance for now)
  itemid-join-worker:
    build:
      context: .
      dockerfile: ./workers/join/in-memory/itemid/Dockerfile
    container_name: itemid-join-worker
    depends_on:
      join-data-handler:
        condition: service_started
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
    profiles: ["orchestration"]

  # StoreID Join Worker (single instance for now)
  storeid-join-worker:
    build:
      context: .
      dockerfile: ./workers/join/in-memory/storeid/Dockerfile
    container_name: storeid-join-worker
    depends_on:
      join-data-handler:
        condition: service_started
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
    profiles: ["orchestration"]

  # In-File Join Worker (Query 4) (single instance for now)
  in-file-join-worker:
    build:
      context: .
      dockerfile: ./workers/join/in-file/Dockerfile
    container_name: in-file-join-worker
    depends_on:
      rabbitmq:
        condition: service_healthy
      join-data-handler:
        condition: service_started
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
    volumes:
      - shared-data:/shared-data
    profiles: ["orchestration"]

  # GroupBy Worker (NOT SCALABLE - has internal distributed architecture)
  groupby-worker:
    build:
      context: .
      dockerfile: ./workers/group_by/Dockerfile
    container_name: groupby-worker
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
    profiles: ["orchestration"]

  # Streaming service (NOT SCALABLE - outputs to stdout)
  streaming-service:
    build:
      context: .
      dockerfile: ./stream/Dockerfile
    container_name: streaming-service
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
    profiles: ["orchestration"]

  # Query gateway service (single instance for now)
  query-gateway:
    build:
      context: .
      dockerfile: ./query-gateway/Dockerfile
    container_name: query-gateway
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
    profiles: ["orchestration"]

EOF_REMAINING

# Generate server service with dependencies on all filter workers
generate_server_dependencies() {
    echo "  # Core application (data flow services)"
    echo "  server:"
    echo "    build:"
    echo "      context: ."
    echo "      dockerfile: ./server/Dockerfile"
    echo "    container_name: server"
    echo "    ports:"
    echo "      - \"8081:8080\"  # TCP port for client connections"
    echo "    depends_on:"
    echo "      rabbitmq:"
    echo "        condition: service_healthy"
    
    # Add dependencies for all year-filter workers
    for i in $(seq 1 $YEAR_FILTER_COUNT); do
        echo "      year-filter-worker-${i}:"
        echo "        condition: service_started"
    done
    
    # Add dependencies for all time-filter workers
    for i in $(seq 1 $TIME_FILTER_COUNT); do
        echo "      time-filter-worker-${i}:"
        echo "        condition: service_started"
    done
    
    # Add dependencies for all amount-filter workers
    for i in $(seq 1 $AMOUNT_FILTER_COUNT); do
        echo "      amount-filter-worker-${i}:"
        echo "        condition: service_started"
    done
    
    # Add remaining dependencies
    echo "      join-data-handler:"
    echo "        condition: service_started"
    echo "      itemid-join-worker:"
    echo "        condition: service_started"
    echo "      storeid-join-worker:"
    echo "        condition: service_started"
    echo "      in-file-join-worker:"
    echo "        condition: service_started"
    echo "      groupby-worker:"
    echo "        condition: service_started"
    echo "      streaming-service:"
    echo "        condition: service_started"
    echo "    environment:"
    echo "      - SERVER_PORT=8080"
    echo "      - RABBITMQ_HOST=rabbitmq"
    echo "      - RABBITMQ_PORT=5672"
    echo "      - RABBITMQ_USER=admin"
    echo "      - RABBITMQ_PASS=password"
    echo "    profiles: [\"data-flow\"]"
    echo ""
}

generate_server_dependencies >> docker-compose.yaml

# Function to generate client services
generate_clients() {
    local count=$1
    for i in $(seq 1 $count); do
        # Generate CLIENT_ID (CLI1, CLI2, CLI3, etc.)
        local client_id=$(printf "CLI%d" $i)
        
        # First client has simpler container name for backward compatibility
        local container_name
        if [ $i -eq 1 ]; then
            container_name="client"
        else
            container_name="client-${i}"
        fi
        
        cat >> docker-compose.yaml << EOF
  # Client ${i}
  client-${i}:
    build:
      context: .
      dockerfile: ./client/Dockerfile
    container_name: ${container_name}
    depends_on:
      server:
        condition: service_started
    volumes:
      - ./data:/app/data
    environment:
      - CLIENT_ID=${client_id}
    command: ["./main", "/app/data", "server:8080"]
    profiles: ["data-flow"]

EOF
    done
}

# Generate clients
echo -e "${BLUE}Generating clients...${NC}"
generate_clients $CLIENT_COUNT

# Add test runner and volumes
cat >> docker-compose.yaml << 'EOF_FOOTER'
  # Testing (optional, can be run with profile)
  test-runner:
    build:
      context: .
      dockerfile: ./tests/Dockerfile
    container_name: test-runner
    profiles: ["test"]
    depends_on:
      rabbitmq:
        condition: service_healthy
      server:
        condition: service_started
    environment:
      RABBITMQ_URL: "amqp://admin:password@rabbitmq:5672/"
      LOG_LEVEL: "info"
      GOMAXPROCS: "1"
    command: ["go", "test", "-v", "./..."]
    stdin_open: true
    tty: true

volumes:
  shared-data:
    driver: local
EOF_FOOTER

echo -e "${GREEN}✓ docker-compose.yaml generated successfully!${NC}"
echo ""
echo -e "${BLUE}Configuration Applied:${NC}"
echo -e "  Year Filter:   ${YELLOW}${YEAR_FILTER_COUNT}${NC} instance(s)"
echo -e "  Time Filter:   ${YELLOW}${TIME_FILTER_COUNT}${NC} instance(s)"
echo -e "  Amount Filter: ${YELLOW}${AMOUNT_FILTER_COUNT}${NC} instance(s)"
echo -e "  Clients:       ${YELLOW}${CLIENT_COUNT}${NC} instance(s)"
echo ""
echo -e "  Total Filter Workers: ${YELLOW}$((YEAR_FILTER_COUNT + TIME_FILTER_COUNT + AMOUNT_FILTER_COUNT))${NC} instances"
echo ""
echo -e "${GREEN}You can now run: ${YELLOW}make docker-compose-up${NC}"
echo ""

