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

# Function to prompt for chunk size (required, no default)
prompt_chunk_size() {
    local chunk_size
    
    while true; do
        echo -e "${BLUE}How many ${YELLOW}rows per chunk${BLUE} do you want? (required)${NC}" >&2
        read -p "> " chunk_size
        
        # Validate input is provided and is a positive integer
        if [ -z "$chunk_size" ]; then
            echo -e "${YELLOW}⚠ Chunk size is required. Please enter a positive number${NC}" >&2
            echo "" >&2
            continue
        fi
        
        if [[ "$chunk_size" =~ ^[1-9][0-9]*$ ]]; then
            echo -e "${GREEN}✓ Setting chunk size: ${chunk_size} rows${NC}" >&2
            echo "" >&2
            echo "$chunk_size"
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

YEAR_FILTER_COUNT=$(prompt_worker_count "year-filter" 3)
TIME_FILTER_COUNT=$(prompt_worker_count "time-filter" 2)
AMOUNT_FILTER_COUNT=$(prompt_worker_count "amount-filter" 1)

echo ""
echo -e "${BLUE}Configure Gateway Services (stateless routing)${NC}"
echo ""

QUERY_GATEWAY_COUNT=$(prompt_worker_count "query-gateway" 1)
JOIN_DATA_HANDLER_COUNT=$(prompt_worker_count "join-data-handler" 1)

echo ""
echo -e "${BLUE}Configure Join Workers (in-memory dictionary with broadcasting)${NC}"
echo ""

ITEMID_JOIN_WORKER_COUNT=$(prompt_worker_count "itemid-join-worker" 1)
STOREID_JOIN_WORKER_COUNT=$(prompt_worker_count "storeid-join-worker" 1)

echo ""
echo -e "${BLUE}Configure User Join Workers (Query 4 - distributed write/read)${NC}"
echo ""

USER_PARTITION_WORKERS=$(prompt_worker_count "user-partition-worker (writer+reader pairs)" 5)
USER_PARTITION_WRITERS=$USER_PARTITION_WORKERS
USER_JOIN_READERS=$USER_PARTITION_WORKERS
echo ""
echo -e "${BLUE}Configure Group By Components${NC}"
echo ""

Q2_PARTITIONER_COUNT=$(prompt_worker_count "query2-partitioner" 1)
Q2_GROUPBY_WORKER_COUNT=$(prompt_worker_count "query2-groupby-worker" 3)
Q2_NUM_PARTITIONS=$(prompt_worker_count "query2-partitions" 10) #correct to ask for number of partitions not 'query2-partitions workers'
Q3_PARTITIONER_COUNT=$(prompt_worker_count "query3-partitioner" 1)
Q3_GROUPBY_WORKER_COUNT=$(prompt_worker_count "query3-groupby-worker" 3)
Q3_NUM_PARTITIONS=$(prompt_worker_count "query3-partitions" 10)
Q4_PARTITIONER_COUNT=$(prompt_worker_count "query4-partitioner" 1)
Q4_GROUPBY_WORKER_COUNT=$(prompt_worker_count "query4-groupby-worker" 3)
Q4_NUM_PARTITIONS=$(prompt_worker_count "query4-partitions" 100)

echo ""
echo -e "${BLUE}Configure Clients${NC}"
echo ""

CLIENT_COUNT=$(prompt_worker_count "client" 1)

echo ""
echo -e "${BLUE}Configure Client Settings${NC}"
echo ""

CHUNK_SIZE=$(prompt_chunk_size)

echo ""
echo -e "${BLUE}Configure Fault Tolerance${NC}"
echo ""

SUPERVISOR_COUNT=$(prompt_worker_count "supervisor" 3)

# Summary
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Configuration Summary:${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "Year Filter Workers:    ${YELLOW}${YEAR_FILTER_COUNT}${NC}"
echo -e "Time Filter Workers:    ${YELLOW}${TIME_FILTER_COUNT}${NC}"
echo -e "Amount Filter Workers:  ${YELLOW}${AMOUNT_FILTER_COUNT}${NC}"
echo -e "Query Gateway:          ${YELLOW}${QUERY_GATEWAY_COUNT}${NC}"
echo -e "Join Data Handler:      ${YELLOW}${JOIN_DATA_HANDLER_COUNT}${NC}"
echo -e "ItemID Join Workers:    ${YELLOW}${ITEMID_JOIN_WORKER_COUNT}${NC}"
echo -e "StoreID Join Workers:   ${YELLOW}${STOREID_JOIN_WORKER_COUNT}${NC}"
echo -e "User Partition Writers: ${YELLOW}${USER_PARTITION_WRITERS}${NC}"
echo -e "User Join Readers:      ${YELLOW}${USER_JOIN_READERS}${NC}"
echo -e "Clients:                ${YELLOW}${CLIENT_COUNT}${NC}"
echo -e "Chunk Size:             ${YELLOW}${CHUNK_SIZE}${NC} rows per chunk"
echo -e "Supervisors:            ${YELLOW}${SUPERVISOR_COUNT}${NC}"
echo ""
echo -e "Query 2 Partitioners:   ${YELLOW}${Q2_PARTITIONER_COUNT}${NC} instance(s)"
echo -e "Query 2 GroupBy Workers: ${YELLOW}${Q2_GROUPBY_WORKER_COUNT}${NC} instance(s) (${Q2_NUM_PARTITIONS} partitions)"
echo -e "Query 3 Partitioners:   ${YELLOW}${Q3_PARTITIONER_COUNT}${NC} instance(s)"
echo -e "Query 3 GroupBy Workers: ${YELLOW}${Q3_GROUPBY_WORKER_COUNT}${NC} instance(s) (${Q3_NUM_PARTITIONS} partitions)"
echo -e "Query 4 Partitioners:   ${YELLOW}${Q4_PARTITIONER_COUNT}${NC} instance(s)"
echo -e "Query 4 GroupBy Workers: ${YELLOW}${Q4_GROUPBY_WORKER_COUNT}${NC} instance(s) (${Q4_NUM_PARTITIONS} partitions)"

echo -e "${BLUE}Note: Query 2 Top Items Worker and Query 4 Top Users Worker will be included${NC}"
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

# Function to generate supervisor services
generate_supervisors() {
    local count=$1

    for i in $(seq 1 $count); do
        # Build SUPERVISOR_PEERS list (all supervisors except current one)
        local peers=""
        for j in $(seq 1 $count); do
            if [ $j -ne $i ]; then
                if [ -n "$peers" ]; then
                    peers="${peers},"
                fi
                peers="${peers}supervisor-${j}:supervisor-${j}:9000"
            fi
        done

        cat >> docker-compose.yaml << EOF
  # Fault Tolerance - Supervisor ${i}
  supervisor-${i}:
    build:
      context: .
      dockerfile: ./supervisor/Dockerfile
    container_name: supervisor-${i}
    depends_on:
      rabbitmq:
        condition: service_healthy
      proxy-1:
        condition: service_started
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      SUPERVISOR_ID: "${i}"
      SUPERVISOR_PEERS: "${peers}"
      ELECTION_PORT: "9000"
      PROBE_TIMEOUT: "2s"
      PROBE_INTERVAL: "5s"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
    profiles: ["orchestration", "data-flow"]

EOF
    done
}

# Generate supervisors
echo -e "${BLUE}Generating supervisors...${NC}"
generate_supervisors $SUPERVISOR_COUNT

# Add Chaos Monkey
cat >> docker-compose.yaml << 'EOF_CHAOS'
  # Chaos Monkey (optional - use with 'chaos' profile)
  chaos-monkey:
    build:
      context: .
      dockerfile: ./chaos-monkey/Dockerfile
    container_name: chaos-monkey
    depends_on:
      proxy-1:
        condition: service_started
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      KILL_INTERVAL: "10s"
      KILL_PROBABILITY: "1"
      KILL_STRATEGY: "exponential"  # Options: "periodic" or "exponential"
      PAUSE_DURATION: "10s"
    profiles: ["chaos"]

EOF_CHAOS

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
      WORKER_ID: "year-filter-worker-${i}"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      HEALTH_PORT: "8888"
    volumes:
      - year-filter-worker-${i}-data:/app/worker-data
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
      WORKER_ID: "time-filter-worker-${i}"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      HEALTH_PORT: "8888"
    volumes:
      - time-filter-worker-${i}-data:/app/worker-data
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
      WORKER_ID: "amount-filter-worker-${i}"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      HEALTH_PORT: "8888"
    volumes:
      - amount-filter-worker-${i}-data:/app/worker-data
    profiles: ["orchestration"]

EOF
    done
}

# Generate filter workers
echo -e "${BLUE}Generating filter workers...${NC}"
generate_year_filter_workers $YEAR_FILTER_COUNT
generate_time_filter_workers $TIME_FILTER_COUNT
generate_amount_filter_workers $AMOUNT_FILTER_COUNT

# Function to generate itemid-join-worker services
generate_itemid_join_workers() {
    local count=$1
    for i in $(seq 1 $count); do
        cat >> docker-compose.yaml << EOF
  # ItemID Join Worker ${i} (scalable with dictionary broadcasting)
  itemid-join-worker-${i}:
    build:
      context: .
      dockerfile: ./workers/join/in-memory/itemid/Dockerfile
    container_name: itemid-join-worker-${i}
    depends_on:
      rabbitmq:
        condition: service_healthy
      join-data-handler-1:
        condition: service_started
    environment:
      WORKER_ID: "itemid-join-worker-${i}"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      WORKER_INSTANCE_ID: "${i}"
      HEALTH_PORT: "8888"
    volumes:
      - itemid-join-worker-${i}-data:/app/worker-data
    profiles: ["orchestration"]

EOF
    done
}

# Generate itemid-join-workers
echo -e "${BLUE}Generating itemid-join-workers...${NC}"
generate_itemid_join_workers $ITEMID_JOIN_WORKER_COUNT

# Function to generate storeid-join-worker services
generate_storeid_join_workers() {
    local count=$1
    local num_groupby_workers=$2  # Number of Query 3 groupby workers
    local num_partitions=$3       # Number of Query 3 partitions
    
    for i in $(seq 1 $count); do
        # Build dependencies on all Query 3 orchestrators
        local orchestrator_deps=""
        for w in $(seq 1 $num_groupby_workers); do
            orchestrator_deps="${orchestrator_deps}      query3-orchestrator-${w}:
        condition: service_started
"
        done
        
        cat >> docker-compose.yaml << EOF
  # StoreID Join Worker ${i} (scalable with dictionary broadcasting)
  storeid-join-worker-${i}:
    build:
      context: .
      dockerfile: ./workers/join/in-memory/storeid/Dockerfile
    container_name: storeid-join-worker-${i}
    depends_on:
      rabbitmq:
        condition: service_healthy
      join-data-handler-1:
        condition: service_started
$(echo -e "${orchestrator_deps}")
    environment:
      WORKER_ID: "storeid-join-worker-${i}"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      WORKER_INSTANCE_ID: "${i}"
      STOREID_BATCH_SIZE: 5
      NUM_PARTITIONS: "${num_partitions}"
      HEALTH_PORT: "8888"
    volumes:
      - storeid-join-worker-${i}-data:/app/worker-data
    profiles: ["orchestration"]

EOF
    done
}

# Generate storeid-join-workers
echo -e "${BLUE}Generating storeid-join-workers...${NC}"
generate_storeid_join_workers $STOREID_JOIN_WORKER_COUNT $Q3_GROUPBY_WORKER_COUNT $Q3_NUM_PARTITIONS

# Add remaining non-scalable services
cat >> docker-compose.yaml << 'EOF_REMAINING'
  # Results dispatcher (NOT SCALABLE - outputs to stdout)
  results-dispatcher-1:
    build:
      context: .
      dockerfile: ./dispatcher/Dockerfile
    container_name: results-dispatcher-1
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      WORKER_ID: "results-dispatcher-1"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      HEALTH_PORT: "8888"
    volumes:
      - results-dispatcher-1-data:/app/worker-data
    profiles: ["orchestration"]

  # In-Memory Join Orchestrator
  in-memory-join-orchestrator-1:
    build:
      context: .
      dockerfile: ./workers/join/in-memory/orchestrator/Dockerfile
    container_name: in-memory-join-orchestrator-1
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      WORKER_ID: "in-memory-join-orchestrator-1"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      HEALTH_PORT: "8888"
    volumes:
      - in-memory-join-orchestrator-data:/app/orchestrator-data
    profiles: ["orchestration"]

  in-file-join-orchestrator-1:
    build:
      context: .
      dockerfile: ./workers/join/in-file/orchestrator/Dockerfile
    container_name: in-file-join-orchestrator-1
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      WORKER_ID: "in-file-join-orchestrator-1"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      HEALTH_PORT: "8888"
    volumes:
      - in-file-join-orchestrator-data:/app/orchestrator-data
    profiles: ["orchestration"]

EOF_REMAINING


# Function to generate partitioner services
generate_partitioner() {
    local query_type=$1
    local count=$2
    local num_workers=$3
    local num_partitions=$4

    for i in $(seq 1 $count); do
        local container_name
        if [ $count -eq 1 ]; then
            container_name="query${query_type}-partitioner"
        else
            container_name="query${query_type}-partitioner-${i}"
        fi

        cat >> docker-compose.yaml << EOF
  # Query ${query_type} Partitioner ${i}
  query${query_type}-partitioner$([ $count -gt 1 ] && echo "-${i}" || echo ""):
    build:
      context: .
      dockerfile: ./workers/group_by/shared/partitioner/Dockerfile
    container_name: ${container_name}
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      WORKER_ID: "query${query_type}-partitioner$([ $count -gt 1 ] && echo "-${i}" || echo "")"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      QUERY_TYPE: "${query_type}"
      NUM_PARTITIONS: "${num_partitions}"
      NUM_WORKERS: "${num_workers}"
      HEALTH_PORT: "8888"
    volumes:
      - query${query_type}-partitioner$([ $count -gt 1 ] && echo "-${i}" || echo "")-data:/app/worker-data
    profiles: ["orchestration"]

EOF
    done
}

# Function to generate orchestrator services (one per worker)
generate_orchestrators() {
    local query_type=$1
    local worker_count=$2

    for i in $(seq 1 $worker_count); do
        cat >> docker-compose.yaml << EOF
  # Query ${query_type} Group By Orchestrator ${i} (for worker ${i})
  query${query_type}-orchestrator-${i}:
    build:
      context: .
      dockerfile: ./workers/group_by/shared/orchestrator/Dockerfile
    container_name: query${query_type}-orchestrator-${i}
    depends_on:
      rabbitmq:
        condition: service_healthy
      query${query_type}-groupby-worker-${i}:
        condition: service_started
    environment:
      WORKER_ID: "query${query_type}-orchestrator-${i}"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      QUERY_TYPE: "${query_type}"
      WORKER_NUMERIC_ID: "${i}"
      HEALTH_PORT: "8888"
    volumes:
      - query${query_type}-groupby-worker-${i}-data:/app/groupby-data
    profiles: ["orchestration"]

EOF
    done
}

# Function to generate groupby worker services
generate_groupby_workers() {
    local query_type=$1
    local count=$2
    local partitioner_count=$3
    local num_partitions=$4

    for i in $(seq 1 $count); do
        # Build dependencies on all partitioners
        local partitioner_deps=""
        for p in $(seq 1 $partitioner_count); do
            if [ $partitioner_count -eq 1 ]; then
                partitioner_deps="${partitioner_deps}      query${query_type}-partitioner:
        condition: service_started
"
            else
                partitioner_deps="${partitioner_deps}      query${query_type}-partitioner-${p}:
        condition: service_started
"
            fi
        done

        cat >> docker-compose.yaml << EOF
  # Query ${query_type} GroupBy Worker ${i}
  query${query_type}-groupby-worker-${i}:
    build:
      context: .
      dockerfile: ./workers/group_by/shared/worker/Dockerfile
    container_name: query${query_type}-groupby-worker-${i}
    depends_on:
      rabbitmq:
        condition: service_healthy
$(echo -e "${partitioner_deps}")
    environment:
      WORKER_ID: "query${query_type}-groupby-worker-${i}"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      QUERY_TYPE: "${query_type}"
      WORKER_NUMERIC_ID: "${i}"
      NUM_WORKERS: "${count}"
      NUM_PARTITIONS: "${num_partitions}"
      HEALTH_PORT: "8888"
    volumes:
      - query${query_type}-groupby-worker-${i}-data:/app/groupby-data
    profiles: ["orchestration"]

EOF
    done
}

# Function to generate top classification workers
generate_top_worker() {
    local query_type=$1
    local worker_name=$2
    local groupby_worker_count=$3
    local num_partitions=$4

    # Build dependencies on all groupby workers and orchestrators
    local deps=""
    for w in $(seq 1 $groupby_worker_count); do
        deps="${deps}      query${query_type}-groupby-worker-${w}:
        condition: service_started
"
        deps="${deps}      query${query_type}-orchestrator-${w}:
        condition: service_started
"
    done

    cat >> docker-compose.yaml << EOF
  # Query ${query_type} Top ${worker_name} Classification
  query${query_type}-top-${worker_name}-worker-1:
    build:
      context: .
      dockerfile: ./workers/top/query${query_type}_top_classification/Dockerfile
    container_name: query${query_type}-top-${worker_name}-worker-1
    depends_on:
      rabbitmq:
        condition: service_healthy
$(echo -e "${deps}")
    environment:
      WORKER_ID: "query${query_type}-top-${worker_name}-worker-1"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      NUM_PARTITIONS: "${num_partitions}"
      HEALTH_PORT: "8888"
    volumes:
      - query${query_type}-top-${worker_name}-worker-data:/app/worker-data
    profiles: ["orchestration"]

EOF
}

# Function to generate user-partition-splitter
generate_user_partition_splitter() {
    local num_writers=$1
    cat >> docker-compose.yaml << EOF
  # User Partition Splitter (distributes users to writers)
  user-partition-splitter-1:
    build:
      context: .
      dockerfile: ./workers/join/in-file/user-id/user-partition-splitter/Dockerfile
    container_name: user-partition-splitter-1
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      WORKER_ID: "user-partition-splitter-1"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      NUM_WRITERS: ${num_writers}
      HEALTH_PORT: "8888"
    volumes:
      - user-partition-splitter-1-data:/app/worker-data
    profiles: ["orchestration"]

EOF
}

# Function to generate user-partition-writer services
generate_user_partition_writers() {
    local count=$1
    for i in $(seq 1 $count); do
        cat >> docker-compose.yaml << EOF
  # User Partition Writer ${i}
  user-partition-writer-${i}:
    build:
      context: .
      dockerfile: ./workers/join/in-file/user-id/user-partition-writer/Dockerfile
    container_name: user-partition-writer-${i}
    depends_on:
      rabbitmq:
        condition: service_healthy
      user-partition-splitter-1:
        condition: service_started
    environment:
      WORKER_ID: "user-partition-writer-${i}"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      WRITER_ID: ${i}
      NUM_WRITERS: ${count}
      HEALTH_PORT: "8888"
    volumes:
      - user-writer-${i}-data:/shared-data
    profiles: ["orchestration"]

EOF
    done
}

# Function to generate user-join-reader services (paired with writers, same count)
generate_user_join_readers() {
    local count=$1

    for i in $(seq 1 $count); do
        cat >> docker-compose.yaml << EOF
  # User Join Reader ${i} (Query 4 - paired with Writer ${i}, reads from local partition files)
  user-join-reader-${i}:
    build:
      context: .
      dockerfile: ./workers/join/in-file/user-id/user-join/Dockerfile
    container_name: user-join-reader-${i}
    depends_on:
      rabbitmq:
        condition: service_healthy
      user-partition-writer-${i}:
        condition: service_started
      in-file-join-orchestrator-1:
        condition: service_started
      query4-top-users-worker-1:
        condition: service_started
    environment:
      WORKER_ID: "user-join-reader-${i}"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      READER_ID: ${i}
      NUM_WRITERS: ${count}
      HEALTH_PORT: "8888"
    volumes:
      - user-writer-${i}-data:/shared-data
      - user-join-reader-${i}-data:/app/worker-data
    profiles: ["orchestration"]

EOF
    done
}

# Function to generate join-data-handler services
generate_join_data_handler() {
    local count=$1
    local itemid_worker_count=$2
    local storeid_worker_count=$3
    for i in $(seq 1 $count); do
        cat >> docker-compose.yaml << EOF
  # Join Data Handler ${i}
  join-data-handler-${i}:
    build:
      context: .
      dockerfile: ./workers/join/data-handler/Dockerfile
    container_name: join-data-handler-${i}
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      WORKER_ID: "join-data-handler-${i}"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      ITEMID_WORKER_COUNT: "${itemid_worker_count}"
      STOREID_WORKER_COUNT: "${storeid_worker_count}"
      HEALTH_PORT: "8888"
    volumes:
      - join-data-handler-${i}-data:/app/worker-data
    profiles: ["orchestration"]

EOF
    done
}

# Function to generate query-gateway services
generate_query_gateway() {
    local count=$1
    for i in $(seq 1 $count); do
        cat >> docker-compose.yaml << EOF
  # Query Gateway ${i}
  query-gateway-${i}:
    build:
      context: .
      dockerfile: ./query-gateway/Dockerfile
    container_name: query-gateway-${i}
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      WORKER_ID: "query-gateway-${i}"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      HEALTH_PORT: "8888"
    volumes:
      - query-gateway-${i}-data:/app/worker-data
    profiles: ["orchestration"]

EOF
    done
}

# Generate join data handlers
echo -e "${BLUE}Generating join data handlers...${NC}"
generate_join_data_handler $JOIN_DATA_HANDLER_COUNT $ITEMID_JOIN_WORKER_COUNT $STOREID_JOIN_WORKER_COUNT

# Generate user partition components
echo -e "${BLUE}Generating user partition splitter...${NC}"
generate_user_partition_splitter $USER_PARTITION_WRITERS

echo -e "${BLUE}Generating user partition writers...${NC}"
generate_user_partition_writers $USER_PARTITION_WRITERS

echo -e "${BLUE}Generating user join readers (paired 1:1 with writers)...${NC}"
generate_user_join_readers $USER_JOIN_READERS

# Generate query gateways
echo -e "${BLUE}Generating query gateways...${NC}"
generate_query_gateway $QUERY_GATEWAY_COUNT


# Generate Query 2 components
echo -e "${BLUE}Generating Query 2 partitioners, groupby workers, and orchestrators...${NC}"
generate_partitioner 2 $Q2_PARTITIONER_COUNT $Q2_GROUPBY_WORKER_COUNT $Q2_NUM_PARTITIONS
generate_groupby_workers 2 $Q2_GROUPBY_WORKER_COUNT $Q2_PARTITIONER_COUNT $Q2_NUM_PARTITIONS
generate_orchestrators 2 $Q2_GROUPBY_WORKER_COUNT
generate_top_worker 2 "items" $Q2_GROUPBY_WORKER_COUNT $Q2_NUM_PARTITIONS

# Generate Query 3 components
echo -e "${BLUE}Generating Query 3 partitioners, groupby workers, and orchestrators...${NC}"
generate_partitioner 3 $Q3_PARTITIONER_COUNT $Q3_GROUPBY_WORKER_COUNT $Q3_NUM_PARTITIONS
generate_groupby_workers 3 $Q3_GROUPBY_WORKER_COUNT $Q3_PARTITIONER_COUNT $Q3_NUM_PARTITIONS
generate_orchestrators 3 $Q3_GROUPBY_WORKER_COUNT

# Generate Query 4 components
echo -e "${BLUE}Generating Query 4 partitioners, groupby workers, and orchestrators...${NC}"
generate_partitioner 4 $Q4_PARTITIONER_COUNT $Q4_GROUPBY_WORKER_COUNT $Q4_NUM_PARTITIONS
generate_groupby_workers 4 $Q4_GROUPBY_WORKER_COUNT $Q4_PARTITIONER_COUNT $Q4_NUM_PARTITIONS
generate_orchestrators 4 $Q4_GROUPBY_WORKER_COUNT
generate_top_worker 4 "users" $Q4_GROUPBY_WORKER_COUNT $Q4_NUM_PARTITIONS


# Generate server service with dependencies on all filter workers
generate_server_dependencies() {
    echo "  # Core application (data flow services)"
    echo "  proxy-1:"
    echo "    build:"
    echo "      context: ."
    echo "      dockerfile: ./proxy/Dockerfile"
    echo "    container_name: proxy-1"
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
    
    # Add dependencies for all query-gateway instances
    for i in $(seq 1 $QUERY_GATEWAY_COUNT); do
        echo "      query-gateway-${i}:"
        echo "        condition: service_started"
    done
    
    # Add dependencies for all join-data-handler instances
    for i in $(seq 1 $JOIN_DATA_HANDLER_COUNT); do
        echo "      join-data-handler-${i}:"
        echo "        condition: service_started"
    done
    
    # Add dependencies for user partition components
    echo "      user-partition-splitter-1:"
    echo "        condition: service_started"
    
    for i in $(seq 1 $USER_PARTITION_WRITERS); do
        echo "      user-partition-writer-${i}:"
        echo "        condition: service_started"
    done
    
    for i in $(seq 1 $USER_JOIN_READERS); do
        echo "      user-join-reader-${i}:"
        echo "        condition: service_started"
    done
    
    # Add dependencies for all itemid-join-worker instances
    for i in $(seq 1 $ITEMID_JOIN_WORKER_COUNT); do
        echo "      itemid-join-worker-${i}:"
        echo "        condition: service_started"
    done
    
    # Add dependencies for all storeid-join-worker instances
    for i in $(seq 1 $STOREID_JOIN_WORKER_COUNT); do
        echo "      storeid-join-worker-${i}:"
        echo "        condition: service_started"
    done
    # Add dependencies for all GroupBy services
    # Query 2 orchestrators
    for i in $(seq 1 $Q2_GROUPBY_WORKER_COUNT); do
        echo "      query2-orchestrator-${i}:"
        echo "        condition: service_started"
    done

    # Query 2 partitioners
    for i in $(seq 1 $Q2_PARTITIONER_COUNT); do
        echo "      query2-partitioner-${i}:"
        echo "        condition: service_started"
    done

    # Query 2 groupby workers
    for i in $(seq 1 $Q2_GROUPBY_WORKER_COUNT); do
        echo "      query2-groupby-worker-${i}:"
        echo "        condition: service_started"
    done

    echo "      query2-top-items-worker-1:"
    echo "        condition: service_started"

    # Query 3 orchestrators
    for i in $(seq 1 $Q3_GROUPBY_WORKER_COUNT); do
        echo "      query3-orchestrator-${i}:"
        echo "        condition: service_started"
    done

    # Query 3 partitioners
    for i in $(seq 1 $Q3_PARTITIONER_COUNT); do
        echo "      query3-partitioner-${i}:"
        echo "        condition: service_started"
    done

    # Query 3 groupby workers
    for i in $(seq 1 $Q3_GROUPBY_WORKER_COUNT); do
        echo "      query3-groupby-worker-${i}:"
        echo "        condition: service_started"
    done

    # Query 4 orchestrators
    for i in $(seq 1 $Q4_GROUPBY_WORKER_COUNT); do
        echo "      query4-orchestrator-${i}:"
        echo "        condition: service_started"
    done

    # Query 4 partitioners
    for i in $(seq 1 $Q4_PARTITIONER_COUNT); do
        echo "      query4-partitioner-${i}:"
        echo "        condition: service_started"
    done

    # Query 4 groupby workers
    for i in $(seq 1 $Q4_GROUPBY_WORKER_COUNT); do
        echo "      query4-groupby-worker-${i}:"
        echo "        condition: service_started"
    done

    echo "      query4-top-users-worker-1:"
    echo "        condition: service_started"

    echo "      results-dispatcher-1:"
    echo "        condition: service_started"
    echo "      in-memory-join-orchestrator-1:"
    echo "        condition: service_started"
    echo "      in-file-join-orchestrator-1:"
    echo "        condition: service_started"
    echo "    environment:"
    echo "      - SERVER_PORT=8080"
    echo "      - RABBITMQ_HOST=rabbitmq"
    echo "      - RABBITMQ_PORT=5672"
    echo "      - RABBITMQ_USER=admin"
    echo "      - RABBITMQ_PASS=password"
    echo "      - HEALTH_PORT=8888"
    echo "    volumes:"
    echo "      - proxy-1-data:/app/worker-data"
    echo "    profiles: [\"orchestration\", \"data-flow\"]"
    echo ""
}

generate_server_dependencies >> docker-compose.yaml

# Function to generate client services
generate_clients() {
    local count=$1
    for i in $(seq 1 $count); do
        # Generate CLIENT_ID (CLI1, CLI2, CLI3, etc.)
        local client_id=$(printf "CLI%d" $i)
        
        # Container name matches docker-compose.yaml pattern
        local container_name="client-${i}"
        
        cat >> docker-compose.yaml << EOF
  # Client ${i}
  client-${i}:
    build:
      context: .
      dockerfile: ./client/Dockerfile
    container_name: ${container_name}
    depends_on:
      proxy-1:
        condition: service_started
    volumes:
      - ./data:/app/data
      - ./results:/results  # Mount local results directory
      - /var/run/docker.sock:/var/run/docker.sock  # Access to Docker daemon
    environment:
      - CLIENT_ID=${client_id}
      - CHUNK_SIZE=${CHUNK_SIZE}  # Number of rows per chunk
    command: ["./main", "/app/data", "proxy-1:8080"]
    profiles: ["data-flow"]

EOF
    done
}

# Generate clients
echo -e "${BLUE}Generating clients...${NC}"
generate_clients $CLIENT_COUNT

# Generate the MONITORED_WORKERS list for supervisors
echo -e "${BLUE}Generating MONITORED_WORKERS list for supervisors...${NC}"

MONITORED_WORKERS=""

# Add all year-filter workers
for i in $(seq 1 $YEAR_FILTER_COUNT); do
    MONITORED_WORKERS="${MONITORED_WORKERS}year-filter-worker-${i}:8888,"
done

# Add all time-filter workers
for i in $(seq 1 $TIME_FILTER_COUNT); do
    MONITORED_WORKERS="${MONITORED_WORKERS}time-filter-worker-${i}:8888,"
done

# Add all amount-filter workers
for i in $(seq 1 $AMOUNT_FILTER_COUNT); do
    MONITORED_WORKERS="${MONITORED_WORKERS}amount-filter-worker-${i}:8888,"
done

# Add all query-gateway workers
for i in $(seq 1 $QUERY_GATEWAY_COUNT); do
    MONITORED_WORKERS="${MONITORED_WORKERS}query-gateway-${i}:8888,"
done

# Add all join-data-handler workers
for i in $(seq 1 $JOIN_DATA_HANDLER_COUNT); do
    MONITORED_WORKERS="${MONITORED_WORKERS}join-data-handler-${i}:8888,"
done

# Add all itemid-join workers
for i in $(seq 1 $ITEMID_JOIN_WORKER_COUNT); do
    MONITORED_WORKERS="${MONITORED_WORKERS}itemid-join-worker-${i}:8888,"
done

# Add all storeid-join workers
for i in $(seq 1 $STOREID_JOIN_WORKER_COUNT); do
    MONITORED_WORKERS="${MONITORED_WORKERS}storeid-join-worker-${i}:8888,"
done

# Add user partition components
MONITORED_WORKERS="${MONITORED_WORKERS}user-partition-splitter-1:8888,"

for i in $(seq 1 $USER_PARTITION_WRITERS); do
    MONITORED_WORKERS="${MONITORED_WORKERS}user-partition-writer-${i}:8888,"
done

for i in $(seq 1 $USER_JOIN_READERS); do
    MONITORED_WORKERS="${MONITORED_WORKERS}user-join-reader-${i}:8888,"
done

# Add Query 2 components
for i in $(seq 1 $Q2_PARTITIONER_COUNT); do
    MONITORED_WORKERS="${MONITORED_WORKERS}query2-partitioner-${i}:8888,"
done

for i in $(seq 1 $Q2_GROUPBY_WORKER_COUNT); do
    MONITORED_WORKERS="${MONITORED_WORKERS}query2-orchestrator-${i}:8888,"
    MONITORED_WORKERS="${MONITORED_WORKERS}query2-groupby-worker-${i}:8888,"
done

MONITORED_WORKERS="${MONITORED_WORKERS}query2-top-items-worker-1:8888,"

# Add Query 3 components
for i in $(seq 1 $Q3_PARTITIONER_COUNT); do
    MONITORED_WORKERS="${MONITORED_WORKERS}query3-partitioner-${i}:8888,"
done

for i in $(seq 1 $Q3_GROUPBY_WORKER_COUNT); do
    MONITORED_WORKERS="${MONITORED_WORKERS}query3-orchestrator-${i}:8888,"
    MONITORED_WORKERS="${MONITORED_WORKERS}query3-groupby-worker-${i}:8888,"
done

# Add Query 4 components
for i in $(seq 1 $Q4_PARTITIONER_COUNT); do
    MONITORED_WORKERS="${MONITORED_WORKERS}query4-partitioner-${i}:8888,"
done

for i in $(seq 1 $Q4_GROUPBY_WORKER_COUNT); do
    MONITORED_WORKERS="${MONITORED_WORKERS}query4-orchestrator-${i}:8888,"
    MONITORED_WORKERS="${MONITORED_WORKERS}query4-groupby-worker-${i}:8888,"
done

MONITORED_WORKERS="${MONITORED_WORKERS}query4-top-users-worker-1:8888,"

# Add orchestrators and dispatcher
MONITORED_WORKERS="${MONITORED_WORKERS}in-memory-join-orchestrator-1:8888,"
MONITORED_WORKERS="${MONITORED_WORKERS}in-file-join-orchestrator-1:8888,"
MONITORED_WORKERS="${MONITORED_WORKERS}results-dispatcher-1:8888,"

# Generate TARGET_CONTAINERS for Chaos Monkey (same as MONITORED_WORKERS but without port numbers)
echo -e "${BLUE}Generating TARGET_CONTAINERS for Chaos Monkey...${NC}"

# Convert MONITORED_WORKERS to TARGET_CONTAINERS by removing :8888 port suffixes
TARGET_CONTAINERS=$(echo "${MONITORED_WORKERS}" | sed 's/:8888//g')

# Now update the supervisor services with the MONITORED_WORKERS variable
# Use perl for cross-platform compatibility (works the same on macOS and Linux)
# Dynamically update each supervisor based on SUPERVISOR_COUNT
for i in $(seq 1 $SUPERVISOR_COUNT); do
    # Build the SUPERVISOR_PEERS pattern for this supervisor (to match in the yaml)
    peers_pattern=""
    for j in $(seq 1 $SUPERVISOR_COUNT); do
        if [ $j -ne $i ]; then
            if [ -n "$peers_pattern" ]; then
                peers_pattern="${peers_pattern},"
            fi
            peers_pattern="${peers_pattern}supervisor-${j}:supervisor-${j}:9000"
        fi
    done

    # we must add the other supervisors to MONITORED_WORKERS so as to monitor them too
    other_supervisors=""
    for j in $(seq 1 $SUPERVISOR_COUNT); do
        if [ $j -ne $i ]; then
            other_supervisors="${other_supervisors}supervisor-${j}:8888,"
        fi
    done

    supervisor_monitored_workers="${other_supervisors}${MONITORED_WORKERS}"

    # Use perl to add MONITORED_WORKERS after SUPERVISOR_PEERS for this supervisor
    perl -i -pe "s|SUPERVISOR_PEERS: \"${peers_pattern}\"|\$&\\n      MONITORED_WORKERS: \"${supervisor_monitored_workers}\"|" docker-compose.yaml
done

# Update chaos-monkey with TARGET_CONTAINERS and KILL_STRATEGY
perl -i -pe "s|KILL_STRATEGY: \"exponential\"  # Options: \"periodic\" or \"exponential\"|\$&\\n      TARGET_CONTAINERS: \"${TARGET_CONTAINERS}\"|" docker-compose.yaml

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
      proxy-1:
        condition: service_started
    environment:
      RABBITMQ_URL: "amqp://admin:password@rabbitmq:5672/"
      LOG_LEVEL: "info"
      GOMAXPROCS: "1"
    command: ["go", "test", "-v", "./..."]
    stdin_open: true
    tty: true

volumes:
EOF_FOOTER

# Generate per-worker volumes for user partition writers
echo -e "${BLUE}Generating user partition writer volumes...${NC}"
cat >> docker-compose.yaml << EOF
  # User partition writer volumes
EOF
for i in $(seq 1 $USER_PARTITION_WRITERS); do
    cat >> docker-compose.yaml << EOF
  user-partition-writer-${i}-data:
    driver: local
EOF
done

# Generate per-worker volumes for groupby workers
echo -e "${BLUE}Generating per-worker volumes...${NC}"

# Generate user partition writer volumes
cat >> docker-compose.yaml << EOF
  # User partition writer volumes (each writer has its own volume)
EOF
for i in $(seq 1 $USER_PARTITION_WRITERS); do
    cat >> docker-compose.yaml << EOF
  user-writer-${i}-data:
    driver: local
EOF
done

cat >> docker-compose.yaml << EOF
  # Query 2 worker volumes
EOF
for i in $(seq 1 $Q2_GROUPBY_WORKER_COUNT); do
    cat >> docker-compose.yaml << EOF
  query2-groupby-worker-${i}-data:
    driver: local
EOF
done

cat >> docker-compose.yaml << EOF
  # Query 3 worker volumes
EOF
for i in $(seq 1 $Q3_GROUPBY_WORKER_COUNT); do
    cat >> docker-compose.yaml << EOF
  query3-groupby-worker-${i}-data:
    driver: local
EOF
done

cat >> docker-compose.yaml << EOF
  # Query 4 worker volumes
EOF
for i in $(seq 1 $Q4_GROUPBY_WORKER_COUNT); do
    cat >> docker-compose.yaml << EOF
  query4-groupby-worker-${i}-data:
    driver: local
EOF
done

cat >> docker-compose.yaml << EOF
  # Filter workers volumes
EOF
for i in $(seq 1 $YEAR_FILTER_COUNT); do
    cat >> docker-compose.yaml << EOF
  year-filter-worker-${i}-data:
    driver: local
EOF
done
for i in $(seq 1 $TIME_FILTER_COUNT); do
    cat >> docker-compose.yaml << EOF
  time-filter-worker-${i}-data:
    driver: local
EOF
done
for i in $(seq 1 $AMOUNT_FILTER_COUNT); do
    cat >> docker-compose.yaml << EOF
  amount-filter-worker-${i}-data:
    driver: local
EOF
done

cat >> docker-compose.yaml << EOF
  # Join workers volumes
EOF
for i in $(seq 1 $ITEMID_JOIN_WORKER_COUNT); do
    cat >> docker-compose.yaml << EOF
  itemid-join-worker-${i}-data:
    driver: local
EOF
done
for i in $(seq 1 $STOREID_JOIN_WORKER_COUNT); do
    cat >> docker-compose.yaml << EOF
  storeid-join-worker-${i}-data:
    driver: local
EOF
done
for i in $(seq 1 $JOIN_DATA_HANDLER_COUNT); do
    cat >> docker-compose.yaml << EOF
  join-data-handler-${i}-data:
    driver: local
EOF
done
cat >> docker-compose.yaml << EOF
  user-partition-splitter-1-data:
    driver: local
EOF
for i in $(seq 1 $USER_JOIN_READERS); do
    cat >> docker-compose.yaml << EOF
  user-join-reader-${i}-data:
    driver: local
EOF
done

cat >> docker-compose.yaml << EOF
  # Partitioner volumes
EOF
for i in $(seq 1 $Q2_PARTITIONER_COUNT); do
    if [ $Q2_PARTITIONER_COUNT -eq 1 ]; then
        cat >> docker-compose.yaml << EOF
  query2-partitioner-data:
    driver: local
EOF
    else
        cat >> docker-compose.yaml << EOF
  query2-partitioner-${i}-data:
    driver: local
EOF
    fi
done
for i in $(seq 1 $Q3_PARTITIONER_COUNT); do
    if [ $Q3_PARTITIONER_COUNT -eq 1 ]; then
        cat >> docker-compose.yaml << EOF
  query3-partitioner-data:
    driver: local
EOF
    else
        cat >> docker-compose.yaml << EOF
  query3-partitioner-${i}-data:
    driver: local
EOF
    fi
done
for i in $(seq 1 $Q4_PARTITIONER_COUNT); do
    if [ $Q4_PARTITIONER_COUNT -eq 1 ]; then
        cat >> docker-compose.yaml << EOF
  query4-partitioner-data:
    driver: local
EOF
    else
        cat >> docker-compose.yaml << EOF
  query4-partitioner-${i}-data:
    driver: local
EOF
    fi
done

cat >> docker-compose.yaml << EOF
  # Query gateway and dispatcher volumes
EOF
for i in $(seq 1 $QUERY_GATEWAY_COUNT); do
    cat >> docker-compose.yaml << EOF
  query-gateway-${i}-data:
    driver: local
EOF
done
cat >> docker-compose.yaml << EOF
  results-dispatcher-1-data:
    driver: local
  # Proxy volume
  proxy-1-data:
    driver: local
  # Join orchestrator volumes
  in-memory-join-orchestrator-data:
    driver: local
  in-file-join-orchestrator-data:
    driver: local
  # Top workers volumes
  query2-top-items-worker-data:
    driver: local
  query4-top-users-worker-data:
    driver: local
EOF

echo -e "${GREEN}✓ docker-compose.yaml generated successfully!${NC}"
echo ""
echo -e "${BLUE}Configuration Applied:${NC}"
echo -e "  Year Filter:            ${YELLOW}${YEAR_FILTER_COUNT}${NC} instance(s)"
echo -e "  Time Filter:            ${YELLOW}${TIME_FILTER_COUNT}${NC} instance(s)"
echo -e "  Amount Filter:          ${YELLOW}${AMOUNT_FILTER_COUNT}${NC} instance(s)"
echo -e "  Query Gateway:          ${YELLOW}${QUERY_GATEWAY_COUNT}${NC} instance(s)"
echo -e "  Join Data Handler:      ${YELLOW}${JOIN_DATA_HANDLER_COUNT}${NC} instance(s)"
echo -e "  ItemID Join Workers:    ${YELLOW}${ITEMID_JOIN_WORKER_COUNT}${NC} instance(s)"
echo -e "  StoreID Join Workers:   ${YELLOW}${STOREID_JOIN_WORKER_COUNT}${NC} instance(s)"
echo -e "  User Partition Workers: ${YELLOW}${USER_PARTITION_WORKERS}${NC} paired writer+reader node(s)"
echo -e "  User Partition Writers: ${YELLOW}${USER_PARTITION_WRITERS}${NC} instance(s)"
echo -e "  User Join Readers:      ${YELLOW}${USER_JOIN_READERS}${NC} instance(s)"
echo -e "  Clients:                ${YELLOW}${CLIENT_COUNT}${NC} instance(s)"
echo ""
echo -e "  Total Filter Workers: ${YELLOW}$((YEAR_FILTER_COUNT + TIME_FILTER_COUNT + AMOUNT_FILTER_COUNT))${NC} instances"
echo -e "  User Join Components: ${YELLOW}$((1 + USER_PARTITION_WRITERS + USER_JOIN_READERS))${NC} instances (1 splitter + ${USER_PARTITION_WRITERS} writers + ${USER_JOIN_READERS} readers)"
echo ""
echo -e "${BLUE}Additional services included:${NC}"
echo -e "  - Query 2 Top Items Worker (1 instance)"
echo -e "  - Query 4 Top Users Worker (1 instance)"
echo -e "  - Join Garbage Collector (1 instance)"
echo ""
echo -e "${GREEN}You can now run: ${YELLOW}make docker-compose-up${NC}"
echo ""

