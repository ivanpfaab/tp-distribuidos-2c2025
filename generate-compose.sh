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

ITEMID_JOIN_WORKER_COUNT=$(prompt_worker_count "itemid-join-worker" 2)
STOREID_JOIN_WORKER_COUNT=$(prompt_worker_count "storeid-join-worker" 2)

echo ""
echo -e "${BLUE}Configure User Join Workers (Query 4 - distributed write/read)${NC}"
echo ""

USER_PARTITION_WRITERS=$(prompt_worker_count "user-partition-writer" 5)
USER_JOIN_READERS=$(prompt_worker_count "user-join-reader" 2)
echo ""
echo -e "${BLUE}Configure Group By Components${NC}"
echo ""

Q2_PARTITIONER_COUNT=$(prompt_worker_count "query2-partitioner" 1)
Q2_GROUPBY_WORKER_COUNT=3  # Fixed at 3 for Query 2
Q3_PARTITIONER_COUNT=$(prompt_worker_count "query3-partitioner" 1)
Q3_GROUPBY_WORKER_COUNT=3  # Fixed at 3 for Query 3
Q4_PARTITIONER_COUNT=$(prompt_worker_count "query4-partitioner" 1)
Q4_GROUPBY_WORKER_COUNT=$(prompt_worker_count "query4-groupby-worker" 3)

echo ""
echo -e "${BLUE}Configure Clients${NC}"
echo ""

CLIENT_COUNT=$(prompt_worker_count "client" 1)

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
echo ""
echo -e "Query 2 Partitioners:   ${YELLOW}${Q2_PARTITIONER_COUNT}${NC} instance(s)"
echo -e "Query 2 GroupBy Workers: ${YELLOW}${Q2_GROUPBY_WORKER_COUNT}${NC} instance(s)"
echo -e "Query 3 Partitioners:   ${YELLOW}${Q3_PARTITIONER_COUNT}${NC} instance(s)"
echo -e "Query 3 GroupBy Workers: ${YELLOW}${Q3_GROUPBY_WORKER_COUNT}${NC} instance(s)"
echo -e "Query 4 Partitioners:   ${YELLOW}${Q4_PARTITIONER_COUNT}${NC} instance(s)"
echo -e "Query 4 GroupBy Workers: ${YELLOW}${Q4_GROUPBY_WORKER_COUNT}${NC} instance(s)"

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

# Function to generate itemid-join-worker services
generate_itemid_join_workers() {
    local count=$1
    for i in $(seq 1 $count); do
        # First worker has simpler container name for backward compatibility
        local container_name
        if [ $i -eq 1 ]; then
            container_name="itemid-join-worker"
        else
            container_name="itemid-join-worker-${i}"
        fi
        
        cat >> docker-compose.yaml << EOF
  # ItemID Join Worker ${i} (scalable with dictionary broadcasting)
  itemid-join-worker-${i}:
    build:
      context: .
      dockerfile: ./workers/join/in-memory/itemid/Dockerfile
    container_name: ${container_name}
    depends_on:
      rabbitmq:
        condition: service_healthy
      join-data-handler-1:
        condition: service_started
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      WORKER_INSTANCE_ID: "${i}"
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
    for i in $(seq 1 $count); do
        # First worker has simpler container name for backward compatibility
        local container_name
        if [ $i -eq 1 ]; then
            container_name="storeid-join-worker"
        else
            container_name="storeid-join-worker-${i}"
        fi
        
        cat >> docker-compose.yaml << EOF
  # StoreID Join Worker ${i} (scalable with dictionary broadcasting)
  storeid-join-worker-${i}:
    build:
      context: .
      dockerfile: ./workers/join/in-memory/storeid/Dockerfile
    container_name: ${container_name}
    depends_on:
      rabbitmq:
        condition: service_healthy
      join-data-handler-1:
        condition: service_started
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      WORKER_INSTANCE_ID: "${i}"
      STOREID_BATCH_SIZE: 5
    profiles: ["orchestration"]

EOF
    done
}

# Generate storeid-join-workers
echo -e "${BLUE}Generating storeid-join-workers...${NC}"
generate_storeid_join_workers $STOREID_JOIN_WORKER_COUNT

# Add remaining non-scalable services
cat >> docker-compose.yaml << 'EOF_REMAINING'
  # Query 2 Group By Orchestrator
  query2-orchestrator:
    build:
      context: .
      dockerfile: ./workers/group_by/shared/orchestrator/Dockerfile
    container_name: query2-orchestrator
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      QUERY_TYPE: "2"
    volumes:
      - query2-groupby-data:/app/groupby-data
    profiles: ["orchestration"]

  # Query 3 Group By Orchestrator
  query3-orchestrator:
    build:
      context: .
      dockerfile: ./workers/group_by/shared/orchestrator/Dockerfile
    container_name: query3-orchestrator
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      QUERY_TYPE: "3"
    volumes:
      - query3-groupby-data:/app/groupby-data
    profiles: ["orchestration"]

  # Query 4 Group By Orchestrator
  query4-orchestrator:
    build:
      context: .
      dockerfile: ./workers/group_by/shared/orchestrator/Dockerfile
    container_name: query4-orchestrator
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      QUERY_TYPE: "4"
    volumes:
      - query4-groupby-data:/app/groupby-data
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

  in-memory-join-orchestrator:
    build:
      context: .
      dockerfile: ./workers/join/in-memory/orchestrator/Dockerfile
    container_name: in-memory-join-orchestrator
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
    profiles: ["orchestration"]

  in-file-join-orchestrator:
    build:
      context: .
      dockerfile: ./workers/join/in-file/orchestrator/Dockerfile
    container_name: in-file-join-orchestrator
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


# Function to generate partitioner services
generate_partitioner() {
    local query_type=$1
    local count=$2
    local num_workers=$3

    # Determine NUM_PARTITIONS based on query type
    local num_partitions
    if [ "$query_type" = "2" ] || [ "$query_type" = "3" ]; then
        num_partitions="3"  # Time-based partitions (3 semesters)
    else
        num_partitions="100"  # Default for Query 4 (configurable)
    fi

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
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      QUERY_TYPE: "${query_type}"
      NUM_PARTITIONS: "${num_partitions}"
      NUM_WORKERS: "${num_workers}"
    profiles: ["orchestration"]

EOF
    done
}

# Function to generate groupby worker services
generate_groupby_workers() {
    local query_type=$1
    local count=$2
    local partitioner_count=$3

    # Determine NUM_PARTITIONS based on query type
    local num_partitions
    if [ "$query_type" = "2" ] || [ "$query_type" = "3" ]; then
        num_partitions="3"  # Time-based partitions (3 semesters)
    else
        num_partitions="100"  # Default for Query 4 (configurable)
    fi

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
      query${query_type}-orchestrator:
        condition: service_started
$(echo -e "${partitioner_deps}")    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      QUERY_TYPE: "${query_type}"
      WORKER_ID: "${i}"
      NUM_WORKERS: "${count}"
      NUM_PARTITIONS: "${num_partitions}"
    volumes:
      - query${query_type}-groupby-data:/app/groupby-data
    profiles: ["orchestration"]

EOF
    done
}

# Function to generate top classification workers
generate_top_worker() {
    local query_type=$1
    local worker_name=$2
    local groupby_worker_count=$3

    # Build dependencies on all groupby workers
    local groupby_deps=""
    for w in $(seq 1 $groupby_worker_count); do
        groupby_deps="${groupby_deps}      query${query_type}-groupby-worker-${w}:
        condition: service_started
"
    done

    cat >> docker-compose.yaml << EOF
  # Query ${query_type} Top ${worker_name} Classification
  query${query_type}-top-${worker_name}-worker:
    build:
      context: .
      dockerfile: ./workers/top/query${query_type}_top_classification/Dockerfile
    container_name: query${query_type}-top-${worker_name}-worker
    depends_on:
      rabbitmq:
        condition: service_healthy
$(echo -e "${groupby_deps}")    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
    profiles: ["orchestration"]

EOF
}

# Function to generate user-partition-splitter
generate_user_partition_splitter() {
    local num_writers=$1
    cat >> docker-compose.yaml << EOF
  # User Partition Splitter (distributes users to writers)
  user-partition-splitter:
    build:
      context: .
      dockerfile: ./workers/join/in-file/user-id/user-partition-splitter/Dockerfile
    container_name: user-partition-splitter
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      NUM_WRITERS: ${num_writers}
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
      user-partition-splitter:
        condition: service_started
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      WRITER_ID: ${i}
      NUM_WRITERS: ${count}
    volumes:
      - shared-data:/shared-data
    profiles: ["orchestration"]

EOF
    done
}

# Function to generate user-join-reader services (scalable readers)
generate_user_join_readers() {
    local count=$1
    for i in $(seq 1 $count); do
        # First reader has simpler container name for backward compatibility
        local container_name
        if [ $i -eq 1 ]; then
            container_name="user-join-reader"
        else
            container_name="user-join-reader-${i}"
        fi
        
        cat >> docker-compose.yaml << EOF
  # User Join Reader ${i} (Query 4 - reads from partition files)
  user-join-reader-${i}:
    build:
      context: .
      dockerfile: ./workers/join/in-file/user-id/user-join/Dockerfile
    container_name: ${container_name}
    depends_on:
      rabbitmq:
        condition: service_healthy
      user-partition-writer-1:
        condition: service_started
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
    volumes:
      - shared-data:/shared-data
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
        # First handler has simpler container name for backward compatibility
        local container_name
        if [ $i -eq 1 ]; then
            container_name="join-data-handler"
        else
            container_name="join-data-handler-${i}"
        fi
        
        cat >> docker-compose.yaml << EOF
  # Join Data Handler ${i}
  join-data-handler-${i}:
    build:
      context: .
      dockerfile: ./workers/join/data-handler/Dockerfile
    container_name: ${container_name}
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: admin
      RABBITMQ_PASS: password
      ITEMID_WORKER_COUNT: "${itemid_worker_count}"
      STOREID_WORKER_COUNT: "${storeid_worker_count}"
    profiles: ["orchestration"]

EOF
    done
}

# Function to generate query-gateway services
generate_query_gateway() {
    local count=$1
    for i in $(seq 1 $count); do
        # First gateway has simpler container name for backward compatibility
        local container_name
        if [ $i -eq 1 ]; then
            container_name="query-gateway"
        else
            container_name="query-gateway-${i}"
        fi
        
        cat >> docker-compose.yaml << EOF
  # Query Gateway ${i}
  query-gateway-${i}:
    build:
      context: .
      dockerfile: ./query-gateway/Dockerfile
    container_name: ${container_name}
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

# Generate join data handlers
echo -e "${BLUE}Generating join data handlers...${NC}"
generate_join_data_handler $JOIN_DATA_HANDLER_COUNT $ITEMID_JOIN_WORKER_COUNT $STOREID_JOIN_WORKER_COUNT

# Generate user partition components
echo -e "${BLUE}Generating user partition splitter...${NC}"
generate_user_partition_splitter $USER_PARTITION_WRITERS

echo -e "${BLUE}Generating user partition writers...${NC}"
generate_user_partition_writers $USER_PARTITION_WRITERS

echo -e "${BLUE}Generating user join readers...${NC}"
generate_user_join_readers $USER_JOIN_READERS

# Generate query gateways
echo -e "${BLUE}Generating query gateways...${NC}"
generate_query_gateway $QUERY_GATEWAY_COUNT


# Generate Query 2 components
echo -e "${BLUE}Generating Query 2 partitioners and groupby workers...${NC}"
generate_partitioner 2 $Q2_PARTITIONER_COUNT $Q2_GROUPBY_WORKER_COUNT
generate_groupby_workers 2 $Q2_GROUPBY_WORKER_COUNT $Q2_PARTITIONER_COUNT
generate_top_worker 2 "items" $Q2_GROUPBY_WORKER_COUNT

# Generate Query 3 components
echo -e "${BLUE}Generating Query 3 partitioners and groupby workers...${NC}"
generate_partitioner 3 $Q3_PARTITIONER_COUNT $Q3_GROUPBY_WORKER_COUNT
generate_groupby_workers 3 $Q3_GROUPBY_WORKER_COUNT $Q3_PARTITIONER_COUNT

# Generate Query 4 components
echo -e "${BLUE}Generating Query 4 partitioners and groupby workers...${NC}"
generate_partitioner 4 $Q4_PARTITIONER_COUNT $Q4_GROUPBY_WORKER_COUNT
generate_groupby_workers 4 $Q4_GROUPBY_WORKER_COUNT $Q4_PARTITIONER_COUNT
generate_top_worker 4 "users" $Q4_GROUPBY_WORKER_COUNT


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
    echo "      user-partition-splitter:"
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
    echo "      query2-orchestrator:"
    echo "        condition: service_started"

    # Query 2 partitioners
    for i in $(seq 1 $Q2_PARTITIONER_COUNT); do
        if [ $Q2_PARTITIONER_COUNT -eq 1 ]; then
            echo "      query2-partitioner:"
        else
            echo "      query2-partitioner-${i}:"
        fi
        echo "        condition: service_started"
    done

    # Query 2 groupby workers
    for i in $(seq 1 $Q2_GROUPBY_WORKER_COUNT); do
        echo "      query2-groupby-worker-${i}:"
        echo "        condition: service_started"
    done

    echo "      query2-top-items-worker:"
    echo "        condition: service_started"

    echo "      query3-orchestrator:"
    echo "        condition: service_started"

    # Query 3 partitioners
    for i in $(seq 1 $Q3_PARTITIONER_COUNT); do
        if [ $Q3_PARTITIONER_COUNT -eq 1 ]; then
            echo "      query3-partitioner:"
        else
            echo "      query3-partitioner-${i}:"
        fi
        echo "        condition: service_started"
    done

    # Query 3 groupby workers
    for i in $(seq 1 $Q3_GROUPBY_WORKER_COUNT); do
        echo "      query3-groupby-worker-${i}:"
        echo "        condition: service_started"
    done

    echo "      query4-orchestrator:"
    echo "        condition: service_started"

    # Query 4 partitioners
    for i in $(seq 1 $Q4_PARTITIONER_COUNT); do
        if [ $Q4_PARTITIONER_COUNT -eq 1 ]; then
            echo "      query4-partitioner:"
        else
            echo "      query4-partitioner-${i}:"
        fi
        echo "        condition: service_started"
    done

    # Query 4 groupby workers
    for i in $(seq 1 $Q4_GROUPBY_WORKER_COUNT); do
        echo "      query4-groupby-worker-${i}:"
        echo "        condition: service_started"
    done

    echo "      query4-top-users-worker:"
    echo "        condition: service_started"

    echo "      streaming-service:"
    echo "        condition: service_started"
    echo "      in-memory-join-orchestrator:"
    echo "        condition: service_started"
    echo "      in-file-join-orchestrator:"
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
  query2-groupby-data:
    driver: local
  query3-groupby-data:
    driver: local
  query4-groupby-data:
    driver: local
EOF_FOOTER

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

