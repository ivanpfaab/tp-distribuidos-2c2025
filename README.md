# Distributed Data Processing System

A distributed system for processing CSV transaction data through multiple query pipelines using RabbitMQ message queues, Docker containers, and fault-tolerant worker architecture.

## System Overview

This system processes transaction data from CSV files through 4 different query pipelines:

- **Query 1**: Filters transactions by year (2024-2025)
- **Query 2**: Groups transaction items by year, month, and item_id (best-selling items)
- **Query 3**: Groups transactions by year, semester, and store_id (total payment volume per store)
- **Query 4**: Groups transactions by user_id and store_id (most purchases per user-store combination)

### Architecture

- **Proxy**: TCP server that receives batch data from clients and routes it to workers
- **Workers**: Specialized workers for filtering, joining, grouping, and aggregating data
- **Query Gateway**: Routes processed data to appropriate query pipelines
- **Results Dispatcher**: Collects and formats results from all queries, sends to clients
- **Supervisor**: Monitors worker health and manages fault tolerance (Bully election algorithm)
- **RabbitMQ**: Message broker for inter-service communication
- **Clients**: Send CSV data batches to the proxy via TCP

### Key Features

- **Fault Tolerance**: State persistence, automatic worker recovery, supervisor-based health monitoring
- **Message Deduplication**: Prevents duplicate processing using persistent message tracking
- **Partitioned Processing**: Data is partitioned across multiple workers for parallel processing
- **State Recovery**: Workers can recover state after restarts from persisted metadata

## Prerequisites

- Docker and Docker Compose installed
- Go 1.21+ (for local development)
- At least 4GB of available RAM (recommended)

## Quick Start

### 1. Start the System

```bash
make cleanup

make volume-cleanup

# Start all services in proper 
make docker-compose-up
```

This will:
1. Start RabbitMQ and wait for it to be healthy
2. Start all orchestration services (workers, gateways, supervisors)
3. Start the proxy
4. Start clients that will process data from the `data/` directory

### 2. Monitor the System

```bash
# View all logs
make docker-compose-logs

# View logs for specific service
make docker-compose-logs SERVICE=proxy-1
```

### 3. Monitor Node Status

Use the monitoring script to see real-time status of all nodes:

```bash
./monitor-nodes.sh
```

This script displays:
- Node status (running/stopped)
- Health status
- Supervisor leader election
- Real-time updates

### 4. Stop the System

```bash
# Stop all services
make docker-compose-down
```

## Testing

### Test with Fault Injection

Test the system's fault tolerance with Chaos Monkey:

```bash
# Start system with Chaos Monkey enabled
make docker-compose-up-chaos

# Monitor Chaos Monkey and supervisors
make docker-compose-logs-chaos
```

Chaos Monkey will randomly kill/pause/stop containers to test recovery mechanisms.

### Results Comparison

After processing, compare results with source of truth:

```bash
./compare_results.sh
```

This compares the generated results in `results/` with expected results in `results_source_of_truth/`.

Note that we use, as our reduced dataset, the following files:
* transactions_*: 202401, 202406, 202407, 202501, 202506
* transaction_items_*:  202401, 202406, 202407, 202501, 202506
* other files: all files

To make sure ./compare_results.sh works as expected, we must have these files in the /data folder or create our own results_source_of_truth files accordingly.

To test the full dataset and compare the full results, we must have all files from the dataset in the /data folder, and change the name of the /results_source_of_truth_full folder to /results_source_of_truth

## Makefile Commands

### Basic Operations

- `make docker-compose-up` - Start all services in orchestrated order
- `make docker-compose-up-chaos` - Start with Chaos Monkey for fault injection testing
- `make docker-compose-down` - Stop all services
- `make docker-compose-build` - Build all Docker images

### Logging

- `make docker-compose-logs` - Show logs from all services (use `SERVICE=name` for specific service)
- `make docker-compose-logs-chaos` - Show logs for Chaos Monkey and supervisors

### Testing & Development

- `make docker-rebuild` - Full Docker cleanup and rebuild

### Cleanup

- `make cleanup` - Cleanup services, images, and volumes
- `make cleanup-excluding-chaos` - Cleanup preserving chaos-monkey image
- `make volume-cleanup` - Clean up all Docker volumes and system resources

### Configuration

- `./generate-compose.sh` - Interactive script to configure the system by generating a custom `docker-compose.yaml` with:
  - Custom worker scaling (number of filter workers, join workers, groupby workers, etc.)
  - Chunk size configuration (rows per chunk)
  - Gateway and client scaling

### Help

- `make help` - Show all available commands

## Data Directory Structure

The `data/` directory contains CSV files organized by type:

```
data/
├── transactions/          # Transaction files (TR*.csv)
├── transaction_items/     # Transaction item files (TI*.csv)
├── stores/               # Store reference data (ST*.csv)
├── menu_items/           # Menu item reference data (MN*.csv)
├── users/               # User files (US*.csv)
├── payment_methods/     # Payment method reference data
└── vouchers/           # Voucher reference data
```

## Results

Processed results are saved in:
- `results/results_CLI1.txt` - Results for client 1
- `results/results_CLI2.txt` - Results for client 2

Expected results (source of truth) are in:
- `results_source_of_truth/` - Contains expected CSV outputs for each query

## Troubleshooting

### Services won't start
- Check if RabbitMQ is healthy: `docker compose ps rabbitmq`
- Check logs: `make docker-compose-logs SERVICE=rabbitmq`
- Ensure ports are not in use

### Workers not processing
- Check worker logs: `make docker-compose-logs SERVICE=year-filter-worker-1`
- Verify RabbitMQ queues are created
- Check supervisor status: `make docker-compose-logs SERVICE=supervisor-1`

### Results are incorrect
- Compare with source of truth: `./compare_results.sh`
- Verify data files are in correct format

### Clean restart
```bash
make cleanup
make volume-cleanup
make docker-compose-up
```

## Project Structure

```
tp-distribuidos-2c2025/
├── proxy/              # TCP proxy server
├── client/             # Client applications
├── workers/           # Processing workers (filter, join, group_by, top)
├── dispatcher/        # Results dispatcher
├── query-gateway/     # Query routing gateway
├── supervisor/        # Fault tolerance supervisor
├── chaos-monkey/      # Fault injection tool
├── shared/            # Shared utilities and middleware
├── protocol/          # Message protocol definitions
├── data/              # Input CSV data files
├── results/           # Generated results
└── tests/             # Test suites
```

