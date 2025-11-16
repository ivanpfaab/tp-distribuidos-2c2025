# Fault Tolerance Test System

A minimal linear pipeline test system to verify fault tolerance behavior with chunk protocol messages.

## Architecture

Three workers in a linear pipeline ending with a final consumer:
```
Test Runner → queue-1-2 → Worker 1 → queue-2-3 → Worker 2 → queue-3-final → Worker 3 → queue-final → Final Consumer
```

## Components

- **Worker**: Receives chunks, waits 3 seconds, forwards to next worker
- **Test Runner**: Sends 100 chunks to start the pipeline
- **Final Consumer**: Consumes chunks from Worker 3 and prints the results
- **RabbitMQ**: Message broker (port 5673 to avoid conflicts)

## Running

### Using Makefile (Recommended)

```bash
# Navigate to the fault_tolerance directory
cd tests/fault_tolerance

# Show available commands
make help

# Start all services
make up

# Start in detached mode
make up-detached

# View logs
make logs

# View logs from specific worker
make logs-worker-1

# Stop services
make down
```

### Using Docker Compose Directly

```bash
# Start all services
docker compose -f tests/fault_tolerance/docker-compose.yaml up --build

# Or in detached mode
docker compose -f tests/fault_tolerance/docker-compose.yaml up --build -d

# View logs
docker compose -f tests/fault_tolerance/docker-compose.yaml logs -f

# Stop services
docker compose -f tests/fault_tolerance/docker-compose.yaml down
```

## Testing Fault Tolerance

### Using Makefile

1. **Kill a worker**:
   ```bash
   make kill-worker-2
   ```

2. **Restart the worker**:
   ```bash
   make restart-worker-2
   ```

### Using Docker Directly

1. **Kill a worker**:
   ```bash
   docker kill fault-tolerance-worker-2
   ```

2. **Restart the worker**:
   ```bash
   docker start fault-tolerance-worker-2
   ```

3. **Monitor queues**:
   - RabbitMQ Management UI: http://localhost:15673 (admin/password)
   - Check queue depths and message flow

## Queue Configuration

- **Durable**: `false` (non-durable queues)
- **Auto-delete**: `true` (queues deleted when unused)
- **Exclusive**: `false`

## Behavior

- Each worker waits 3 seconds before forwarding
- Basic logging: Worker ID, received/sent messages
- Final consumer prints all received chunks with their data
- 100 chunks are sent through the linear pipeline

