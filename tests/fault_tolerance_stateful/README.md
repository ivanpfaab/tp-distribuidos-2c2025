# Fault Tolerance Stateful Worker Test

A test scenario to verify fault-tolerant stateful workers that can restore their state from persisted CSV metadata.

## Architecture

Three workers in a linear pipeline:
```
Test Runner → queue-1-2 → Worker 2 (Stateful) → queue-2-3 → Worker 3 (Verifier)
```

## Components

- **Test Runner**: Generates chunks with numeric values and sends to Worker 2
- **Worker 2 (Stateful)**: 
  - Receives chunks
  - Maintains per-client state (aggregated sum, received chunks)
  - Persists metadata to CSV files
  - Rebuilds state from CSV on restart
  - Generates result chunks when client is ready
- **Worker 3 (Verifier)**: 
  - Receives chunks from Worker 2
  - Verifies result chunks match expected aggregated sums
  - Prints `SUCCESS [clientID]` or `FAILURE [clientID]`

## Features

### State Persistence

1. **CSV Metadata Storage**: 
   - One CSV file per client: `/app/worker-data/metadata/{clientID}.csv`
   - Format: `msg_id,chunk_number,value`
   - Stores message ID for duplicate detection
   - Stores data needed to rebuild state

2. **State Rebuild**:
   - On startup, reads all CSV files
   - Rebuilds state for non-ready clients
   - Filters duplicates on read (Option B)
   - Deletes CSV files for ready clients

3. **Duplicate Detection**:
   - Checks if message ID exists in CSV before processing
   - Skips already processed messages

4. **State Cleanup**:
   - Deletes CSV file when client becomes ready
   - Removes state from memory

## Running

### Using Docker Compose

```bash
# Navigate to the test directory
cd tests/fault_tolerance_stateful

# Start all services
docker compose up --build

# Or in detached mode
docker compose up --build -d

# View logs
docker compose logs -f

# View logs from specific worker
docker compose logs -f worker-2

# Stop services
docker compose down
```

### Using Makefile

```bash
# Show available commands
make help

# Start all services
make up

# Start in detached mode
make up-detached

# View logs
make logs

# View logs from specific worker
make logs-worker-2

# Stop services
make down
```

### Environment Variables

- `NUM_CHUNKS`: Number of chunks per client (default: 3)
- `EXPECTED_CHUNKS`: Expected chunks for Worker 2 (default: 3)

Example:
```bash
NUM_CHUNKS=5 EXPECTED_CHUNKS=5 docker compose up --build
```

## Testing Fault Tolerance

### Test Scenarios

1. **Normal Flow**: All chunks processed, state built correctly, results generated
2. **Worker Restart**: Kill Worker 2 mid-processing, restart it
   - Should rebuild state from CSV metadata
   - Should skip already processed chunks
   - Should continue from where it left off
   - Should generate correct results

3. **Duplicate Messages**: Send same chunk twice
   - First time: Process normally
   - Second time: Skip (already in CSV)

### Manual Testing

1. **Kill Worker 2**:
   ```bash
   make kill-worker-2
   ```

2. **Restart Worker 2**:
   ```bash
   make restart-worker-2
   ```

3. **Monitor Logs**:
   ```bash
   make logs-worker-2
   ```

4. **Check Metadata Files**:
   ```bash
   make check-metadata
   make view-metadata CLIENT=CLI1
   ```

## Expected Output

### Worker 2 Logs
```
Worker 2: Rebuilding state from metadata...
Worker 2: Rebuilt state for client CLI1 (2 unique rows)
Worker 2: Processing chunk 1 for client CLI1
Worker 2: Client CLI1 not ready yet (1/3 chunks, sum: 10)
Worker 2: Client CLI1 is ready! Aggregated sum: 60
Worker 2: Sent result chunk for client CLI1 (sum: 60)
```

### Worker 3 Logs
```
Worker 3: Expected sum for client CLI1: 60
Worker 3: SUCCESS [CLI1] - Expected: 60, Received: 60
```

## File Structure

### Metadata Files
- Location: `/app/worker-data/metadata/`
- Format: `{clientID}.csv`
- Example: `CLI1.csv`, `CLI2.csv`
- CSV format:
  ```csv
  msg_id,chunk_number,value
  CLI100000001,1,10
  CLI100000002,2,20
  CLI100000003,3,30
  ```

## Integration with Production System

The `StatefulWorkerManager` component in `shared/stateful_worker/` is designed to be reusable in the production system. It can be integrated with:

- `workers/top/query2_top_classification/` - Top items worker
- `workers/top/query4_top_classification/` - Top users worker
- `workers/group_by/shared/orchestrator/` - GroupBy orchestrators
- `dispatcher/` - Results dispatcher

The component handles:
- CSV metadata persistence
- State rebuild from metadata
- Duplicate detection
- Memory-efficient file reading

## StatefulWorkerManager Usage

```go
// Define CSV columns
csvColumns := []string{"msg_id", "chunk_number", "user_id", "store_id", "purchase_count"}

// Create state manager
stateManager := stateful_worker.NewStatefulWorkerManager(
    "/app/worker-data/metadata",
    buildStatusCallback,      // Function to rebuild state from CSV rows
    extractMetadataCallback,  // Function to extract CSV row from message
    csvColumns,
)

// Rebuild state on startup
stateManager.RebuildState()

// Process messages
stateManager.ProcessMessage(chunkMsg)

// Check if client ready
state := stateManager.GetClientState(clientID)
if state.IsReady() {
    // Generate results
    stateManager.MarkClientReady(clientID)
}
```

