# Fault Tolerance Partition Writes Test

A test scenario to verify fault-tolerant partition writing with duplicate prevention and incomplete write handling.

## Architecture

Three workers in a linear pipeline:
```
Test Runner → queue-1-2 → Worker 2 (Partition Writer) → queue-2-3 → Worker 3 (Verifier)
```

**Note:** Worker 1 is included for consistency but test-runner sends directly to Worker 2.

## Components

- **Test Runner**: Generates CSV chunks with `user_id,name` data and sends to Worker 2
- **Worker 2 (Partition Writer)**: 
  - Receives CSV chunks
  - Partitions data by `user_id % NUM_PARTITIONS`
  - Writes to partition files with fault tolerance
  - Tracks processed chunks and partition-chunk pairs
  - Handles incomplete writes (missing `\n`)
  - Forwards chunks to Worker 3
- **Worker 3 (Verifier)**: 
  - Receives chunks from Worker 2
  - Verifies all CSV lines exist in correct partition files
  - Prints `SUCCESS [chunkID]` or `FAILURE [chunkID]`

## Features

### Fault Tolerance

1. **Duplicate Prevention**: 
   - Tracks processed chunk IDs (prevents reprocessing same chunk)
   - Tracks partition-chunk pairs (prevents rewriting same data from same chunk)

2. **Incomplete Write Handling**:
   - Detects incomplete lines (missing `\n` at end of file)
   - Fixes incomplete lines on restart
   - Handles duplicate detection when fixing incomplete lines

3. **State Persistence**:
   - `processed-chunks.txt`: Chunk IDs that were fully processed
   - `written-partition-chunks.txt`: Partition-chunk pairs that were written
   - State files persist across worker restarts

### Partition Manager

The `PartitionManager` (in `shared/partition_manager/`) provides:
- Fault-tolerant partition writing
- Incomplete line detection and fixing
- Duplicate line detection
- Short read/write handling
- Automatic CSV header writing

## Running

### Using Docker Compose

```bash
# Navigate to the test directory
cd tests/fault_tolerance_partition_writes

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

### Environment Variables

- `NUM_PARTITIONS`: Number of partitions (default: 5)
- `NUM_CHUNKS`: Number of chunks to send (default: 20)

Example:
```bash
NUM_PARTITIONS=10 NUM_CHUNKS=50 docker compose up --build
```

## Testing Fault Tolerance

### Test Scenarios

1. **Normal Flow**: All chunks processed, partitions written correctly
2. **Worker Restart**: Kill Worker 2 mid-processing, restart it
   - Should skip already written partitions
   - Should fix incomplete writes
   - Should continue from where it left off

3. **Duplicate Chunks**: Send same chunk twice
   - First time: Process normally
   - Second time: Skip (already processed)

4. **Incomplete Write**: Kill Worker 2 while writing a partition
   - Restart: Should detect incomplete line
   - Should fix incomplete line and continue

### Manual Testing

1. **Kill Worker 2**:
   ```bash
   docker kill partition-writes-worker-2
   ```

2. **Restart Worker 2**:
   ```bash
   docker start partition-writes-worker-2
   ```

3. **Monitor Logs**:
   ```bash
   docker compose logs -f worker-2 worker-3
   ```

4. **Check Partition Files**:
   ```bash
   docker exec partition-writes-worker-2 ls -la /app/worker-data/partitions/
   docker exec partition-writes-worker-2 cat /app/worker-data/partitions/TEST-users-partition-000.csv
   ```

5. **Check State Files**:
   ```bash
   docker exec partition-writes-worker-2 cat /app/worker-data/state/processed-chunks.txt
   docker exec partition-writes-worker-2 cat /app/worker-data/state/written-partition-chunks.txt
   ```

## Expected Output

### Worker 2 Logs
```
Worker 2: Processing chunk <chunkID>
Worker 2: Wrote partition 0 from chunk <chunkID>
Worker 2: Wrote partition 1 from chunk <chunkID>
Worker 2: Completed processing chunk <chunkID>
```

### Worker 3 Logs
```
SUCCESS [<chunkID>]
```

## File Structure

### Partition Files
- Location: `/app/worker-data/partitions/`
- Format: `{clientID}-users-partition-{XXX}.csv`
- Example: `TEST-users-partition-000.csv`, `TEST-users-partition-001.csv`

### State Files
- Location: `/app/worker-data/state/`
- `processed-chunks.txt`: One chunk ID per line
- `written-partition-chunks.txt`: One partition-chunk pair per line (format: `{partitionNum}:{chunkID}`)

## Queue Configuration

- **Durable**: `false` (non-durable queues)
- **Auto-delete**: `false` (queues persist)
- **Exclusive**: `false`

## Integration with Production System

The `PartitionManager` component in `shared/partition_manager/` is designed to be reusable in the production system. It can be integrated with:

- `workers/join/in-file/user-id/user-partition-writer/`
- Any other workers that write partitioned data

The component handles:
- Fault-tolerant writing
- Incomplete write detection and fixing
- Duplicate prevention
- Short read/write handling

