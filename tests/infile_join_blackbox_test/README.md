# In-File Join Black Box Test

This test verifies the end-to-end functionality of the in-file join pipeline for user data partitioning.

## What This Test Does

1. **Generates Test Data**: 1000 user records for each of 2 clients
2. **Sends Chunks**: Breaks data into chunks and sends them to the `join-userid-dictionary` queue
3. **Monitors Volume**: Tracks the size of partition files in `/shared-data` at key points:
   - Before sending data
   - When orchestrator receives first chunk notification
   - When orchestrator marks client as completed
   - After orchestrator sends completion signals
4. **Verifies Flow**: Ensures partition files are created and orchestrator signals completion

## System Architecture

The test runs the following components:

- **RabbitMQ**: Message broker
- **1 Orchestrator**: Tracks completion and signals join workers
- **1 Partitioner** (user-partition-splitter): Receives chunks and routes to writers
- **5 Writers** (user-partition-writer): Write partition files to shared volume
- **1 Join Worker** (user-join): Performs joins (cleanup is its responsibility, not tested here)

## Data Flow

1. **Test → Partitioner Queue**: Sends user chunks to `join-userid-dictionary`
2. **Partitioner → Writer Queues**: Splits data across 5 writers (`users-post-partition-1` through `users-post-partition-5`)
3. **Writers → Shared Volume**: Each writer creates partition files like `{clientID}-users-partition-{XXX}.csv`
4. **Writers → Orchestrator**: Send chunk notifications to `user-partition-completion`
5. **Orchestrator → Join Workers**: Broadcasts completion signals via exchange

## File Structure

Each client generates files in `/shared-data`:
- Pattern: `{clientID}-users-partition-{000-099}.csv`
- Example: `CLI1-users-partition-023.csv`
- 100 total partitions distributed across 5 writers
- Each writer owns ~20 partitions (partition % 5 == writerID-1)

## Running the Test

```bash
# Start all services and run the test
docker compose -f docker-compose-infile-join-test.yaml up --build

# Or run in detached mode and follow test logs
docker compose -f docker-compose-infile-join-test.yaml up --build -d
docker logs -f infile-join-test-runner
```

## Understanding the Output

The test will log:
- Number of chunks sent per client
- File sizes at different stages
- Orchestrator completion signals
- Final statistics

### Success Criteria:

✅ **Files created**: Partition files exist during processing
✅ **Completion signals sent**: Orchestrator signals all clients completed
✅ **Files remain**: Files NOT deleted (join worker's responsibility)

## Test Data

- **Generated at runtime**: `testdata/users_test.csv`
- **Records per client**: 1000
- **Chunk size**: 50 records per chunk
- **Schema**: `user_id,gender,birthdate,registered_at`

## Cleanup

To stop and remove all containers and volumes:

```bash
docker compose -f docker-compose-infile-join-test.yaml down -v
```

## Troubleshooting

### No files created
- Check writer logs: `docker logs user-partition-writer-1-test`
- Verify partitioner is routing correctly: `docker logs user-partition-splitter-test`
- Check that SharedDataDir (/shared-data) is mounted correctly

### No completion signals
- Check orchestrator logs: `docker logs infile-join-orchestrator-test`
- Verify chunk notifications are being sent by writers
- Check orchestrator received all file notifications

### Test hangs or times out
- Increase timeout in test
- Check RabbitMQ management UI: http://localhost:15674 (admin/password)
- Verify all services are running: `docker ps`
- Check queue depths in RabbitMQ

## Differences from GroupBy Test

Unlike the GroupBy test, this test:
- **Does NOT verify cleanup**: Files remain after orchestrator completes (join worker deletes them)
- **Monitors flat file structure**: All files in one directory with client prefix
- **Uses 5 writers**: More parallel processing than GroupBy (3 workers)
- **Different queue**: `join-userid-dictionary` instead of `query2-groupby-queue`
