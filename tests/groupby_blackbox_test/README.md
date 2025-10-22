# Group By Black Box Test

This test verifies the end-to-end functionality of the Query 2 group by system, with a focus on proper file cleanup in the shared volume.

## What This Test Does

1. **Generates Test Data**: 1000 transaction items distributed across 3 semesters (2024-S1, 2024-S2, 2025-S1)
2. **Sends Chunks**: Breaks data into chunks and sends them to the `query2-groupby-queue`
3. **Monitors Volume**: Tracks the size of `/app/groupby-data` every 100ms to verify file cleanup
4. **Verifies Cleanup**: Ensures that files are properly deleted after processing

## System Architecture

The test runs the following components:

- **RabbitMQ**: Message broker
- **1 Orchestrator**: Aggregates results and cleans up files
- **2 Partitioners**: Receive chunks and partition data by semester
- **3 Workers**: Process partitioned data and write to shared volume

## Volume Monitoring

The test monitors the shared volume (`groupby-test-data`) which is mounted at `/app/groupby-data` in all containers.

### File Lifecycle:
1. **Workers write** JSON files: `/app/groupby-data/q2/<clientID>/<partition>.json`
2. **Orchestrator reads** these files when all chunks are processed
3. **Orchestrator deletes** files and the client directory after sending results

### Expected Behavior:
- Volume size should **increase** as workers write aggregated data
- Volume size should **decrease** dramatically after orchestrator cleanup
- Final size should be **close to zero** (< 1 MB)

## Running the Test

```bash
# Start all services and run the test
docker compose -f docker-compose-groupby-test.yaml up --build

# Or run in detached mode and follow test logs
docker compose -f docker-compose-groupby-test.yaml up --build -d
docker logs -f groupby-test-runner
```

## Understanding the Output

The test will log:
- Number of chunks sent
- Volume size every 1 second (every 10 measurements)
- Final statistics:
  - Max size (peak memory usage)
  - Min size
  - Final size
  - Size reduction percentage

### Success Criteria:

✅ **Size reduction >= 50%**: Files are being cleaned up
✅ **Final size < 1 MB**: Volume is properly cleaned

⚠️ **Size reduction < 50%**: Potential cleanup issue
⚠️ **Final size > 1 MB**: Files may not be deleted

## Test Data

- **File**: `testdata/transaction_items.csv`
- **Records**: 1000
- **Distribution**: ~333 records per semester
- **Schema**: `transaction_id,item_id,quantity,unit_price,subtotal,created_at`

## Cleanup

To stop and remove all containers and volumes:

```bash
docker compose -f docker-compose-groupby-test.yaml down -v
```

## Troubleshooting

### Volume size keeps growing
- Check orchestrator logs: `docker logs groupby-orchestrator-test`
- Verify that orchestrator receives "last chunk" signal
- Check that DeleteFile and DeleteClientDirectory are being called

### No volume size changes
- Check worker logs: `docker logs groupby-worker-1-test`
- Verify workers are receiving and processing chunks
- Check that files are being written to `/app/groupby-data/q2/<clientID>/`

### Test hangs or times out
- Increase timeout in test: change `time.Sleep(60 * time.Second)` to a longer duration
- Check RabbitMQ management UI: http://localhost:15673 (admin/password)
- Verify all services are running: `docker ps`
