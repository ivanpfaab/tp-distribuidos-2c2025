# Query Finalization Test Suite

This test suite validates the **query finalization protocol** for **N-to-M synchronization** in the distributed system. It ensures that reduce workers properly wait for completion signals from **ALL** map workers before finalizing results for each client.

## Problem Solved

Previously, reduce workers would finalize immediately upon receiving **any** completion signal from **any** map worker. This caused issues when multiple map workers existed, as reduce workers would finalize before all map workers had completed their processing.

## Solution Implemented

- **Reduce workers** now track completion signals from **each map worker** individually
- **Reduce workers** wait for completion signals from **ALL expected map workers** before finalizing
- **Map workers** include their ID in completion signals for proper tracking
- **Environment variables** configure the expected number of map workers per query (`QUERY2_EXPECTED_MAP_WORKERS`, `QUERY3_EXPECTED_MAP_WORKERS`, etc.)
- **Comprehensive logging** provides visibility into the finalization process

## Test Scenarios

### 1. 1 Map → 1 Reduce (`1map-1reduce`)
- **Map Workers**: 1 container
- **Reduce Workers**: 1 container  
- **Clients**: CLI1, CLI2, CLI3
- **Test**: Verify single reduce worker receives completion signal from single map worker

### 2. 1 Map → Many Reduce (`1map-manyreduce`)
- **Map Workers**: 1 container
- **Reduce Workers**: 3 containers (S1-2024, S2-2024, S1-2025)
- **Clients**: CLI1, CLI2, CLI3
- **Test**: Verify all 3 reduce workers receive completion signal from single map worker

### 3. Many Map → 1 Reduce (`manymap-1reduce`)
- **Map Workers**: 3 containers
- **Reduce Workers**: 1 container
- **Clients**: CLI1, CLI2, CLI3
- **Test**: Verify single reduce worker receives completion signals from all 3 map workers

### 4. Many Map → Many Reduce (`manymap-manyreduce`)
- **Map Workers**: 3 containers
- **Reduce Workers**: 3 containers
- **Clients**: CLI1, CLI2, CLI3
- **Test**: Verify all reduce workers receive completion signals from all map workers

## Usage

### Run All Tests
```bash
cd tests/query_finalization
./test_scripts/run_all_tests.sh
```

### Run Specific Scenario
```bash
cd tests/query_finalization
./test_scripts/run_test.sh <scenario> [clients]

# Examples:
./test_scripts/run_test.sh 1map-1reduce
./test_scripts/run_test.sh manymap-manyreduce CLI1,CLI2,CLI3
```

### Verify Results
```bash
cd tests/query_finalization
./test_scripts/verify_finalization.sh <scenario> [clients]
```

## Expected Behavior

### Successful Test Output
```
✅ Client CLI1: 3/3 map workers completed
✅ Client CLI1: Finalization completed
✅ Client CLI2: 3/3 map workers completed  
✅ Client CLI2: Finalization completed
✅ Client CLI3: 3/3 map workers completed
✅ Client CLI3: Finalization completed
```

### Key Log Messages
- `"Using expected map workers from environment: QUERY3_EXPECTED_MAP_WORKERS=3"`
- `"Initialized data for client: CLI1 (expecting 3 map workers)"`
- `"Map worker query3-map-worker-1 completed for client CLI1 (1/3 map workers completed)"`
- `"All 3 map workers completed for client CLI1, finalizing results..."`
- `"Reduce worker for semester S1-2024 completed processing for client CLI1"`

## Test Data

The test uses small, controlled datasets:
- **CLI1**: 5 transactions from S1-2024
- **CLI2**: 5 transactions from S2-2024  
- **CLI3**: 5 transactions from S1-2025

## Verification Points

Each test verifies:
1. **Completion Signal Delivery**: All reduce workers receive completion signals for all clients
2. **Map Worker Tracking**: Reduce workers correctly track which map workers have completed
3. **Finalization Order**: Clients are finalized only after all map workers complete
4. **Resource Cleanup**: Client data is properly cleared after finalization
5. **No Race Conditions**: Proper synchronization between map and reduce workers

## Debugging

If tests fail, check:
1. **Container logs** for error messages
2. **Completion signal counts** - should match expected map workers
3. **Finalization timing** - should happen after all map workers complete
4. **Resource cleanup** - memory should be freed after finalization

## Architecture

```
Test Environment:
├── Query Gateway (1) - Sends test chunks
├── Map Workers (1-N) - Process chunks and send completion signals  
├── Orchestrator (1) - Coordinates finalization
├── Reduce Workers (1-M) - Receive completion signals and finalize
└── Test Clients (3) - CLI1, CLI2, CLI3 with test data
```

This test suite ensures the **query finalization protocol** works correctly for **N-to-M synchronization**, providing confidence that the distributed system properly handles multiple map and reduce workers.
