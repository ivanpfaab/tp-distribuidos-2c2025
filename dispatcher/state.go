package main

import (
	"strconv"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	statefulworker "github.com/tp-distribuidos-2c2025/shared/stateful_worker"
)

// DispatcherClientState implements ClientState interface for results dispatcher
type DispatcherClientState struct {
	ClientID            string
	Query1Completed     bool
	Query2Completed     bool
	Query3Completed     bool
	Query4Completed     bool
	AllQueriesCompleted bool
	worker              *ResultsDispatcherWorker // Reference to worker for accessing CompletionTrackers
}

// GetClientID returns the client ID
func (d *DispatcherClientState) GetClientID() string {
	return d.ClientID
}

// IsReady returns whether all queries are completed
func (d *DispatcherClientState) IsReady() bool {
	return d.AllQueriesCompleted
}

// UpdateState updates state from a CSV metadata row and replays to CompletionTracker
// CSV format: msg_id,client_id,query_type,file_id,table_id,chunk_number,is_last_chunk,is_last_from_table
func (d *DispatcherClientState) UpdateState(csvRow []string) error {
	if len(csvRow) < 8 || d.worker == nil {
		return nil
	}

	// Parse query_type
	queryType, err := strconv.Atoi(csvRow[2])
	if err != nil {
		return nil
	}

	// Parse fields from CSV row
	fileID := csvRow[3]
	tableID, _ := strconv.Atoi(csvRow[4])
	chunkNumber, _ := strconv.Atoi(csvRow[5])
	isLastChunk := csvRow[6] == "1"
	isLastFromTable := csvRow[7] == "1"

	// Create chunk notification
	notification := signals.NewChunkNotification(
		d.ClientID,
		fileID,
		"query-results",
		tableID,
		chunkNumber,
		isLastChunk,
		isLastFromTable,
	)

	// Replay to appropriate CompletionTracker based on query type
	switch queryType {
	case QueryType1:
		d.worker.query1Tracker.ProcessChunkNotification(notification)
	case QueryType2:
		d.worker.query2Tracker.ProcessChunkNotification(notification)
	case QueryType3:
		d.worker.query3Tracker.ProcessChunkNotification(notification)
	case QueryType4:
		d.worker.query4Tracker.ProcessChunkNotification(notification)
	}

	return nil
}

// buildDispatcherStatus creates a status builder function for StatefulWorkerManager
func buildDispatcherStatus(worker *ResultsDispatcherWorker) statefulworker.StatusBuilderFunc {
	return func(clientID string, csvRows [][]string) (statefulworker.ClientState, error) {
		state := &DispatcherClientState{
			ClientID:            clientID,
			Query1Completed:     false,
			Query2Completed:     false,
			Query3Completed:     false,
			Query4Completed:     false,
			AllQueriesCompleted: false,
			worker:              worker,
		}

		// Update state from CSV rows (this will replay to CompletionTrackers)
		for _, row := range csvRows {
			if err := state.UpdateState(row); err != nil {
				continue
			}
		}

		return state, nil
	}
}

// extractDispatcherMetadataRow extracts CSV row data from a chunk message
// Returns: [msg_id, client_id, query_type, file_id, table_id, chunk_number, is_last_chunk, is_last_from_table]
func extractDispatcherMetadataRow(message interface{}) []string {
	chunkMsg, ok := message.(*chunk.Chunk)
	if !ok {
		return nil
	}

	msgID := chunkMsg.ID
	clientID := chunkMsg.ClientID
	queryType := strconv.Itoa(int(chunkMsg.QueryType))
	fileID := chunkMsg.FileID
	tableID := strconv.Itoa(chunkMsg.TableID)
	chunkNumber := strconv.Itoa(int(chunkMsg.ChunkNumber))
	isLastChunk := "0"
	if chunkMsg.IsLastChunk {
		isLastChunk = "1"
	}
	isLastFromTable := "0"
	if chunkMsg.IsLastFromTable {
		isLastFromTable = "1"
	}

	return []string{msgID, clientID, queryType, fileID, tableID, chunkNumber, isLastChunk, isLastFromTable}
}
