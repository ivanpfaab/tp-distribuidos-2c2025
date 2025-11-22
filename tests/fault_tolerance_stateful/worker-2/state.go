package main

import (
	"strconv"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	statefulworker "github.com/tp-distribuidos-2c2025/shared/stateful_worker"
)

// TestClientState represents the state for a test client
type TestClientState struct {
	ClientID       string
	ReceivedChunks map[int]bool
	AggregatedSum  int
	ExpectedChunks int
	ready          bool
}

// GetClientID returns the client ID
func (t *TestClientState) GetClientID() string {
	return t.ClientID
}

// IsReady returns whether the client is ready
func (t *TestClientState) IsReady() bool {
	return t.ready
}

// UpdateFromMessage updates state from a message
func (t *TestClientState) UpdateFromMessage(message interface{}) error {
	chunkMsg, ok := message.(*chunk.Chunk)
	if !ok {
		return nil
	}

	chunkNum := int(chunkMsg.ChunkNumber)

	// Parse value from chunk data
	valueStr := strings.TrimSpace(chunkMsg.ChunkData)
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		// If not a number, skip
		return nil
	}

	t.ReceivedChunks[chunkNum] = true
	t.AggregatedSum += value

	// Check if ready
	if len(t.ReceivedChunks) >= t.ExpectedChunks {
		t.ready = true
	}

	return nil
}

// buildTestStatus creates a status builder function that uses the expected chunks
func buildTestStatus(expectedChunks int) statefulworker.StatusBuilderFunc {
	return func(clientID string, csvRows [][]string) (statefulworker.ClientState, error) {
		state := &TestClientState{
			ClientID:       clientID,
			ReceivedChunks: make(map[int]bool),
			AggregatedSum:  0,
			ExpectedChunks: expectedChunks,
			ready:          false,
		}

		// CSV format: [msg_id, chunk_number, value]
		for _, row := range csvRows {
			if len(row) < 3 {
				continue
			}

			chunkNum, err := strconv.Atoi(row[1])
			if err != nil {
				continue
			}

			value, err := strconv.Atoi(row[2])
			if err != nil {
				continue
			}

			state.ReceivedChunks[chunkNum] = true
			state.AggregatedSum += value
		}

		// Check if ready
		if len(state.ReceivedChunks) >= state.ExpectedChunks {
			state.ready = true
		}

		return state, nil
	}
}

// extractTestMetadataRow extracts CSV row data from a chunk message
func extractTestMetadataRow(message interface{}) []string {
	chunkMsg, ok := message.(*chunk.Chunk)
	if !ok {
		return nil
	}

	msgID := chunkMsg.ID
	chunkNum := strconv.Itoa(int(chunkMsg.ChunkNumber))
	value := strings.TrimSpace(chunkMsg.ChunkData)

	return []string{msgID, chunkNum, value}
}
