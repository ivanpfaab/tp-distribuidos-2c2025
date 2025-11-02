package batch

import (
	"fmt"
	"strings"
	"sync"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
)

// ClientState holds the state for a specific client's batch processing
type ClientState struct {
	Chunks []*chunk.Chunk
	mutex  sync.RWMutex
}

// Manager manages client states for batch processing
type Manager struct {
	clientStates map[string]*ClientState
	mutex        sync.RWMutex
}

// NewManager creates a new batch manager
func NewManager() *Manager {
	return &Manager{
		clientStates: make(map[string]*ClientState),
	}
}

// GetOrCreateClientState gets or creates client state for batching
func (bm *Manager) GetOrCreateClientState(clientID string) *ClientState {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	if bm.clientStates[clientID] == nil {
		bm.clientStates[clientID] = &ClientState{
			Chunks: make([]*chunk.Chunk, 0),
		}
	}
	return bm.clientStates[clientID]
}

// AddChunk adds a chunk to a client's batch
func (bm *Manager) AddChunk(clientID string, chunkMsg *chunk.Chunk) {
	clientState := bm.GetOrCreateClientState(clientID)

	clientState.mutex.Lock()
	defer clientState.mutex.Unlock()
	clientState.Chunks = append(clientState.Chunks, chunkMsg)
}

// GetChunks gets all chunks for a client
func (bm *Manager) GetChunks(clientID string) []*chunk.Chunk {
	bm.mutex.RLock()
	clientState, exists := bm.clientStates[clientID]
	bm.mutex.RUnlock()

	if !exists {
		return nil
	}

	clientState.mutex.RLock()
	defer clientState.mutex.RUnlock()
	return clientState.Chunks
}

// ClearClient removes a client's state
func (bm *Manager) ClearClient(clientID string) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()
	delete(bm.clientStates, clientID)
}

// CombineChunks combines all chunks for a client into a single chunk
func (bm *Manager) CombineChunks(clientID string) (*chunk.Chunk, error) {
	chunks := bm.GetChunks(clientID)
	if len(chunks) == 0 {
		return nil, fmt.Errorf("no chunks found for client %s", clientID)
	}

	// Use metadata from the first chunk
	firstChunk := chunks[0]

	// Combine all chunk data
	var combinedData strings.Builder
	var totalChunkSize int

	for _, chunkMsg := range chunks {
		combinedData.WriteString(chunkMsg.ChunkData)
		totalChunkSize += chunkMsg.ChunkSize
	}

	// Create a single combined chunk
	combinedChunk := chunk.NewChunk(
		clientID,
		firstChunk.FileID,
		firstChunk.QueryType,
		1,    // ChunkNumber - single chunk
		true, // IsLastChunk
		true, // IsLastFromTable
		totalChunkSize,
		firstChunk.TableID,
		combinedData.String(),
	)

	return combinedChunk, nil
}
