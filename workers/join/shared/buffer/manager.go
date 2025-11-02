package buffer

import (
	"fmt"
	"sync"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
)

// ClientBuffer holds buffered chunks and parsed records for a client
type ClientBuffer struct {
	Chunks                   []*chunk.Chunk
	Records                  []interface{}
	ChunkCounter             int
	CompletionSignalReceived bool
	ChunkDataReceived        bool
	JoinProcessed            bool
	mutex                    sync.RWMutex
}

// Manager manages client buffers with capacity limits
type Manager struct {
	buffers     map[string]*ClientBuffer
	maxClients  int
	mutex       sync.RWMutex
}

// NewManager creates a new buffer manager
func NewManager(maxClients int) *Manager {
	return &Manager{
		buffers:    make(map[string]*ClientBuffer),
		maxClients: maxClients,
	}
}

// GetOrCreateBuffer gets or creates a buffer for a client
// Returns false if buffer is full and client doesn't exist
func (bm *Manager) GetOrCreateBuffer(clientID string) (*ClientBuffer, bool) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	// Check if buffer already exists
	if buffer, exists := bm.buffers[clientID]; exists {
		return buffer, true
	}

	// Check capacity
	if len(bm.buffers) >= bm.maxClients {
		return nil, false
	}

	// Create new buffer
	buffer := &ClientBuffer{
		Chunks:                   make([]*chunk.Chunk, 0),
		Records:                  make([]interface{}, 0),
		ChunkCounter:             1,
		CompletionSignalReceived: false,
		ChunkDataReceived:        false,
		JoinProcessed:            false,
	}
	bm.buffers[clientID] = buffer
	return buffer, true
}

// GetBuffer gets a buffer for a client (read-only)
func (bm *Manager) GetBuffer(clientID string) *ClientBuffer {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	return bm.buffers[clientID]
}

// AddChunk adds a chunk to a client's buffer
func (bm *Manager) AddChunk(clientID string, chunk *chunk.Chunk) error {
	buffer := bm.GetBuffer(clientID)
	if buffer == nil {
		return fmt.Errorf("buffer not found for client %s", clientID)
	}

	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()
	buffer.Chunks = append(buffer.Chunks, chunk)
	return nil
}

// AddRecords adds records to a client's buffer
func (bm *Manager) AddRecords(clientID string, records []interface{}) error {
	buffer := bm.GetBuffer(clientID)
	if buffer == nil {
		return fmt.Errorf("buffer not found for client %s", clientID)
	}

	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()
	buffer.Records = append(buffer.Records, records...)
	return nil
}

// MarkChunkDataReceived marks chunk data as received
func (bm *Manager) MarkChunkDataReceived(clientID string) {
	buffer := bm.GetBuffer(clientID)
	if buffer == nil {
		return
	}

	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()
	buffer.ChunkDataReceived = true
}

// MarkCompletionSignalReceived marks completion signal as received
func (bm *Manager) MarkCompletionSignalReceived(clientID string) {
	buffer := bm.GetBuffer(clientID)
	if buffer == nil {
		return
	}

	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()
	buffer.CompletionSignalReceived = true
}

// IsReady checks if both conditions are met for processing
func (bm *Manager) IsReady(clientID string) bool {
	buffer := bm.GetBuffer(clientID)
	if buffer == nil {
		return false
	}

	buffer.mutex.RLock()
	defer buffer.mutex.RUnlock()
	return buffer.CompletionSignalReceived && buffer.ChunkDataReceived && !buffer.JoinProcessed
}

// MarkProcessed marks the join as processed to prevent duplicates
func (bm *Manager) MarkProcessed(clientID string) bool {
	buffer := bm.GetBuffer(clientID)
	if buffer == nil {
		return false
	}

	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()
	if buffer.JoinProcessed {
		return false // Already processed
	}
	buffer.JoinProcessed = true
	return true
}

// GetRecords gets all records for a client
func (bm *Manager) GetRecords(clientID string) []interface{} {
	buffer := bm.GetBuffer(clientID)
	if buffer == nil {
		return nil
	}

	buffer.mutex.RLock()
	defer buffer.mutex.RUnlock()
	return buffer.Records
}

// GetFirstChunkMetadata gets metadata from the first chunk
func (bm *Manager) GetFirstChunkMetadata(clientID string) *chunk.Chunk {
	buffer := bm.GetBuffer(clientID)
	if buffer == nil || len(buffer.Chunks) == 0 {
		return nil
	}

	buffer.mutex.RLock()
	defer buffer.mutex.RUnlock()
	return buffer.Chunks[0]
}

// IncrementChunkCounter increments and returns the chunk counter
func (bm *Manager) IncrementChunkCounter(clientID string) int {
	buffer := bm.GetBuffer(clientID)
	if buffer == nil {
		return 0
	}

	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()
	buffer.ChunkCounter++
	return buffer.ChunkCounter
}

// RemoveBuffer removes a buffer for a client
func (bm *Manager) RemoveBuffer(clientID string) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()
	delete(bm.buffers, clientID)
}

