package main

import (
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// In-memory storage for chunks (for now)
type ChunkStorage struct {
	chunks map[string][]*chunk.Chunk // key: clientID_fileID, value: slice of chunks
	mutex  sync.RWMutex
}

// Global storage instance
var storage = &ChunkStorage{
	chunks: make(map[string][]*chunk.Chunk),
}

// processMessage processes a single chunk message
func (dww *DataWriterWorker) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	fmt.Printf("Data Writer: Received chunk message\n")

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("Data Writer: Failed to deserialize chunk message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Process the chunk (store it)
	fmt.Printf("Data Writer: Processing chunk - QueryType: %d, Step: %d, ClientID: %s, FileID: %s, ChunkNumber: %d, IsLastChunk: %t\n",
		chunkMsg.QueryType, chunkMsg.Step, chunkMsg.ClientID, chunkMsg.FileID, chunkMsg.ChunkNumber, chunkMsg.IsLastChunk)

	// Store the chunk
	if err := dww.storeChunk(chunkMsg); err != 0 {
		fmt.Printf("Data Writer: Failed to store chunk: %v\n", err)
		return err
	}

	fmt.Printf("Data Writer: Successfully stored chunk for ClientID: %s, FileID: %s, ChunkNumber: %d\n",
		chunkMsg.ClientID, chunkMsg.FileID, chunkMsg.ChunkNumber)

	return 0
}

// storeChunk stores a chunk in memory
func (dww *DataWriterWorker) storeChunk(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Create a unique key for this client-file combination
	key := fmt.Sprintf("%s_%s", chunkMsg.ClientID, chunkMsg.FileID)

	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	// Initialize slice if it doesn't exist
	if storage.chunks[key] == nil {
		storage.chunks[key] = make([]*chunk.Chunk, 0)
	}

	// Add the chunk to the storage
	storage.chunks[key] = append(storage.chunks[key], chunkMsg)

	totalChunks := len(storage.chunks[key])
	fmt.Printf("Data Writer: Stored chunk %d for %s (total chunks for this file: %d)\n",
		chunkMsg.ChunkNumber, key, totalChunks)

	// Check if this is the last chunk based on the IsLastChunk flag
	if chunkMsg.IsLastChunk {
		fmt.Printf("Data Writer: Received last chunk (IsLastChunk=true) for %s. Total chunks: %d\n", key, totalChunks)
		// TODO: In the future, this could trigger CSV writing or other processing
	} else {
		fmt.Printf("Data Writer: Chunk %d for %s is not marked as last chunk (IsLastChunk=false)\n",
			chunkMsg.ChunkNumber, key)
	}

	return 0
}

// GetStoredChunks returns all stored chunks for a given client-file combination
func GetStoredChunks(clientID, fileID string) []*chunk.Chunk {
	key := fmt.Sprintf("%s_%s", clientID, fileID)

	storage.mutex.RLock()
	defer storage.mutex.RUnlock()

	if chunks, exists := storage.chunks[key]; exists {
		// Return a copy to avoid race conditions
		result := make([]*chunk.Chunk, len(chunks))
		copy(result, chunks)
		return result
	}

	return nil
}

// GetStorageStats returns statistics about the stored data
func GetStorageStats() map[string]int {
	storage.mutex.RLock()
	defer storage.mutex.RUnlock()

	stats := make(map[string]int)
	for key, chunks := range storage.chunks {
		stats[key] = len(chunks)
	}

	return stats
}
