package main

import (
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"strings"
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

	// Check if this is reference data and forward it directly to join worker
	if isReferenceDataFile(chunkMsg.FileID) {
		fmt.Printf("Data Writer: FileID %s is reference data, forwarding to join worker\n", chunkMsg.FileID)
		if err := dww.forwardReferenceDataToJoinWorker(chunkMsg); err != 0 {
			fmt.Printf("Data Writer: Failed to forward reference data to join worker: %v\n", err)
			return err
		}
		fmt.Printf("Data Writer: Successfully forwarded reference data to join worker\n")
	} else {
		fmt.Printf("Data Writer: FileID %s is not reference data, storing normally\n", chunkMsg.FileID)
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

// isReferenceDataFile checks if the fileID corresponds to reference data
func isReferenceDataFile(fileID string) bool {
	// Reference data files that need to be sent to join worker:
	// - ST: stores.csv (for store_id joins in query 3)
	// - MN: menu_items.csv (for item_id joins in query 2)
	// - US: users_*.csv (for user_id joins in query 4)
	return strings.HasPrefix(fileID, "ST") || strings.HasPrefix(fileID, "MN") || strings.HasPrefix(fileID, "US")
}

// forwardReferenceDataToJoinWorker forwards reference data chunks directly to the join worker
func (dww *DataWriterWorker) forwardReferenceDataToJoinWorker(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	fmt.Printf("Data Writer: Forwarding reference data chunk for FileID: %s to join worker\n", chunkMsg.FileID)

	// Send the actual CSV data to the join worker
	// Format: "REFERENCE_DATA_CSV:fileID:csvData"
	csvData := string(chunkMsg.ChunkData)
	message := fmt.Sprintf("REFERENCE_DATA_CSV:%s:%s", chunkMsg.FileID, csvData)

	// Send the reference data CSV to the join worker
	if err := dww.SendReferenceDataToJoinWorker(chunkMsg.FileID, []byte(message)); err != 0 {
		fmt.Printf("Data Writer: Failed to send reference data CSV to join worker: %v\n", err)
		return err
	}

	fmt.Printf("Data Writer: Successfully sent reference data CSV for FileID: %s (size: %d bytes)\n", chunkMsg.FileID, len(csvData))
	return 0
}
