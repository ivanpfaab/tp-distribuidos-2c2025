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
	// - ST01: stores.csv (for store_id joins in query 3)
	// - MN01: menu_items.csv (for item_id joins in query 2)
	// - US01: users_*.csv (for user_id joins in query 4)
	return fileID == "ST01" || fileID == "MN01" || fileID == "US01"
}

// forwardReferenceDataToJoinWorker forwards reference data chunks directly to the join worker
// TODO: In a future version, this should be enhanced to:
// 1. Accumulate all chunks for a file before sending (for multi-chunk files)
// 2. Send the actual CSV data instead of just a notification
// 3. Handle chunk ordering and completeness validation
func (dww *DataWriterWorker) forwardReferenceDataToJoinWorker(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	fmt.Printf("Data Writer: Forwarding reference data chunk for FileID: %s to join worker\n", chunkMsg.FileID)

	// Create a simple notification message for the join worker
	// In the future, this should send the actual CSV data
	message := fmt.Sprintf("REFERENCE_DATA_READY:%s:chunk_%d", chunkMsg.FileID, chunkMsg.ChunkNumber)

	// Send the reference data notification to the join worker
	if err := dww.SendReferenceDataToJoinWorker(chunkMsg.FileID, []byte(message)); err != 0 {
		fmt.Printf("Data Writer: Failed to send reference data to join worker: %v\n", err)
		return err
	}

	fmt.Printf("Data Writer: Successfully sent reference data notification for FileID: %s\n", chunkMsg.FileID)
	return 0
}

// sendReferenceDataToJoinWorker sends all stored chunks for a reference data file to the join worker
// DEPRECATED: This function is kept for backward compatibility but should be replaced by forwardReferenceDataToJoinWorker
func (dww *DataWriterWorker) sendReferenceDataToJoinWorker(fileID, key string) middleware.MessageMiddlewareError {
	fmt.Printf("Data Writer: === ENTERING sendReferenceDataToJoinWorker ===\n")
	fmt.Printf("Data Writer: Preparing to send reference data for FileID: %s to join worker\n", fileID)
	fmt.Printf("Data Writer: Key: %s, FileID: %s\n", key, fileID)

	// Extract clientID from key (key format: "clientID_fileID")
	clientID := key[:len(key)-len(fileID)-1] // Remove "_fileID" from the end
	fmt.Printf("Data Writer: Extracted clientID: %s\n", clientID)

	// Get all stored chunks for this file
	chunks := GetStoredChunks(clientID, fileID)
	if chunks == nil {
		fmt.Printf("Data Writer: No chunks found for clientID: %s, fileID: %s\n", clientID, fileID)
		fmt.Printf("Data Writer: Available keys in storage: %v\n", GetStorageStats())
		return middleware.MessageMiddlewareMessageError
	}

	fmt.Printf("Data Writer: Found %d chunks for clientID: %s, fileID: %s\n", len(chunks), clientID, fileID)

	// For now, we'll send a simple message indicating the reference data is ready
	// In a real implementation, this would serialize and send the actual CSV data
	message := fmt.Sprintf("REFERENCE_DATA_READY:%s:%d_chunks", fileID, len(chunks))

	return dww.SendReferenceDataToJoinWorker(fileID, []byte(message))
}
