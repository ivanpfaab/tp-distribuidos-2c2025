package main

import (
	"fmt"
	"log"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/tests/fault_tolerance_vanilla/utils"
)

func main() {
	config := LoadConfig()

	log.Println("Test Runner: Starting")

	// Wait for RabbitMQ to be ready
	log.Println("Test Runner: Waiting for RabbitMQ to be ready...")
	if err := middleware.WaitForConnection(config.RabbitMQ, 30, 1*time.Second); err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	log.Println("Test Runner: RabbitMQ is ready")

	// Create producer to send initial chunk (sends to Worker 1's input queue)
	producer := workerqueue.NewMessageMiddlewareQueue("queue-1-2", config.RabbitMQ)
	if producer == nil {
		log.Fatal("Failed to create producer")
	}
	defer producer.Close()

	// Declare queue (non-durable, non-auto-delete)
	if err := producer.DeclareQueue(false, false, false, false); err != 0 {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	// Wait a bit for workers to be ready
	log.Println("Test Runner: Waiting for workers to be ready...")
	time.Sleep(3 * time.Second)

	// Send 100 chunks
	numChunks := 100
	log.Printf("Test Runner: Sending %d chunks (duplicate rate: %.2f%%)", numChunks, config.DuplicateRate*100)

	// Initialize duplicate sender
	duplicateSender := utils.NewDuplicateSender(config.DuplicateRate)
	duplicateCount := 0
	totalSent := 0

	for i := 1; i <= numChunks; i++ {
		var serialized []byte
		var chunkNumber int

		// Decide if we should send a duplicate
		if duplicateSender.ShouldSendDuplicate() {
			// Send a duplicate: pick a random previously sent chunk
			duplicateID, duplicateData, found := duplicateSender.GetRandomDuplicate()
			if found {
				serialized = duplicateData
				// Extract chunk number from the duplicate for logging
				dupChunk, err := chunk.DeserializeChunk(duplicateData)
				if err == nil {
					chunkNumber = dupChunk.ChunkNumber
				}
				duplicateCount++
				log.Printf("Test Runner: Sending DUPLICATE chunk (ID: %s, ChunkNumber: %d)", duplicateID, chunkNumber)
			}
		}

		chunkData := fmt.Sprintf("Hello this is the chunk number %d", i)
		// Create test chunk
		testChunk := chunk.NewChunk(
			"TEST",         // ClientID (4 bytes)
			"FT01",         // FileID (4 bytes)
			1,              // QueryType
			i,              // ChunkNumber
			false,          // IsLastChunk
			false,          // IsLastFromTable
			len(chunkData), // ChunkSize
			0,              // TableID
			chunkData,      // ChunkData
		)
		// Serialize
		chunkMessage := chunk.NewChunkMessage(testChunk)
		var err error
		serialized, err = chunk.SerializeChunkMessage(chunkMessage)
		if err != nil {
			log.Fatalf("Failed to serialize chunk %d: %v", i, err)
		}

		// Store for potential future duplicates
		duplicateSender.StoreChunk(testChunk, serialized)
		chunkNumber = i

		// Send the chunk
		if err := producer.Send(serialized); err != 0 {
			log.Fatalf("Failed to send chunk %d: %v", chunkNumber, err)
		}

		totalSent++
	}

	log.Printf("Test Runner: All chunks sent - Unique: %d, Duplicates: %d, Total: %d", numChunks, duplicateCount, totalSent)

	log.Println("Test Runner: Chunk sent, monitoring...")

	// Keep running to monitor
	time.Sleep(30 * time.Second)

	log.Println("Test Runner: Done")
}
