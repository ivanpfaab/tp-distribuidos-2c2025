package main

import (
	"log"
	"time"
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
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
	log.Printf("Test Runner: Sending %d chunks", numChunks)

	for i := 1; i <= numChunks; i++ {
		chunkData := fmt.Sprintf("Hello this is the chunk number %d", i)
		// Create test chunk
		testChunk := chunk.NewChunk(
			"TEST",       // ClientID (4 bytes)
			"FT01",       // FileID (4 bytes)
			1,            // QueryType
			i,            // ChunkNumber
			false,        // IsLastChunk
			false,        // IsLastFromTable
			len(chunkData), // ChunkSize
			0,            // TableID
			chunkData,      // ChunkData
		)

		// Serialize and send
		chunkMessage := chunk.NewChunkMessage(testChunk)
		serialized, err := chunk.SerializeChunkMessage(chunkMessage)
		if err != nil {
			log.Fatalf("Failed to serialize chunk %d: %v", i, err)
		}

		if err := producer.Send(serialized); err != 0 {
			log.Fatalf("Failed to send chunk %d: %v", i, err)
		}

		if i%10 == 0 {
			log.Printf("Test Runner: Sent %d chunks", i)
		}
	}

	log.Printf("Test Runner: All %d chunks sent", numChunks)

	log.Println("Test Runner: Chunk sent, monitoring...")

	// Keep running to monitor
	time.Sleep(30 * time.Second)

	log.Println("Test Runner: Done")
}
