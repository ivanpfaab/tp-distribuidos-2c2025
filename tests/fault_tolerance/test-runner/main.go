package main

import (
	"log"
	"time"

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

	// Create producer to send initial chunk
	producer := workerqueue.NewMessageMiddlewareQueue("queue-3-1", config.RabbitMQ)
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

	// Create test chunk
	testChunk := chunk.NewChunk(
		"TEST",       // ClientID (4 bytes)
		"FT01",       // FileID (4 bytes)
		1,            // QueryType
		1,            // ChunkNumber
		false,        // IsLastChunk
		false,        // IsLastFromTable
		len("Hello"), // ChunkSize
		0,            // TableID
		"Hello",      // ChunkData
	)

	// Serialize and send
	chunkMessage := chunk.NewChunkMessage(testChunk)
	serialized, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		log.Fatalf("Failed to serialize chunk: %v", err)
	}

	log.Println("Test Runner: Sending initial chunk")
	if err := producer.Send(serialized); err != 0 {
		log.Fatalf("Failed to send chunk: %v", err)
	}

	log.Println("Test Runner: Chunk sent, monitoring...")

	// Keep running to monitor
	time.Sleep(30 * time.Second)

	log.Println("Test Runner: Done")
}
