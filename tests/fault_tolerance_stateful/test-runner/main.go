package main

import (
	"log"
	"strconv"
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

	// Create producer
	producer := workerqueue.NewMessageMiddlewareQueue(config.OutputQueue, config.RabbitMQ)
	if producer == nil {
		log.Fatal("Failed to create producer")
	}
	defer producer.Close()

	// Declare queue
	if err := producer.DeclareQueue(false, false, false, false); err != 0 {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	// Wait for workers to be ready
	log.Println("Test Runner: Waiting for workers to be ready...")
	time.Sleep(3 * time.Second)

	// Send chunks for multiple clients
	clients := []string{"CLI1", "CLI2", "CLI3", "CLI4", "CLI5", "CLI6", "CLI7", "CLI8", "CLI9", "CLI0"}
	chunksPerClient := config.NumChunks

	// Send one chunk for each client at a time
	for i := 1; i <= chunksPerClient; i++ {

		for j, clientID := range clients {
			log.Printf("Test Runner: Sending %d chunks for client %s", chunksPerClient, clientID)
			// Create chunk with value (chunk number * 10 for easy verification)
			value := j * 10
			chunkData := strconv.Itoa(value)

			testChunk := chunk.NewChunk(
				clientID,
				"FT01",
				1,              // QueryType
				i,              // ChunkNumber
				i == chunksPerClient, // IsLastChunk
				false,          // IsLastFromTable
				len(chunkData),
				0,
				chunkData,
			)

			// Serialize
			chunkMessage := chunk.NewChunkMessage(testChunk)
			serialized, err := chunk.SerializeChunkMessage(chunkMessage)
			if err != nil {
				log.Fatalf("Failed to serialize chunk %d: %v", i, err)
			}

			// Send chunk
			if err := producer.Send(serialized); err != 0 {
				log.Fatalf("Failed to send chunk %d: %v", i, err)
			}

			log.Printf("Test Runner: Sent chunk %d for client %s (value: %d)", i, clientID, value)
			time.Sleep(3000 * time.Millisecond) // Small delay between chunks
		}
	}

	log.Printf("Test Runner: All chunks sent for %d clients", len(clients))
	log.Println("Test Runner: Monitoring...")

	// Keep running to monitor
	time.Sleep(60 * time.Second)

	log.Println("Test Runner: Done")
}

