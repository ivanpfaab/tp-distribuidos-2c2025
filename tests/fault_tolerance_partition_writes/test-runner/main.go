package main

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

var names = []string{
	"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
	"Iris", "Jack", "Kate", "Liam", "Mia", "Noah", "Olivia", "Paul",
	"Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xavier",
	"Yara", "Zoe",
}

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

	// Generate and send chunks
	log.Printf("Test Runner: Sending %d chunks", config.NumChunks)

	userIDCounter := 1
	for i := 1; i <= config.NumChunks; i++ {
		// Generate CSV data (10-20 lines per chunk)
		numLines := 10 + rand.Intn(11) // 10-20 lines
		var csvLines []string
		csvLines = append(csvLines, "user_id,name") // Header

		for j := 0; j < numLines; j++ {
			userID := userIDCounter
			name := names[rand.Intn(len(names))]
			csvLines = append(csvLines, fmt.Sprintf("%d,%s", userID, name))
			userIDCounter++
		}

		csvData := strings.Join(csvLines, "\n") + "\n"

		// Create chunk
		testChunk := chunk.NewChunk(
			"TEST",         // ClientID (4 bytes)
			"PT01",         // FileID (4 bytes)
			1,              // QueryType
			i,              // ChunkNumber
			i == config.NumChunks, // IsLastChunk
			false,          // IsLastFromTable
			len(csvData),   // ChunkSize
			0,              // TableID
			csvData,        // ChunkData
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

		log.Printf("Test Runner: Sent chunk %d (%d lines)", i, numLines)
		time.Sleep(5000 * time.Millisecond) // Small delay between chunks
	}

	log.Printf("Test Runner: All %d chunks sent", config.NumChunks)
	log.Println("Test Runner: Monitoring...")

	// Keep running to monitor
	time.Sleep(60 * time.Second)

	log.Println("Test Runner: Done")
}

