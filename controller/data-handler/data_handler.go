package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
	"tp-distribuidos-2c2025/protocol/chunk"
	"tp-distribuidos-2c2025/shared/middleware"
	"tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

func main() {
	// Get configuration from environment variables or use defaults
	host := getEnv("RABBITMQ_HOST", "localhost")
	port := getEnv("RABBITMQ_PORT", "5672")
	portInt, err := strconv.Atoi(port)
	if err != nil {
		fmt.Printf("Invalid port: %v\n", err)
		return
	}
	username := getEnv("RABBITMQ_USER", "admin")
	password := getEnv("RABBITMQ_PASS", "password")

	fmt.Printf("Connecting to RabbitMQ at %s:%s with user %s\n", host, port, username)

	// Create connection configuration
	config := &middleware.ConnectionConfig{
		Host:     host,
		Port:     portInt,
		Username: username,
		Password: password,
	}

	// Create Data Handler
	dataHandler := NewDataHandler(config)

	// Initialize the data handler
	if err := dataHandler.Initialize(); err != 0 {
		fmt.Printf("Failed to initialize data handler: %v\n", err)
		return
	}
	defer dataHandler.Close()

	fmt.Println("Data Handler initialized successfully!")
	fmt.Println("Ready to send chunks to query orchestrator...")

	// Send some dummy chunks
	dataHandler.SendDummyChunks()

	// Keep the data handler running
	select {}
}

// DataHandler struct
type DataHandler struct {
	// Queue producer for sending chunks to query orchestrator
	queueProducer *workerqueue.QueueMiddleware

	// Configuration
	config *middleware.ConnectionConfig
}

// NewDataHandler creates a new Data Handler instance
func NewDataHandler(config *middleware.ConnectionConfig) *DataHandler {
	return &DataHandler{
		config: config,
	}
}

// Initialize sets up the queue producer
func (dh *DataHandler) Initialize() middleware.MessageMiddlewareError {
	// Initialize queue producer for sending chunks to query orchestrator
	dh.queueProducer = workerqueue.NewMessageMiddlewareQueue(
		"query-orchestrator-queue",
		dh.config,
	)
	if dh.queueProducer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	// Declare the queue
	if err := dh.queueProducer.DeclareQueue(true, false, false, false); err != 0 {
		return err
	}

	return 0
}

// SendChunk sends a chunk message to the query orchestrator
func (dh *DataHandler) SendChunk(chunkMsg *chunk.ChunkMessage) middleware.MessageMiddlewareError {
	// Serialize the chunk message
	messageData, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
		fmt.Printf("Failed to serialize chunk message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send to queue
	return dh.queueProducer.Send(messageData)
}

// SendDummyChunks sends some dummy chunks for testing
func (dh *DataHandler) SendDummyChunks() {
	fmt.Println("Sending dummy chunks...")

	// Create some dummy chunks with different query types and steps
	dummyChunks := []struct {
		clientID    string
		queryType   uint8
		tableID     int
		chunkNumber int
		step        int
		chunkData   string
		isLastChunk bool
	}{
		{"CLI1", 1, 1, 1, 1, "dummy data for query 1 step 1", false},
		{"CLI1", 1, 1, 2, 2, "dummy data for query 1 step 2", false},
		{"CLI1", 1, 1, 3, 3, "dummy data for query 1 step 3", true},
		{"CLI2", 2, 2, 1, 1, "dummy data for query 2 step 1", false},
		{"CLI2", 2, 2, 2, 2, "dummy data for query 2 step 2", false},
		{"CLI2", 2, 2, 3, 3, "dummy data for query 2 step 3", false},
		{"CLI2", 2, 2, 4, 4, "dummy data for query 2 step 4", false},
		{"CLI2", 2, 2, 5, 5, "dummy data for query 2 step 5", true},
	}

	for i, chunkData := range dummyChunks {
		// Create chunk
		chunkObj := chunk.NewChunk(
			chunkData.clientID,
			chunkData.queryType,
			chunkData.chunkNumber,
			chunkData.isLastChunk,
			chunkData.step,
			len(chunkData.chunkData),
			chunkData.tableID,
			chunkData.chunkData,
		)

		// Create chunk message
		chunkMsg := chunk.NewChunkMessage(chunkObj)

		// Send chunk
		if err := dh.SendChunk(chunkMsg); err != 0 {
			fmt.Printf("Failed to send chunk %d: %v\n", i+1, err)
		} else {
			fmt.Printf("Sent chunk %d: QueryType=%d, Step=%d, ChunkNumber=%d\n",
				i+1, chunkData.queryType, chunkData.step, chunkData.chunkNumber)
		}

		// Small delay between chunks
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("Finished sending dummy chunks!")
}

// Close closes all connections
func (dh *DataHandler) Close() middleware.MessageMiddlewareError {
	if dh.queueProducer != nil {
		return dh.queueProducer.Close()
	}
	return 0
}

// Helper function to get environment variable with default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
