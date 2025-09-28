package main

import (
	"fmt"
	"os"
	"strconv"
	"tp-distribuidos-2c2025/protocol/chunk"
	"tp-distribuidos-2c2025/shared/middleware"
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

	// Create Query Orchestrator
	orchestrator := NewQueryOrchestrator(config)

	// Initialize the orchestrator (creates exchanges and producers)
	if err := orchestrator.Initialize(); err != 0 {
		fmt.Printf("Failed to initialize orchestrator: %v\n", err)
		return
	}
	defer orchestrator.Close()

	fmt.Println("Query Orchestrator initialized successfully!")
	fmt.Println("Exchanges created: filter-exchange, aggregator-exchange, join-exchange, groupby-exchange")

	// Test different routing scenarios
	testCases := []struct {
		queryType uint8
		step      int
		expected  string
	}{
		{1, 1, "filter"},
		{1, 2, "aggregator"},
		{1, 3, "join"},
		{1, 4, "groupby"},
	}

	for i, testCase := range testCases {
		fmt.Printf("\n--- Test Case %d ---\n", i+1)

		// Create a chunk message
		chunkMsg := &chunk.ChunkMessage{
			QueryType:   testCase.queryType,
			Step:        testCase.step,
			ClientID:    fmt.Sprintf("c%d", i+1), // Shortened to fit 4-byte limit
			ChunkNumber: i + 1,
			IsLastChunk: i == len(testCases)-1,
			ChunkSize:   100,
			TableID:     1,
			ChunkData:   fmt.Sprintf("test data for query %d step %d", testCase.queryType, testCase.step),
		}

		fmt.Printf("Sending chunk: QueryType=%d, Step=%d (expected target: %s)\n",
			chunkMsg.QueryType, chunkMsg.Step, testCase.expected)
		fmt.Println("ChunkMsg: ", chunkMsg)

		// Process the chunk (route it to the appropriate node)
		if err := orchestrator.ProcessChunk(chunkMsg); err != 0 {
			fmt.Printf("Failed to process chunk: %v\n", err)
		} else {
			fmt.Printf("Chunk sent successfully to %s node!\n", testCase.expected)
		}
	}

	fmt.Println("\nAll test cases completed!")
	fmt.Println("Check RabbitMQ Management UI to see the messages in the exchanges.")
}

// Helper function to get environment variable with default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
