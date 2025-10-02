package main

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// TestDistributedGroupByFlow tests the entire distributed group by flow
func TestDistributedGroupByFlow(t *testing.T) {
	fmt.Println("=== Starting Distributed Group By Flow Test ===")

	// Test configuration
	numWorkers := 3
	numChunks := 5

	// Create test data
	testChunks := createTestChunks(numChunks)

	// Create mock config for testing
	config := &middleware.ConnectionConfig{
		Host:     "localhost",
		Port:     5672,
		Username: "guest",
		Password: "guest",
	}

	// Create the distributed orchestrator
	orchestrator, err := NewGroupByOrchestrator(config, numWorkers)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}

	// Start the orchestrator
	orchestrator.Start()
	defer orchestrator.Stop()

	// Wait a bit for all components to start
	time.Sleep(100 * time.Millisecond)

	// Channel to collect results
	results := make(chan *chunk.Chunk, numChunks)

	// Start a goroutine to collect results
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for result := range orchestrator.GetResultChannel() {
			fmt.Printf("🎯 FINAL RESULT RECEIVED - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d\n",
				result.ClientID, result.QueryType, result.Step, result.ChunkNumber, len(result.ChunkData))
			results <- result
		}
	}()

	// Process all test chunks
	fmt.Printf("📤 Sending %d chunks to orchestrator...\n", len(testChunks))
	for i, chunk := range testChunks {
		fmt.Printf("📤 Sending chunk %d - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d\n",
			i+1, chunk.ClientID, chunk.QueryType, chunk.Step, chunk.ChunkNumber, len(chunk.ChunkData))
		orchestrator.ProcessChunk(chunk)
		time.Sleep(50 * time.Millisecond) // Small delay between chunks
	}

	// Wait for all results
	time.Sleep(2 * time.Second)

	// Close the orchestrator to signal completion
	orchestrator.Stop()

	// Close results channel
	close(results)

	// Wait for result collection to finish
	wg.Wait()

	// Verify results
	resultCount := len(results)
	fmt.Printf("✅ Test completed - Received %d results\n", resultCount)

	if resultCount == 0 {
		t.Error("Expected to receive at least one result, but got none")
	}

	fmt.Println("=== Distributed Group By Flow Test Completed ===")
}

// TestDistributedGroupByWithRealData tests with sample real data
func TestDistributedGroupByWithRealData(t *testing.T) {
	fmt.Println("=== Starting Real Data Distributed Group By Test ===")

	// Test configuration
	numWorkers := 4
	numChunks := 3

	// Create test data that mimics real coffee shop data
	testChunks := createRealDataTestChunks(numChunks)

	// Create mock config for testing
	config := &middleware.ConnectionConfig{
		Host:     "localhost",
		Port:     5672,
		Username: "guest",
		Password: "guest",
	}

	// Create the distributed orchestrator
	orchestrator, err := NewGroupByOrchestrator(config, numWorkers)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %v", err)
	}

	// Start the orchestrator
	orchestrator.Start()
	defer orchestrator.Stop()

	// Wait for components to start
	time.Sleep(100 * time.Millisecond)

	// Channel to collect results
	results := make(chan *chunk.Chunk, numChunks)

	// Start a goroutine to collect results
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for result := range orchestrator.GetResultChannel() {
			fmt.Printf("🎯 FINAL RESULT RECEIVED - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d\n",
				result.ClientID, result.QueryType, result.Step, result.ChunkNumber, len(result.ChunkData))
			fmt.Printf("📊 Result Data Preview: %s\n", truncateString(result.ChunkData, 100))
			results <- result
		}
	}()

	// Process all test chunks
	fmt.Printf("📤 Sending %d real data chunks to orchestrator...\n", len(testChunks))
	for i, chunk := range testChunks {
		fmt.Printf("📤 Sending chunk %d - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d\n",
			i+1, chunk.ClientID, chunk.QueryType, chunk.Step, chunk.ChunkNumber, len(chunk.ChunkData))
		orchestrator.ProcessChunk(chunk)
		time.Sleep(100 * time.Millisecond) // Small delay between chunks
	}

	// Wait for all results
	time.Sleep(3 * time.Second)

	// Close the orchestrator to signal completion
	orchestrator.Stop()

	// Close results channel
	close(results)

	// Wait for result collection to finish
	wg.Wait()

	// Verify results
	resultCount := len(results)
	fmt.Printf("✅ Real Data Test completed - Received %d results\n", resultCount)

	if resultCount == 0 {
		t.Error("Expected to receive at least one result, but got none")
	}

	fmt.Println("=== Real Data Distributed Group By Test Completed ===")
}

// createTestChunks creates mock test chunks
func createTestChunks(count int) []*chunk.Chunk {
	chunks := make([]*chunk.Chunk, count)

	for i := 0; i < count; i++ {
		// Create mock data with multiple lines
		lines := make([]string, 15) // More than 10 lines to test filtering
		for j := 0; j < 15; j++ {
			lines[j] = fmt.Sprintf("Line %d from chunk %d - Data: sample_data_%d_%d", j+1, i+1, i+1, j+1)
		}

		chunkData := strings.Join(lines, "\n")

		chunks[i] = &chunk.Chunk{
			ClientID:    fmt.Sprintf("CLIENT_%d", i+1),
			QueryType:   1,
			TableID:     1,
			ChunkSize:   len(chunkData),
			ChunkNumber: i + 1,
			IsLastChunk: i == count-1,
			Step:        1,
			ChunkData:   chunkData,
		}
	}

	return chunks
}

// createRealDataTestChunks creates test chunks with coffee shop-like data
func createRealDataTestChunks(count int) []*chunk.Chunk {
	chunks := make([]*chunk.Chunk, count)

	// Sample coffee shop transaction data
	coffeeShopData := []string{
		"transaction_id,store_id,product_id,quantity,price,timestamp",
		"TXN001,STORE_001,COFFEE_001,2,4.50,2024-01-01 08:30:00",
		"TXN002,STORE_001,COFFEE_002,1,3.25,2024-01-01 09:15:00",
		"TXN003,STORE_002,COFFEE_001,3,4.50,2024-01-01 10:00:00",
		"TXN004,STORE_001,COFFEE_003,1,5.75,2024-01-01 11:30:00",
		"TXN005,STORE_003,COFFEE_002,2,3.25,2024-01-01 12:00:00",
		"TXN006,STORE_002,COFFEE_001,1,4.50,2024-01-01 13:45:00",
		"TXN007,STORE_001,COFFEE_004,1,6.00,2024-01-01 14:20:00",
		"TXN008,STORE_003,COFFEE_003,2,5.75,2024-01-01 15:10:00",
		"TXN009,STORE_002,COFFEE_002,1,3.25,2024-01-01 16:00:00",
		"TXN010,STORE_001,COFFEE_001,2,4.50,2024-01-01 17:30:00",
		"TXN011,STORE_003,COFFEE_004,1,6.00,2024-01-01 18:15:00",
		"TXN012,STORE_002,COFFEE_003,1,5.75,2024-01-01 19:00:00",
		"TXN013,STORE_001,COFFEE_002,3,3.25,2024-01-01 20:30:00",
		"TXN014,STORE_003,COFFEE_001,1,4.50,2024-01-01 21:00:00",
		"TXN015,STORE_002,COFFEE_004,2,6.00,2024-01-01 21:45:00",
	}

	for i := 0; i < count; i++ {
		// Create chunk data with coffee shop transactions
		chunkData := strings.Join(coffeeShopData, "\n")

		chunks[i] = &chunk.Chunk{
			ClientID:    "COFFEE_SHOP_CLIENT",
			QueryType:   2, // Different query type for real data test
			TableID:     2,
			ChunkSize:   len(chunkData),
			ChunkNumber: i + 1,
			IsLastChunk: i == count-1,
			Step:        1,
			ChunkData:   chunkData,
		}
	}

	return chunks
}

// truncateString truncates a string to the specified length
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
