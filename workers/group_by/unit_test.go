package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
)

// TestSimpleDistributedGroupBy tests the distributed group by with a simpler approach
func TestSimpleDistributedGroupBy(t *testing.T) {
	fmt.Println("=== Starting Simple Distributed Group By Test ===")

	// Test configuration
	numWorkers := 2
	numChunks := 3

	// Create test data
	testChunks := createSimpleTestChunks(numChunks)

	// Create the distributed orchestrator for testing (without RabbitMQ)
	orchestrator := createTestOrchestrator(numWorkers)

	// Start the orchestrator
	orchestrator.Start()
	defer func() {
		fmt.Println("Stopping orchestrator...")
		orchestrator.Stop()
	}()

	// Wait a bit for all components to start
	time.Sleep(100 * time.Millisecond)

	// Start a goroutine to listen for results
	resultReceived := make(chan bool, 1)
	go func() {
		select {
		case result := <-orchestrator.GetResultChannel():
			fmt.Printf("\033[36m[TEST] RECEIVED FINAL RESULT - ClientID: %s, QueryType: %d, Size: %d bytes\033[0m\n",
				result.ClientID, result.QueryType, len(result.ChunkData))
			resultReceived <- true
		case <-time.After(40 * time.Second):
			fmt.Println("\033[31m[TEST] TIMEOUT - No result received within 30 seconds\033[0m")
			resultReceived <- false
		}
	}()

	// Process all test chunks
	fmt.Printf("\033[36m[TEST] Sending %d chunks to orchestrator...\033[0m\n", len(testChunks))
	for i, chunk := range testChunks {
		fmt.Printf("\033[36m[TEST] Sending chunk %d - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d\033[0m\n",
			i+1, chunk.ClientID, chunk.QueryType, chunk.Step, chunk.ChunkNumber, len(chunk.ChunkData))
		orchestrator.ProcessChunk(chunk)
		time.Sleep(50 * time.Millisecond) // Small delay between chunks
	}

	// Signal that all chunks have been sent
	orchestrator.FinishProcessing()

	// Wait for result
	success := <-resultReceived
	if !success {
		t.Error("Test failed - no result received or timeout occurred")
	}

	fmt.Println("\033[36m[TEST] Simple Distributed Group By Test Completed\033[0m")
}

// TestDistributedGroupByQueryType2 tests the distributed group by with QueryType 2 (transaction_items)
func TestDistributedGroupByQueryType2(t *testing.T) {
	fmt.Println("=== Starting Distributed Group By Test - QueryType 2 ===")

	// Test configuration
	numWorkers := 2
	numChunks := 3

	// Create test data for QueryType 2 (transaction_items)
	testChunks := createQueryType2TestChunks(numChunks)

	// Create the distributed orchestrator for testing (without RabbitMQ)
	orchestrator := createTestOrchestrator(numWorkers)

	// Start the orchestrator
	orchestrator.Start()
	defer func() {
		fmt.Println("Stopping orchestrator...")
		orchestrator.Stop()
	}()

	// Wait a bit for all components to start
	time.Sleep(100 * time.Millisecond)

	// Start a goroutine to listen for results
	resultReceived := make(chan bool, 1)
	go func() {
		select {
		case result := <-orchestrator.GetResultChannel():
			fmt.Printf("\033[36m[TEST] RECEIVED FINAL RESULT - ClientID: %s, QueryType: %d, Size: %d bytes\033[0m\n",
				result.ClientID, result.QueryType, len(result.ChunkData))
			fmt.Printf("\033[36m[TEST] FINAL RESULT DATA:\n%s\033[0m\n", result.ChunkData)
			resultReceived <- true
		case <-time.After(40 * time.Second):
			fmt.Println("\033[31m[TEST] TIMEOUT - No result received within 40 seconds\033[0m")
			resultReceived <- false
		}
	}()

	// Process all test chunks
	fmt.Printf("\033[36m[TEST] Sending %d chunks to orchestrator...\033[0m\n", len(testChunks))
	for i, chunk := range testChunks {
		fmt.Printf("\033[36m[TEST] Sending chunk %d - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d\033[0m\n",
			i+1, chunk.ClientID, chunk.QueryType, chunk.Step, chunk.ChunkNumber, len(chunk.ChunkData))
		orchestrator.ProcessChunk(chunk)
		time.Sleep(50 * time.Millisecond) // Small delay between chunks
	}

	// Signal that all chunks have been sent
	orchestrator.FinishProcessing()

	// Wait for result
	success := <-resultReceived
	if !success {
		t.Error("Test failed - no result received or timeout occurred")
	}

	fmt.Println("\033[36m[TEST] Distributed Group By Test - QueryType 2 Completed\033[0m")
}

// TestDistributedGroupByQueryType3 tests the distributed group by with QueryType 3 (transactions)
func TestDistributedGroupByQueryType3(t *testing.T) {
	fmt.Println("=== Starting Distributed Group By Test - QueryType 3 ===")

	// Test configuration
	numWorkers := 2
	numChunks := 3

	// Create test data for QueryType 3 (transactions)
	testChunks := createQueryType3TestChunks(numChunks)

	// Create the distributed orchestrator for testing (without RabbitMQ)
	orchestrator := createTestOrchestrator(numWorkers)

	// Start the orchestrator
	orchestrator.Start()
	defer func() {
		fmt.Println("Stopping orchestrator...")
		orchestrator.Stop()
	}()

	// Wait a bit for all components to start
	time.Sleep(100 * time.Millisecond)

	// Start a goroutine to listen for results
	resultReceived := make(chan bool, 1)
	go func() {
		select {
		case result := <-orchestrator.GetResultChannel():
			fmt.Printf("\033[36m[TEST] RECEIVED FINAL RESULT - ClientID: %s, QueryType: %d, Size: %d bytes\033[0m\n",
				result.ClientID, result.QueryType, len(result.ChunkData))
			fmt.Printf("\033[36m[TEST] FINAL RESULT DATA:\n%s\033[0m\n", result.ChunkData)
			resultReceived <- true
		case <-time.After(40 * time.Second):
			fmt.Println("\033[31m[TEST] TIMEOUT - No result received within 40 seconds\033[0m")
			resultReceived <- false
		}
	}()

	// Process all test chunks
	fmt.Printf("\033[36m[TEST] Sending %d chunks to orchestrator...\033[0m\n", len(testChunks))
	for i, chunk := range testChunks {
		fmt.Printf("\033[36m[TEST] Sending chunk %d - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d\033[0m\n",
			i+1, chunk.ClientID, chunk.QueryType, chunk.Step, chunk.ChunkNumber, len(chunk.ChunkData))
		orchestrator.ProcessChunk(chunk)
		time.Sleep(50 * time.Millisecond) // Small delay between chunks
	}

	// Signal that all chunks have been sent
	orchestrator.FinishProcessing()

	// Wait for result
	success := <-resultReceived
	if !success {
		t.Error("Test failed - no result received or timeout occurred")
	}

	fmt.Println("\033[36m[TEST] Distributed Group By Test - QueryType 3 Completed\033[0m")
}

// TestDistributedGroupByQueryType4 tests the distributed group by with QueryType 4 (transactions by user_id, store_id)
func TestDistributedGroupByQueryType4(t *testing.T) {
	fmt.Println("=== Starting Distributed Group By Test - QueryType 4 ===")

	// Test configuration
	numWorkers := 2
	numChunks := 3

	// Create test data for QueryType 4 (transactions by user_id, store_id)
	testChunks := createQueryType4TestChunks(numChunks)

	// Create the distributed orchestrator for testing (without RabbitMQ)
	orchestrator := createTestOrchestrator(numWorkers)

	// Start the orchestrator
	orchestrator.Start()
	defer func() {
		fmt.Println("Stopping orchestrator...")
		orchestrator.Stop()
	}()

	// Wait a bit for all components to start
	time.Sleep(100 * time.Millisecond)

	// Start a goroutine to listen for results
	resultReceived := make(chan bool, 1)
	go func() {
		select {
		case result := <-orchestrator.GetResultChannel():
			fmt.Printf("\033[36m[TEST] RECEIVED FINAL RESULT - ClientID: %s, QueryType: %d, Size: %d bytes\033[0m\n",
				result.ClientID, result.QueryType, len(result.ChunkData))
			fmt.Printf("\033[36m[TEST] FINAL RESULT DATA:\n%s\033[0m\n", result.ChunkData)
			resultReceived <- true
		case <-time.After(40 * time.Second):
			fmt.Println("\033[31m[TEST] TIMEOUT - No result received within 40 seconds\033[0m")
			resultReceived <- false
		}
	}()

	// Process all test chunks
	fmt.Printf("\033[36m[TEST] Sending %d chunks to orchestrator...\033[0m\n", len(testChunks))
	for i, chunk := range testChunks {
		fmt.Printf("\033[36m[TEST] Sending chunk %d - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d\033[0m\n",
			i+1, chunk.ClientID, chunk.QueryType, chunk.Step, chunk.ChunkNumber, len(chunk.ChunkData))
		orchestrator.ProcessChunk(chunk)
		time.Sleep(50 * time.Millisecond) // Small delay between chunks
	}

	// Signal that all chunks have been sent
	orchestrator.FinishProcessing()

	// Wait for result
	success := <-resultReceived
	if !success {
		t.Error("Test failed - no result received or timeout occurred")
	}

	fmt.Println("\033[36m[TEST] Distributed Group By Test - QueryType 4 Completed\033[0m")
}

// TestDistributedGroupByMultipleQueryTypes tests the distributed group by with multiple queries simultaneously
func TestDistributedGroupByMultipleQueryTypes(t *testing.T) {
	fmt.Println("=== Starting Distributed Group By Test - Multiple Queries (Simultaneous) ===")

	// Test configuration
	numWorkers := 2
	numChunksPerQuery := 2

	// Create test data for two different queries of the same type but different client IDs
	query2Chunks := createQueryType2TestChunks(numChunksPerQuery)
	query3Chunks := createQueryType3TestChunks(numChunksPerQuery)

	// Create the distributed orchestrator for testing (without RabbitMQ)
	orchestrator := createTestOrchestrator(numWorkers)

	// Start the orchestrator
	orchestrator.Start()
	defer func() {
		fmt.Println("Stopping orchestrator...")
		orchestrator.Stop()
	}()

	// Wait a bit for all components to start
	time.Sleep(100 * time.Millisecond)

	// Start a goroutine to listen for results
	resultReceived := make(chan bool, 1)
	results := make([]*chunk.Chunk, 0)
	go func() {
		timeout := time.After(60 * time.Second)
		for {
			select {
			case result := <-orchestrator.GetResultChannel():
				fmt.Printf("\033[36m[TEST] RECEIVED FINAL RESULT - ClientID: %s, QueryType: %d, Size: %d bytes\033[0m\n",
					result.ClientID, result.QueryType, len(result.ChunkData))
				fmt.Printf("\033[36m[TEST] FINAL RESULT DATA:\n%s\033[0m\n", result.ChunkData)
				results = append(results, result)

				// Check if we received both queries
				if len(results) >= 2 {
					resultReceived <- true
					return
				}
			case <-timeout:
				fmt.Println("\033[31m[TEST] TIMEOUT - Not all results received within 60 seconds\033[0m")
				resultReceived <- false
				return
			}
		}
	}()

	// Send chunks in alternating manner: Query A, Query B, Query A, Query B
	fmt.Printf("\033[36m[TEST] Sending chunks in alternating manner...\033[0m\n")

	// Send Query A chunk 1
	fmt.Printf("\033[36m[TEST] Sending Query A chunk 1 - ClientID: %s, QueryType: %d, Size: %d\033[0m\n",
		query2Chunks[0].ClientID, query2Chunks[0].QueryType, len(query2Chunks[0].ChunkData))
	orchestrator.ProcessChunk(query2Chunks[0])
	time.Sleep(50 * time.Millisecond)

	// Send Query B chunk 1
	fmt.Printf("\033[36m[TEST] Sending Query B chunk 1 - ClientID: %s, QueryType: %d, Size: %d\033[0m\n",
		query3Chunks[0].ClientID, query3Chunks[0].QueryType, len(query3Chunks[0].ChunkData))
	orchestrator.ProcessChunk(query3Chunks[0])
	time.Sleep(50 * time.Millisecond)

	// Send Query A chunk 2 (mark as NOT last chunk since we have more to send)
	query2Chunk2 := *query2Chunks[1] // Copy the chunk
	fmt.Printf("\033[36m[TEST] Sending Query A chunk 2 - ClientID: %s, QueryType: %d, Size: %d\033[0m\n",
		query2Chunk2.ClientID, query2Chunk2.QueryType, len(query2Chunk2.ChunkData))
	orchestrator.ProcessChunk(query2Chunks[1])
	time.Sleep(50 * time.Millisecond)

	// Send Query B chunk 2 (mark as last chunk since this is the final chunk)
	fmt.Printf("\033[36m[TEST] Sending Query B chunk 2 - ClientID: %s, QueryType: %d, Size: %d\033[0m\n",
		query3Chunks[1].ClientID, query3Chunks[1].QueryType, len(query3Chunks[1].ChunkData))
	orchestrator.ProcessChunk(query3Chunks[1])
	time.Sleep(50 * time.Millisecond)

	// Wait a bit for all chunks to be processed
	time.Sleep(200 * time.Millisecond)

	// Signal that all chunks have been sent
	orchestrator.FinishProcessing()

	// Wait for results
	success := <-resultReceived
	if !success {
		t.Error("Test failed - not all results received or timeout occurred")
	}

	// Verify we received results for both queries
	clientIDs := make(map[string]bool)
	for _, result := range results {
		clientIDs[result.ClientID] = true
	}

	if !clientIDs["CLIENT_A"] {
		t.Error("Test failed - no result received for CLIENT_A")
	}
	if !clientIDs["CLIENT_B"] {
		t.Error("Test failed - no result received for CLIENT_B")
	}

	fmt.Println("\033[36m[TEST] Distributed Group By Test - Multiple Queries (Simultaneous) Completed\033[0m")
}

// TestGroupByComponents tests individual components
func TestGroupByComponents(t *testing.T) {
	fmt.Println("=== Starting Component Test ===")

	// Test the group by worker directly
	chunkQueue := make(chan *chunk.Chunk, 10)
	partialChannel := make(chan *chunk.Chunk, 10)

	worker := NewGroupByWorker(0, chunkQueue, partialChannel)

	// Start worker in goroutine
	go worker.Start()

	// Create test chunk
	testChunk := createSimpleTestChunks(1)[0]

	// Send chunk to worker
	chunkQueue <- testChunk
	close(chunkQueue)

	// Wait for result
	select {
	case result := <-partialChannel:
		fmt.Printf("✅ Worker processed chunk - Original: %d lines, Result: %d lines\n",
			len(strings.Split(testChunk.ChunkData, "\n")),
			len(strings.Split(result.ChunkData, "\n")))

		// Verify the result
		if len(result.ChunkData) == 0 {
			t.Error("Expected non-empty result data")
		}

		if result.ClientID != testChunk.ClientID {
			t.Errorf("Expected ClientID %s, got %s", testChunk.ClientID, result.ClientID)
		}

		if result.QueryType != testChunk.QueryType {
			t.Errorf("Expected QueryType %d, got %d", testChunk.QueryType, result.QueryType)
		}

	case <-time.After(5 * time.Second):
		t.Error("Timeout waiting for worker result")
	}

	fmt.Println("✅ Component Test Completed")
}

// createSimpleTestChunks creates simple test chunks
func createSimpleTestChunks(count int) []*chunk.Chunk {
	chunks := make([]*chunk.Chunk, count)

	for i := 0; i < count; i++ {
		// Create transaction_items test data
		lines := []string{
			"transaction_id,item_id,quantity,unit_price,subtotal,created_at",
		}

		// Add some test transaction items
		for j := 0; j < 10; j++ {
			itemID := (j % 3) + 1            // Items 1, 2, 3
			quantity := (j % 2) + 1          // Quantity 1 or 2
			unitPrice := 10.0 + float64(j%5) // Price 10-14
			subtotal := unitPrice * float64(quantity)
			createdAt := fmt.Sprintf("2023-07-%02d 10:%02d:00", (j%3)+1, (j % 60))

			line := fmt.Sprintf("txn-%d-%d,%d,%d,%.1f,%.1f,%s",
				i+1, j+1, itemID, quantity, unitPrice, subtotal, createdAt)
			lines = append(lines, line)
		}

		chunkData := strings.Join(lines, "\n")

		chunks[i] = &chunk.Chunk{
			ClientID:    "CLIENT_A",
			QueryType:   2,
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

// createQueryType2TestChunks creates test chunks for QueryType 2 (transaction_items)
func createQueryType2TestChunks(count int) []*chunk.Chunk {
	chunks := make([]*chunk.Chunk, count)

	for i := 0; i < count; i++ {
		// Create transaction_items test data
		lines := []string{
			"transaction_id,item_id,quantity,unit_price,subtotal,created_at",
		}

		// Add some test transaction items with different months for better grouping
		for j := 0; j < 10; j++ {
			itemID := (j % 3) + 1            // Items 1, 2, 3
			quantity := (j % 2) + 1          // Quantity 1 or 2
			unitPrice := 10.0 + float64(j%5) // Price 10-14
			subtotal := unitPrice * float64(quantity)
			// Use different months to test year/month grouping
			month := 7 + (j % 3) // July, August, September
			day := (j % 28) + 1  // Day 1-28
			createdAt := fmt.Sprintf("2023-%02d-%02d 10:%02d:00", month, day, (j % 60))

			line := fmt.Sprintf("txn-%d-%d,%d,%d,%.1f,%.1f,%s",
				i+1, j+1, itemID, quantity, unitPrice, subtotal, createdAt)
			lines = append(lines, line)
		}

		chunkData := strings.Join(lines, "\n")

		chunks[i] = &chunk.Chunk{
			ClientID:    "CLIENT_A", // Same client ID for all chunks of the same query
			QueryType:   2,
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

// createQueryType3TestChunks creates test chunks for QueryType 3 (transactions)
func createQueryType3TestChunks(count int) []*chunk.Chunk {
	chunks := make([]*chunk.Chunk, count)

	for i := 0; i < count; i++ {
		// Create transactions test data
		lines := []string{
			"transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at",
		}

		// Add some test transactions with different months for semester grouping
		for j := 0; j < 10; j++ {
			storeID := (j % 3) + 1                 // Stores 1, 2, 3
			userID := 100 + (j % 5)                // Users 100-104
			paymentMethodID := (j % 3) + 1         // Payment methods 1, 2, 3
			originalAmount := 50.0 + float64(j%50) // Amount 50-99
			discountApplied := float64(j % 10)     // Discount 0-9
			finalAmount := originalAmount - discountApplied

			// Use different months to test year/semester grouping
			month := 1 + (j % 12) // January to December
			day := (j % 28) + 1   // Day 1-28
			createdAt := fmt.Sprintf("2023-%02d-%02d 10:%02d:00", month, day, (j % 60))

			line := fmt.Sprintf("txn-%d-%d,%d,%d,,%d,%.1f,%.1f,%.1f,%s",
				i+1, j+1, storeID, paymentMethodID, userID, originalAmount, discountApplied, finalAmount, createdAt)
			lines = append(lines, line)
		}

		chunkData := strings.Join(lines, "\n")

		chunks[i] = &chunk.Chunk{
			ClientID:    "CLIENT_A", // Same client ID for all chunks of the same query
			QueryType:   3,
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

// createQueryType4TestChunks creates test chunks for QueryType 4 (transactions by user_id, store_id)
func createQueryType4TestChunks(count int) []*chunk.Chunk {
	chunks := make([]*chunk.Chunk, count)

	for i := 0; i < count; i++ {
		// Create transactions test data for QueryType 4
		lines := []string{
			"transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at",
		}

		// Add some test transactions with different user_id and store_id combinations
		for j := 0; j < 10; j++ {
			storeID := (j % 3) + 1                 // Stores 1, 2, 3
			userID := 100 + (j % 5)                // Users 100-104
			paymentMethodID := (j % 3) + 1         // Payment methods 1, 2, 3
			originalAmount := 50.0 + float64(j%50) // Amount 50-99
			discountApplied := float64(j % 10)     // Discount 0-9
			finalAmount := originalAmount - discountApplied

			// Use different months for variety
			month := 1 + (j % 12) // January to December
			day := (j % 28) + 1   // Day 1-28
			createdAt := fmt.Sprintf("2023-%02d-%02d 10:%02d:00", month, day, (j % 60))

			line := fmt.Sprintf("txn-%d-%d,%d,%d,,%d,%.1f,%.1f,%.1f,%s",
				i+1, j+1, storeID, paymentMethodID, userID, originalAmount, discountApplied, finalAmount, createdAt)
			lines = append(lines, line)
		}

		chunkData := strings.Join(lines, "\n")

		chunks[i] = &chunk.Chunk{
			ClientID:    "CLIENT_A", // Same client ID for all chunks of the same query
			QueryType:   4,
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

// createTestOrchestrator creates an orchestrator for testing without RabbitMQ
func createTestOrchestrator(numWorkers int) *GroupByOrchestrator {
	// Create channels for internal communication
	chunkQueue := make(chan *chunk.Chunk, numWorkers*2)
	workerChannels := make([]chan *chunk.Chunk, numWorkers)
	partialChannels := make([]chan *chunk.Chunk, numWorkers)
	reducerChannel := make(chan *chunk.Chunk, numWorkers)

	for i := 0; i < numWorkers; i++ {
		workerChannels[i] = make(chan *chunk.Chunk)
		partialChannels[i] = make(chan *chunk.Chunk)
	}

	orchestrator := &GroupByOrchestrator{
		// No RabbitMQ components for testing
		consumer:      nil,
		replyProducer: nil,
		config:        nil,

		// Distributed processing components
		chunkQueue:      chunkQueue,
		numWorkers:      numWorkers,
		workerChannels:  workerChannels,
		partialChannels: partialChannels,
		reducerChannel:  reducerChannel,
		done:            make(chan bool),
	}

	// Create workers
	orchestrator.workers = make([]*GroupByWorker, numWorkers)
	for i := 0; i < numWorkers; i++ {
		orchestrator.workers[i] = NewGroupByWorker(i, chunkQueue, workerChannels[i])
	}

	// Create partial reducers
	orchestrator.partialReducers = make([]*GroupByPartialReducer, numWorkers)
	for i := 0; i < numWorkers; i++ {
		orchestrator.partialReducers[i] = NewGroupByPartialReducer(i, workerChannels[i], partialChannels[i])
	}

	// Create final reducer
	orchestrator.reducer = NewGroupByReducer(partialChannels, reducerChannel)

	return orchestrator
}
