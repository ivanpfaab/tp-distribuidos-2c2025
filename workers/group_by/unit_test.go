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

	fmt.Println("\033[32m[TEST] Simple Distributed Group By Test Completed\033[0m")
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
		// Create simple test data
		lines := make([]string, 12) // More than 10 lines to test filtering
		for j := 0; j < 12; j++ {
			lines[j] = fmt.Sprintf("Data line %d from chunk %d", j+1, i+1)
		}

		chunkData := strings.Join(lines, "\n")

		chunks[i] = &chunk.Chunk{
			ClientID:    fmt.Sprintf("TEST_CLIENT_%d", i+1),
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
