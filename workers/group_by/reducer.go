package main

import (
	"fmt"
	"strings"
	"sync"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
)

// GroupByReducer collects results from all partial reducers and applies final reduction
type GroupByReducer struct {
	partialChannels []<-chan *chunk.Chunk
	resultChannel   chan<- *chunk.Chunk
	collectedData   []string
	queryType       byte
	step            int
	clientID        string
	expectedResults int
	receivedResults int
	mu              sync.Mutex
	running         bool
	queryProcessor  *QueryProcessor
}

// NewGroupByReducer creates a new final reducer
func NewGroupByReducer(partialChannels []chan *chunk.Chunk, resultChannel chan *chunk.Chunk) *GroupByReducer {
	// Convert []chan to []<-chan for read-only access
	readChannels := make([]<-chan *chunk.Chunk, len(partialChannels))
	for i, ch := range partialChannels {
		readChannels[i] = ch
	}

	return &GroupByReducer{
		partialChannels: readChannels,
		resultChannel:   resultChannel,
		collectedData:   make([]string, 0),
		expectedResults: len(partialChannels),
		receivedResults: 0,
		running:         false,
		queryProcessor:  NewQueryProcessor(),
	}
}

// Start starts the final reducer
func (gbr *GroupByReducer) Start() {
	gbr.running = true
	fmt.Printf("\033[32m[FINAL REDUCER] Started, expecting %d results from partial reducers\033[0m\n", gbr.expectedResults)

	// Use a WaitGroup to wait for all partial reducers to finish
	var wg sync.WaitGroup

	// Start goroutines to listen to each partial reducer
	for i, partialChannel := range gbr.partialChannels {
		wg.Add(1)
		go func(channel <-chan *chunk.Chunk, reducerID int) {
			defer wg.Done()
			gbr.listenToPartialReducer(channel, reducerID)
		}(partialChannel, i)
	}

	// Wait for all partial reducers to finish
	wg.Wait()

	// Apply final reduction if we have received all expected results
	if gbr.receivedResults >= gbr.expectedResults {
		gbr.applyFinalReduction()
	}

	fmt.Printf("\033[32m[FINAL REDUCER] Stopped\033[0m\n")
}

// listenToPartialReducer listens to a specific partial reducer
func (gbr *GroupByReducer) listenToPartialReducer(partialChannel <-chan *chunk.Chunk, reducerID int) {
	fmt.Printf("\033[32m[FINAL REDUCER] Listening to partial reducer %d\033[0m\n", reducerID)

	for resultChunk := range partialChannel {
		gbr.mu.Lock()

		fmt.Printf("\033[32m[FINAL REDUCER] RECEIVED - From Partial Reducer %d, ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d, IsLastChunk: %t\033[0m\n",
			reducerID, resultChunk.ClientID, resultChunk.QueryType, resultChunk.Step, resultChunk.ChunkNumber, len(resultChunk.ChunkData), resultChunk.IsLastChunk)
		fmt.Printf("\033[32m[FINAL REDUCER] PARTIAL DATA:\n%s\033[0m\n", resultChunk.ChunkData)

		// Initialize query metadata if this is the first result
		if gbr.receivedResults == 0 {
			gbr.queryType = resultChunk.QueryType
			gbr.step = resultChunk.Step
			gbr.clientID = resultChunk.ClientID
			fmt.Printf("\033[32m[FINAL REDUCER] INITIALIZED - QueryType: %d, Step: %d, ClientID: %s\033[0m\n",
				gbr.queryType, gbr.step, gbr.clientID)
		}

		// Store the result from partial reducer
		lines := strings.Split(resultChunk.ChunkData, "\n")
		for _, line := range lines {
			if strings.TrimSpace(line) != "" {
				gbr.collectedData = append(gbr.collectedData, line)
			}
		}

		gbr.receivedResults++
		fmt.Printf("\033[32m[FINAL REDUCER] COLLECTED - From Partial %d (%d/%d) - Lines: %d, Total: %d\033[0m\n",
			reducerID, gbr.receivedResults, gbr.expectedResults, len(lines), len(gbr.collectedData))

		// Check if we have received all expected results
		if gbr.receivedResults >= gbr.expectedResults {
			fmt.Printf("\033[32m[FINAL REDUCER] Received all %d results, applying final reduction\033[0m\n", gbr.expectedResults)
			// Apply final reduction without holding the mutex to avoid deadlock
			go gbr.applyFinalReduction()
		}

		gbr.mu.Unlock()
	}

	fmt.Printf("\033[32m[FINAL REDUCER] Partial reducer %d finished\033[0m\n", reducerID)
}

// applyFinalReduction applies the final group by operation to all collected data
func (gbr *GroupByReducer) applyFinalReduction() {
	gbr.mu.Lock()
	defer gbr.mu.Unlock()

	fmt.Printf("\033[32m[FINAL REDUCER] APPLYING REDUCTION - Collected: %d lines from %d partial reducers\033[0m\n",
		len(gbr.collectedData), gbr.receivedResults)

	// Combine all collected data
	combinedData := strings.Join(gbr.collectedData, "\n")

	// Create a temporary chunk for processing
	tempChunk := &chunk.Chunk{
		ClientID:    gbr.clientID,
		QueryType:   gbr.queryType,
		ChunkSize:   len(combinedData),
		ChunkNumber: 0,
		IsLastChunk: true,
		Step:        gbr.step,
		ChunkData:   combinedData,
	}

	// Process the combined data using the query processor (GROUPED DATA)
	results := gbr.queryProcessor.ProcessQuery(tempChunk, GroupedData)

	// Convert results to CSV format
	resultData := gbr.queryProcessor.ResultsToCSV(results, int(gbr.queryType))

	// Get the number of groups for logging
	numGroups := getResultLength(results, int(gbr.queryType))

	resultChunk := &chunk.Chunk{
		ClientID:    gbr.clientID,
		QueryType:   gbr.queryType,
		TableID:     0, // Final reducer result
		ChunkSize:   len(resultData),
		ChunkNumber: 0,        // Final result
		IsLastChunk: true,     // This is the final result
		Step:        gbr.step, // Keep same step
		ChunkData:   resultData,
	}

	// Send final result
	gbr.resultChannel <- resultChunk
	fmt.Printf("\033[32m[FINAL REDUCER] SENT RESULT - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Groups: %d, Size: %d, IsLastChunk: %t\033[0m\n",
		gbr.clientID, gbr.queryType, gbr.step, resultChunk.ChunkNumber, numGroups, len(resultData), resultChunk.IsLastChunk)
	fmt.Printf("\033[32m[FINAL REDUCER] FINAL DATA:\n%s\033[0m\n", resultData)

	// Clear collected data for next query
	gbr.collectedData = make([]string, 0)
	gbr.receivedResults = 0
}

// Stop stops the final reducer
func (gbr *GroupByReducer) Stop() {
	gbr.running = false
}
