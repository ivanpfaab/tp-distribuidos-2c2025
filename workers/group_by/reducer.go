package main

import (
	"fmt"
	"strings"
	"sync"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
)

// QueryReducerState tracks the state of a specific query in the final reducer
type QueryReducerState struct {
	collectedData   []string
	queryType       byte
	step            int
	clientID        string
	expectedResults int
	receivedResults int
	processed       bool // Track if this query has been processed
}

// GroupByReducer collects results from all partial reducers and applies final reduction
type GroupByReducer struct {
	partialChannels []<-chan *chunk.Chunk
	resultChannel   chan<- *chunk.Chunk
	queryStates     map[string]*QueryReducerState // key: "queryType_clientID"
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
		queryStates:     make(map[string]*QueryReducerState),
		running:         false,
		queryProcessor:  NewQueryProcessor(),
	}
}

// Start starts the final reducer
func (gbr *GroupByReducer) Start() {
	gbr.running = true
	fmt.Printf("\033[32m[FINAL REDUCER] Started, listening to %d partial reducers\033[0m\n", len(gbr.partialChannels))

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

		// Create query key
		queryKey := fmt.Sprintf("%d_%s", resultChunk.QueryType, resultChunk.ClientID)

		// Initialize query state if this is the first result for this query
		if _, exists := gbr.queryStates[queryKey]; !exists {
			gbr.queryStates[queryKey] = &QueryReducerState{
				collectedData:   make([]string, 0),
				queryType:       resultChunk.QueryType,
				step:            resultChunk.Step,
				clientID:        resultChunk.ClientID,
				expectedResults: len(gbr.partialChannels),
				receivedResults: 0,
				processed:       false,
			}
			fmt.Printf("\033[32m[FINAL REDUCER] INITIALIZED - QueryType: %d, Step: %d, ClientID: %s\033[0m\n",
				resultChunk.QueryType, resultChunk.Step, resultChunk.ClientID)
		}

		// Store the result from partial reducer
		lines := strings.Split(resultChunk.ChunkData, "\n")
		for _, line := range lines {
			if strings.TrimSpace(line) != "" {
				gbr.queryStates[queryKey].collectedData = append(gbr.queryStates[queryKey].collectedData, line)
			}
		}

		gbr.queryStates[queryKey].receivedResults++
		fmt.Printf("\033[32m[FINAL REDUCER] COLLECTED - From Partial %d (%d/%d) - Lines: %d, Total: %d for QueryType %d\033[0m\n",
			reducerID, gbr.queryStates[queryKey].receivedResults, gbr.queryStates[queryKey].expectedResults, len(lines), len(gbr.queryStates[queryKey].collectedData), resultChunk.QueryType)

		// Check if we have received all expected results for this query and haven't processed it yet
		if gbr.queryStates[queryKey].receivedResults >= gbr.queryStates[queryKey].expectedResults && !gbr.queryStates[queryKey].processed {
			fmt.Printf("\033[32m[FINAL REDUCER] Received all %d results for query %s, applying final reduction\033[0m\n", gbr.queryStates[queryKey].expectedResults, queryKey)
			// Mark as processed to prevent multiple processing
			gbr.queryStates[queryKey].processed = true
			// Apply final reduction for this specific query without holding the mutex to avoid deadlock
			go gbr.applyFinalReductionForQuery(queryKey, gbr.queryStates[queryKey])
		}

		gbr.mu.Unlock()
	}

	fmt.Printf("\033[32m[FINAL REDUCER] Partial reducer %d finished\033[0m\n", reducerID)
}

// applyFinalReductionForQuery applies the final group by operation to a specific query's collected data
func (gbr *GroupByReducer) applyFinalReductionForQuery(queryKey string, queryState *QueryReducerState) {
	fmt.Printf("\033[32m[FINAL REDUCER] APPLYING REDUCTION - Query: %s, Collected: %d lines from %d partial reducers\033[0m\n",
		queryKey, len(queryState.collectedData), queryState.receivedResults)

	// Combine all collected data for this query
	combinedData := strings.Join(queryState.collectedData, "\n")

	// Create a temporary chunk for processing
	tempChunk := &chunk.Chunk{
		ClientID:    queryState.clientID,
		QueryType:   queryState.queryType,
		ChunkSize:   len(combinedData),
		ChunkNumber: 0,
		IsLastChunk: true,
		Step:        queryState.step,
		ChunkData:   combinedData,
	}

	// Process the combined data using the query processor (GROUPED DATA)
	results := gbr.queryProcessor.ProcessQuery(tempChunk, GroupedData)

	// Convert results to CSV format
	resultData := gbr.queryProcessor.ResultsToCSV(results, int(queryState.queryType))

	// Get the number of groups for logging
	numGroups := getResultLength(results, int(queryState.queryType))

	resultChunk := &chunk.Chunk{
		ClientID:    queryState.clientID,
		QueryType:   queryState.queryType,
		TableID:     0, // Final reducer result
		ChunkSize:   len(resultData),
		ChunkNumber: 0,               // Final result
		IsLastChunk: true,            // This is the final result
		Step:        queryState.step, // Keep same step
		ChunkData:   resultData,
	}

	// Send final result
	gbr.resultChannel <- resultChunk
	fmt.Printf("\033[32m[FINAL REDUCER] SENT RESULT - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Groups: %d, Size: %d, IsLastChunk: %t\033[0m\n",
		queryState.clientID, queryState.queryType, queryState.step, resultChunk.ChunkNumber, numGroups, len(resultData), resultChunk.IsLastChunk)
	fmt.Printf("\033[32m[FINAL REDUCER] FINAL DATA:\n%s\033[0m\n", resultData)

	// Clear collected data for this query
	queryState.collectedData = make([]string, 0)
}

// Stop stops the final reducer
func (gbr *GroupByReducer) Stop() {
	gbr.running = false
}
