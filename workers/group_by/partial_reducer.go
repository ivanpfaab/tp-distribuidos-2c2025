package main

import (
	"fmt"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
)

// QueryState tracks the state of a specific query
type QueryState struct {
	collectedData  []string
	queryType      byte
	step           int
	clientID       string
	workerFinished bool
}

// GroupByPartialReducer collects results from a worker and applies reduction
type GroupByPartialReducer struct {
	reducerID      int
	workerChannel  <-chan *chunk.Chunk
	reducerChannel chan<- *chunk.Chunk
	queryStates    map[string]*QueryState // key: "queryType_clientID"
	running        bool
	queryProcessor *QueryProcessor
}

// NewGroupByPartialReducer creates a new partial reducer
func NewGroupByPartialReducer(reducerID int, workerChannel <-chan *chunk.Chunk, reducerChannel chan<- *chunk.Chunk) *GroupByPartialReducer {
	return &GroupByPartialReducer{
		reducerID:      reducerID,
		workerChannel:  workerChannel,
		reducerChannel: reducerChannel,
		queryStates:    make(map[string]*QueryState),
		running:        false,
		queryProcessor: NewQueryProcessor(),
	}
}

// Start starts the partial reducer
func (gbpr *GroupByPartialReducer) Start() {
	gbpr.running = true
	fmt.Printf("\033[33m[PARTIAL REDUCER %d] Started and waiting for worker results\033[0m\n", gbpr.reducerID)

	for gbpr.running {
		resultChunk, ok := <-gbpr.workerChannel
		if !ok {
			// Worker channel closed, worker finished
			fmt.Printf("\033[33m[PARTIAL REDUCER %d] Worker finished, applying reduction for all queries\033[0m\n", gbpr.reducerID)
			gbpr.applyReductionForAllQueries()
			gbpr.running = false
			break
		}

		fmt.Printf("\033[33m[PARTIAL REDUCER %d] RECEIVED RESULT - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d, IsLastChunk: %t\033[0m\n",
			gbpr.reducerID, resultChunk.ClientID, resultChunk.QueryType, resultChunk.Step, resultChunk.ChunkNumber, len(resultChunk.ChunkData), resultChunk.IsLastChunk)
		fmt.Printf("\033[33m[PARTIAL REDUCER %d] RESULT DATA:\n%s\033[0m\n", gbpr.reducerID, resultChunk.ChunkData)

		// Check if this is a query completion signal (ChunkNumber == -1)
		if resultChunk.ChunkNumber == -1 {
			queryKey := fmt.Sprintf("%d_%s", resultChunk.QueryType, resultChunk.ClientID)
			fmt.Printf("\033[33m[PARTIAL REDUCER %d] QUERY COMPLETION SIGNAL - QueryKey: %s\033[0m\n", gbpr.reducerID, queryKey)

			// Apply reduction for this specific query
			if queryState, exists := gbpr.queryStates[queryKey]; exists {
				fmt.Printf("\033[33m[PARTIAL REDUCER %d] APPLYING REDUCTION FOR QUERY - QueryKey: %s\033[0m\n", gbpr.reducerID, queryKey)
				gbpr.applyReductionForQuery(queryKey, queryState)
			} else {
				fmt.Printf("\033[33m[PARTIAL REDUCER %d] WARNING - No data found for completed query: %s\033[0m\n", gbpr.reducerID, queryKey)
			}
		} else {
			// Store the result from worker
			gbpr.storeResult(resultChunk)
		}
	}

	fmt.Printf("\033[33m[PARTIAL REDUCER %d] Stopped\033[0m\n", gbpr.reducerID)
}

// storeResult stores a result from the worker
func (gbpr *GroupByPartialReducer) storeResult(resultChunk *chunk.Chunk) {
	fmt.Printf("\033[33m[PARTIAL REDUCER %d] STORING - ChunkNumber: %d, ClientID: %s, QueryType: %d\033[0m\n",
		gbpr.reducerID, resultChunk.ChunkNumber, resultChunk.ClientID, resultChunk.QueryType)

	// Create query key
	queryKey := fmt.Sprintf("%d_%s", resultChunk.QueryType, resultChunk.ClientID)

	// Initialize query state if this is the first chunk for this query
	if _, exists := gbpr.queryStates[queryKey]; !exists {
		gbpr.queryStates[queryKey] = &QueryState{
			collectedData:  make([]string, 0),
			queryType:      resultChunk.QueryType,
			step:           resultChunk.Step,
			clientID:       resultChunk.ClientID,
			workerFinished: false,
		}
		fmt.Printf("\033[33m[PARTIAL REDUCER %d] INITIALIZED - QueryType: %d, Step: %d, ClientID: %s\033[0m\n",
			gbpr.reducerID, resultChunk.QueryType, resultChunk.Step, resultChunk.ClientID)
	}

	// Add the processed data to the query's collection
	lines := strings.Split(resultChunk.ChunkData, "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			gbpr.queryStates[queryKey].collectedData = append(gbpr.queryStates[queryKey].collectedData, line)
		}
	}

	fmt.Printf("\033[33m[PARTIAL REDUCER %d] STORED - ChunkNumber: %d, Lines: %d, Total: %d lines for QueryType %d\033[0m\n",
		gbpr.reducerID, resultChunk.ChunkNumber, len(lines), len(gbpr.queryStates[queryKey].collectedData), resultChunk.QueryType)
}

// applyReductionForAllQueries applies reduction to all active queries
func (gbpr *GroupByPartialReducer) applyReductionForAllQueries() {
	fmt.Printf("\033[33m[PARTIAL REDUCER %d] APPLYING REDUCTION FOR ALL QUERIES - Active queries: %d\033[0m\n",
		gbpr.reducerID, len(gbpr.queryStates))

	for queryKey, queryState := range gbpr.queryStates {
		fmt.Printf("\033[33m[PARTIAL REDUCER %d] Processing query %s with %d lines\033[0m\n",
			gbpr.reducerID, queryKey, len(queryState.collectedData))
		gbpr.applyReductionForQuery(queryKey, queryState)
	}
}

// applyReductionForQuery applies the group by operation to a specific query's collected data
func (gbpr *GroupByPartialReducer) applyReductionForQuery(queryKey string, queryState *QueryState) {
	fmt.Printf("\033[33m[PARTIAL REDUCER %d] APPLYING REDUCTION - Query: %s, Collected: %d lines\033[0m\n",
		gbpr.reducerID, queryKey, len(queryState.collectedData))

	// Combine all collected data for this query
	combinedData := strings.Join(queryState.collectedData, "\n")

	// Create a temporary chunk for processing
	tempChunk := &chunk.Chunk{
		ClientID:    queryState.clientID,
		QueryType:   queryState.queryType,
		ChunkSize:   len(combinedData),
		ChunkNumber: gbpr.reducerID,
		IsLastChunk: true,
		Step:        queryState.step,
		ChunkData:   combinedData,
	}

	// Process the combined data using the query processor (GROUPED DATA)
	results := gbpr.queryProcessor.ProcessQuery(tempChunk, GroupedData)

	// Convert results to CSV format
	resultData := gbpr.queryProcessor.ResultsToCSV(results, int(queryState.queryType))

	// Get the number of groups for logging
	numGroups := getResultLength(results, int(queryState.queryType))

	resultChunk := &chunk.Chunk{
		ClientID:    queryState.clientID,
		QueryType:   queryState.queryType,
		TableID:     0, // Partial reducer result
		ChunkSize:   len(resultData),
		ChunkNumber: gbpr.reducerID,  // Use reducer ID as chunk number
		IsLastChunk: false,           // Will be determined by final reducer
		Step:        queryState.step, // Keep same step
		ChunkData:   resultData,
	}

	// Send result to final reducer
	gbpr.reducerChannel <- resultChunk
	fmt.Printf("\033[33m[PARTIAL REDUCER %d] SENT RESULT - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Groups: %d, Size: %d, IsLastChunk: %t\033[0m\n",
		gbpr.reducerID, queryState.clientID, queryState.queryType, queryState.step, resultChunk.ChunkNumber, numGroups, len(resultData), resultChunk.IsLastChunk)
	fmt.Printf("\033[33m[PARTIAL REDUCER %d] REDUCED DATA:\n%s\033[0m\n", gbpr.reducerID, resultData)

	// Clear collected data for this query
	queryState.collectedData = make([]string, 0)
}

// Stop stops the partial reducer
func (gbpr *GroupByPartialReducer) Stop() {
	gbpr.running = false
}
