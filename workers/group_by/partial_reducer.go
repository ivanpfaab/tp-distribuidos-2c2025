package main

import (
	"fmt"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
)

// GroupByPartialReducer collects results from a worker and applies reduction
type GroupByPartialReducer struct {
	reducerID      int
	workerChannel  <-chan *chunk.Chunk
	reducerChannel chan<- *chunk.Chunk
	collectedData  []string
	queryType      byte
	step           int
	clientID       string
	workerFinished bool
	running        bool
	queryProcessor *QueryProcessor
}

// NewGroupByPartialReducer creates a new partial reducer
func NewGroupByPartialReducer(reducerID int, workerChannel <-chan *chunk.Chunk, reducerChannel chan<- *chunk.Chunk) *GroupByPartialReducer {
	return &GroupByPartialReducer{
		reducerID:      reducerID,
		workerChannel:  workerChannel,
		reducerChannel: reducerChannel,
		collectedData:  make([]string, 0),
		workerFinished: false,
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
			fmt.Printf("\033[33m[PARTIAL REDUCER %d] Worker finished, applying reduction\033[0m\n", gbpr.reducerID)
			gbpr.workerFinished = true
			gbpr.applyReduction()
			gbpr.running = false
			break
		}

		fmt.Printf("\033[33m[PARTIAL REDUCER %d] RECEIVED RESULT - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Size: %d, IsLastChunk: %t\033[0m\n",
			gbpr.reducerID, resultChunk.ClientID, resultChunk.QueryType, resultChunk.Step, resultChunk.ChunkNumber, len(resultChunk.ChunkData), resultChunk.IsLastChunk)
		fmt.Printf("\033[33m[PARTIAL REDUCER %d] RESULT DATA:\n%s\033[0m\n", gbpr.reducerID, resultChunk.ChunkData)

		// Store the result from worker
		gbpr.storeResult(resultChunk)
	}

	fmt.Printf("\033[33m[PARTIAL REDUCER %d] Stopped\033[0m\n", gbpr.reducerID)
}

// storeResult stores a result from the worker
func (gbpr *GroupByPartialReducer) storeResult(resultChunk *chunk.Chunk) {
	fmt.Printf("\033[33m[PARTIAL REDUCER %d] STORING - ChunkNumber: %d, ClientID: %s\033[0m\n",
		gbpr.reducerID, resultChunk.ChunkNumber, resultChunk.ClientID)

	// Initialize query metadata if this is the first chunk
	if len(gbpr.collectedData) == 0 {
		gbpr.queryType = resultChunk.QueryType
		gbpr.step = resultChunk.Step
		gbpr.clientID = resultChunk.ClientID
		fmt.Printf("\033[33m[PARTIAL REDUCER %d] INITIALIZED - QueryType: %d, Step: %d, ClientID: %s\033[0m\n",
			gbpr.reducerID, gbpr.queryType, gbpr.step, gbpr.clientID)
	}

	// Add the processed data to our collection
	lines := strings.Split(resultChunk.ChunkData, "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			gbpr.collectedData = append(gbpr.collectedData, line)
		}
	}

	fmt.Printf("\033[33m[PARTIAL REDUCER %d] STORED - ChunkNumber: %d, Lines: %d, Total: %d lines\033[0m\n",
		gbpr.reducerID, resultChunk.ChunkNumber, len(lines), len(gbpr.collectedData))
}

// applyReduction applies the group by operation to all collected data
func (gbpr *GroupByPartialReducer) applyReduction() {
	fmt.Printf("\033[33m[PARTIAL REDUCER %d] APPLYING REDUCTION - Collected: %d lines\033[0m\n",
		gbpr.reducerID, len(gbpr.collectedData))

	// Combine all collected data
	combinedData := strings.Join(gbpr.collectedData, "\n")

	// Create a temporary chunk for processing
	tempChunk := &chunk.Chunk{
		ClientID:    gbpr.clientID,
		QueryType:   gbpr.queryType,
		ChunkSize:   len(combinedData),
		ChunkNumber: gbpr.reducerID,
		IsLastChunk: true,
		Step:        gbpr.step,
		ChunkData:   combinedData,
	}

	// Process the combined data using the query processor (GROUPED DATA)
	results := gbpr.queryProcessor.ProcessQuery(tempChunk, GroupedData)

	// Convert results to CSV format
	resultData := gbpr.queryProcessor.ResultsToCSV(results, int(gbpr.queryType))

	// Get the number of groups for logging
	numGroups := getResultLength(results, int(gbpr.queryType))

	resultChunk := &chunk.Chunk{
		ClientID:    gbpr.clientID,
		QueryType:   gbpr.queryType,
		TableID:     0, // Partial reducer result
		ChunkSize:   len(resultData),
		ChunkNumber: gbpr.reducerID, // Use reducer ID as chunk number
		IsLastChunk: false,          // Will be determined by final reducer
		Step:        gbpr.step,      // Keep same step
		ChunkData:   resultData,
	}

	// Send result to final reducer
	gbpr.reducerChannel <- resultChunk
	fmt.Printf("\033[33m[PARTIAL REDUCER %d] SENT RESULT - ClientID: %s, QueryType: %d, Step: %d, ChunkNumber: %d, Groups: %d, Size: %d, IsLastChunk: %t\033[0m\n",
		gbpr.reducerID, gbpr.clientID, gbpr.queryType, gbpr.step, resultChunk.ChunkNumber, numGroups, len(resultData), resultChunk.IsLastChunk)
	fmt.Printf("\033[33m[PARTIAL REDUCER %d] REDUCED DATA:\n%s\033[0m\n", gbpr.reducerID, resultData)

	// Clear collected data for next query
	gbpr.collectedData = make([]string, 0)
}

// Stop stops the partial reducer
func (gbpr *GroupByPartialReducer) Stop() {
	gbpr.running = false
}
