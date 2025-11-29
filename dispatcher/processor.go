package main

import (
	"fmt"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/testing"
	"github.com/tp-distribuidos-2c2025/workers/shared"
)

// processMessage processes incoming messages and sends formatted data to client
func (rd *ResultsDispatcherWorker) processMessage(chunkData *chunk.Chunk, queryType int) middleware.MessageMiddlewareError {

	// Check if chunk was already processed (duplicate detection)
	if rd.messageManager.IsProcessed(chunkData.ID) {
		testing.LogInfo("Results Dispatcher", "Chunk %s already processed, skipping", chunkData.ID)
		return 0 // Return 0 to ACK (handled by createCallback)
	}

	// Send formatted data to client
	rd.sendFormattedDataToClient(chunkData)

	// Persist metadata through StatefulWorkerManager
	if err := rd.statefulWorkerManager.ProcessMessage(chunkData); err != nil {
		testing.LogError("Results Dispatcher", "Failed to process message through StatefulWorkerManager: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	chunkNotification := signals.NewChunkNotification(
		chunkData.ClientID,
		chunkData.FileID,
		"query-results",
		chunkData.TableID,
		chunkData.ChunkNumber,
		chunkData.IsLastChunk,
		chunkData.IsLastFromTable,
	)

	if err := rd.processChunkNotification(chunkNotification, queryType); err != 0 {
		testing.LogError("Results Dispatcher", "Failed to process Query%d chunk notification: %v", queryType, err)
		return err
	}

	// Mark chunk as processed after successful processing
	if err := rd.messageManager.MarkProcessed(chunkData.ID); err != nil {
		testing.LogError("Results Dispatcher", "Failed to mark chunk as processed: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	return middleware.MessageMiddlewareError(0)
}

// processChunkNotification processes chunk notifications for completion tracking (all queries)
func (rd *ResultsDispatcherWorker) processChunkNotification(notification *signals.ChunkNotification, queryType int) middleware.MessageMiddlewareError {
	// Get the appropriate CompletionTracker
	var tracker *shared.CompletionTracker
	switch queryType {
	case QueryType1:
		tracker = rd.query1Tracker
	case QueryType2:
		tracker = rd.query2Tracker
	case QueryType3:
		tracker = rd.query3Tracker
	case QueryType4:
		tracker = rd.query4Tracker
	default:
		testing.LogError("Results Dispatcher", "processChunkNotification called for unsupported query type: %d", queryType)
		return middleware.MessageMiddlewareMessageError
	}

	// Process notification through CompletionTracker
	if err := tracker.ProcessChunkNotification(notification); err != nil {
		testing.LogError("Results Dispatcher", "Failed to process Query%d chunk notification: %v", queryType, err)
		return middleware.MessageMiddlewareMessageError
	}

	return middleware.MessageMiddlewareError(0)
}

// sendFormattedDataToClient formats the chunk data and sends it to client request handler
func (rd *ResultsDispatcherWorker) sendFormattedDataToClient(chunkData *chunk.Chunk) middleware.MessageMiddlewareError {
	// Format the data the same way as it was being printed
	var formattedRows []string
	rows := strings.Split(strings.TrimSpace(chunkData.ChunkData), "\n")
	for _, row := range rows {
		if strings.TrimSpace(row) != "" { // Skip empty rows
			formattedRow := fmt.Sprintf("%s | Q%d | %s", chunkData.ClientID, chunkData.QueryType, row)
			formattedRows = append(formattedRows, formattedRow)
		}
	}

	// Join all formatted rows with newlines
	formattedData := strings.Join(formattedRows, "\n")
	if formattedData != "" {
		formattedData += "\n" // Add final newline
	}

	chunkData.ChunkData = formattedData
	chunkData.ChunkSize = len(formattedData)

	chunkMessage := chunk.NewChunkMessage(chunkData)
	serializedData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		testing.LogError("Results Dispatcher", "Failed to serialize chunk: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	return rd.clientResultsProducer.Send(serializedData)
}
