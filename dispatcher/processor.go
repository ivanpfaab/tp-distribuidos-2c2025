package main

import (
	"fmt"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/testing"
)

// processMessage processes incoming messages and sends formatted data to client
func (rd *ResultsDispatcherWorker) processMessage(chunkData *chunk.Chunk) middleware.MessageMiddlewareError {

	// Check if chunk was already processed (duplicate detection)
	if rd.messageManager.IsProcessed(chunkData.ClientID, chunkData.ID) {
		testing.LogInfo("Results Dispatcher", "Chunk %s already processed, skipping", chunkData.ID)
		return 0 // Return 0 to ACK (handled by createCallback)
	}

	// Send formatted data to client
	rd.sendFormattedDataToClient(chunkData)

	// Persist metadata through StatefulWorkerManager
	// Note: ProcessMessage internally calls UpdateState() which processes chunk notifications
	// through the CompletionTracker, so we don't need to call processChunkNotification() separately
	if err := rd.statefulWorkerManager.ProcessMessage(chunkData); err != nil {
		testing.LogError("Results Dispatcher", "Failed to process message through StatefulWorkerManager: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Mark chunk as processed after successful processing
	if err := rd.messageManager.MarkProcessed(chunkData.ClientID, chunkData.ID); err != nil {
		testing.LogError("Results Dispatcher", "Failed to mark chunk as processed: %v", err)
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
