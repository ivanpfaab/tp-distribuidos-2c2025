package main

import (
	"fmt"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// processMessage processes incoming messages and sends formatted data to client
func (rd *ResultsDispatcherWorker) processMessage(delivery amqp.Delivery, queryType int) middleware.MessageMiddlewareError {
	// Deserialize the message
	message, err := deserializer.Deserialize(delivery.Body)
	if err != nil {
		testing.LogError("Results Dispatcher", "Failed to deserialize message: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Check if it's a Chunk message for data processing
	chunkData, ok := message.(*chunk.Chunk)
	if !ok {
		testing.LogError("Results Dispatcher", "Failed to cast message to chunk")
		return middleware.MessageMiddlewareMessageError
	}
	rd.sendFormattedDataToClient(chunkData)

	// Handle completion tracking based on query type
	if queryType == QueryType1 {
		// Query1: Use completion tracker (may receive multiple chunks)
		chunkNotification := signals.NewChunkNotification(
			chunkData.ClientID,
			chunkData.FileID,
			"query1-streaming",
			chunkData.TableID,
			chunkData.ChunkNumber,
			chunkData.IsLastChunk,
			chunkData.IsLastFromTable,
		)

		if err := rd.processChunkNotification(chunkNotification, queryType); err != 0 {
			testing.LogError("Results Dispatcher", "Failed to process Query1 chunk notification: %v", err)
			return err
		}
	} else {
		// Queries 2, 3, 4: Simple counter (only one chunk expected)
		rd.handleSingleChunkCompletion(chunkData.ClientID, queryType)
	}
	return middleware.MessageMiddlewareError(0)
}

// handleSingleChunkCompletion handles completion for queries that only receive one chunk
func (rd *ResultsDispatcherWorker) handleSingleChunkCompletion(clientID string, queryType int) {
	rd.completionMutex.Lock()
	defer rd.completionMutex.Unlock()

	// Initialize client query status if not exists
	if rd.clientQueryCompletion[clientID] == nil {
		rd.clientQueryCompletion[clientID] = &ClientQueryStatus{
			ClientID:            clientID,
			Query1Completed:     false,
			Query2Completed:     false,
			Query3Completed:     false,
			Query4Completed:     false,
			AllQueriesCompleted: false,
		}
	}

	clientQueryStatus := rd.clientQueryCompletion[clientID]

	// Mark the specific query as completed
	switch queryType {
	case QueryType2:
		clientQueryStatus.Query2Completed = true
		testing.LogInfo("Results Dispatcher", "✅ Query2 completed for client %s (single chunk)", clientID)
	case QueryType3:
		clientQueryStatus.Query3Completed = true
		testing.LogInfo("Results Dispatcher", "✅ Query3 completed for client %s (single chunk)", clientID)
	case QueryType4:
		clientQueryStatus.Query4Completed = true
		testing.LogInfo("Results Dispatcher", "✅ Query4 completed for client %s (single chunk)", clientID)
	}

	// Check if all queries are completed
	if clientQueryStatus.Query1Completed && clientQueryStatus.Query2Completed &&
		clientQueryStatus.Query3Completed && clientQueryStatus.Query4Completed {

		if !clientQueryStatus.AllQueriesCompleted {
			clientQueryStatus.AllQueriesCompleted = true
			rd.sendSystemCompleteMessage(clientID)
		}
	}
}

// processChunkNotification processes chunk notifications for completion tracking (Query1 only)
func (rd *ResultsDispatcherWorker) processChunkNotification(notification *signals.ChunkNotification, queryType int) middleware.MessageMiddlewareError {
	// Only handle Query1 with completion tracker
	if queryType == QueryType1 {
		if err := rd.query1Tracker.ProcessChunkNotification(notification); err != nil {
			testing.LogError("Results Dispatcher", "Failed to process Query1 chunk notification: %v", err)
			return middleware.MessageMiddlewareMessageError
		}
		return middleware.MessageMiddlewareError(0)
	}

	// This should not happen since we only call this for Query1
	testing.LogError("Results Dispatcher", "processChunkNotification called for non-Query1: %d", queryType)
	return middleware.MessageMiddlewareMessageError
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
