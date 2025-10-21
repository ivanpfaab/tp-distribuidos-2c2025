package main

import (
	"fmt"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// processMessage processes incoming messages and sends formatted data to client
func (sw *StreamingWorker) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	// Deserialize the chunk message
	chunkFromMsg, err := deserializer.Deserialize(delivery.Body)
	if err != nil {
		testing.LogError("Streaming Worker", "Failed to deserialize chunk message: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	chunkData, ok := chunkFromMsg.(*chunk.Chunk)
	if !ok {
		testing.LogError("Streaming Worker", "Failed to cast message to chunk")
		return middleware.MessageMiddlewareMessageError
	}

	// Send formatted data to client instead of printing
	return sw.sendFormattedDataToClient(chunkData)
}

// sendFormattedDataToClient formats the chunk data and sends it to client request handler
func (sw *StreamingWorker) sendFormattedDataToClient(chunkData *chunk.Chunk) middleware.MessageMiddlewareError {
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
		testing.LogError("Streaming Worker", "Failed to serialize chunk: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	return sw.clientResultsProducer.Send(serializedData)
}
