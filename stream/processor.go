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

// processMessage processes incoming messages and prints the results
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

	// Print the result
	sw.printResult(chunkData)

	return 0
}

// printResult prints the chunk data in a formatted way
func (sw *StreamingWorker) printResult(chunkData *chunk.Chunk) {


	// Split the CSV data into individual rows and print each one
	rows := strings.Split(strings.TrimSpace(chunkData.ChunkData), "\n")
	for _, row := range rows {
		if strings.TrimSpace(row) != "" { // Skip empty rows
			fmt.Printf("Q%d | %s\n", chunkData.QueryType, row)
		}
	}
}
