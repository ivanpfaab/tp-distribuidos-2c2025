package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// processMessage processes incoming messages and prints the results
func (sw *StreamingWorker) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	testing.LogInfo("Streaming Worker", "Received message: %s", string(delivery.Body))

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
	fmt.Println("==================================================")
	fmt.Println("STREAMING SERVICE - QUERY RESULT")
	fmt.Println("==================================================")
	fmt.Printf("File ID: %s\n", chunkData.FileID)
	fmt.Printf("Query Type: %d\n", chunkData.QueryType)
	fmt.Printf("Table ID: %d\n", chunkData.TableID)
	fmt.Printf("Chunk Number: %d\n", chunkData.ChunkNumber)
	fmt.Printf("Is Last Chunk: %t\n", chunkData.IsLastChunk)
	fmt.Printf("Step: %d\n", chunkData.Step)
	fmt.Printf("Data: %s\n", chunkData.ChunkData)
	fmt.Println("==================================================")
	fmt.Println()
}
