package main

import (
	"fmt"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// processMessage processes incoming messages and prints the results
func (qg *QueryGateway) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("Query Gateway: Failed to deserialize chunk message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Print the results in the same format as streaming service
	qg.printResult(chunkMsg)

	return 0
}

// printResult prints the chunk data in a formatted way (same format as streaming service)
func (qg *QueryGateway) printResult(chunkData *chunk.Chunk) {
	// Split the CSV data into individual rows and print each one
	rows := strings.Split(strings.TrimSpace(chunkData.ChunkData), "\n")
	for _, row := range rows {
		if strings.TrimSpace(row) != "" { // Skip empty rows
			fmt.Printf("Q%d | %s\n", chunkData.QueryType, row)
		}
	}
}
