package main

import (
	"fmt"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// processMessage processes incoming messages and routes them to join workers
func (qg *QueryGateway) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("Query Gateway: Failed to deserialize chunk message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Route chunk to appropriate destination based on query type
	switch chunkMsg.QueryType {
	case 1:
		// Query 1: Send to Query1 results queue for streaming service
		if err := qg.sendToQuery1Results(chunkMsg); err != 0 {
			fmt.Printf("Query Gateway: Failed to send chunk to Query1 results queue: %v\n", err)
			return err
		}
		fmt.Printf("Query Gateway: Routed Query 1 chunk to results queue - ClientID: %s, FileID: %s, ChunkNumber: %d\n",
			chunkMsg.ClientID, chunkMsg.FileID, chunkMsg.ChunkNumber)
	case 2:
		// Query 2: Send to ItemID GroupBy (MapReduce)
		if err := qg.sendToItemIdGroupBy(chunkMsg); err != 0 {
			fmt.Printf("Query Gateway: Failed to send chunk to ItemID GroupBy worker: %v\n", err)
			return err
		}
		fmt.Printf("Query Gateway: Routed Query 2 chunk to ItemID GroupBy worker - ClientID: %s, FileID: %s, ChunkNumber: %d\n",
			chunkMsg.ClientID, chunkMsg.FileID, chunkMsg.ChunkNumber)
	case 3:
		// Query 3: Send to StoreID GroupBy (Dummy Pass-Through)
		if err := qg.sendToStoreIdGroupBy(chunkMsg); err != 0 {
			fmt.Printf("Query Gateway: Failed to send chunk to StoreID GroupBy worker: %v\n", err)
			return err
		}
		fmt.Printf("Query Gateway: Routed Query 3 chunk to StoreID GroupBy worker - ClientID: %s, FileID: %s, ChunkNumber: %d\n",
			chunkMsg.ClientID, chunkMsg.FileID, chunkMsg.ChunkNumber)
	case 4:
		// Query 4: Send to StoreID GroupBy (Dummy Pass-Through)
		if err := qg.sendToStoreIdGroupBy(chunkMsg); err != 0 {
			fmt.Printf("Query Gateway: Failed to send chunk to StoreID GroupBy worker: %v\n", err)
			return err
		}
		fmt.Printf("Query Gateway: Routed Query 4 chunk to StoreID GroupBy worker - ClientID: %s, FileID: %s, ChunkNumber: %d\n",
			chunkMsg.ClientID, chunkMsg.FileID, chunkMsg.ChunkNumber)
	default:
		fmt.Printf("Query Gateway: Unknown query type %d, printing result\n", chunkMsg.QueryType)
		qg.printResult(chunkMsg)
	}

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

// sendToQuery1Results sends a chunk message to the Query1 results queue
func (qg *QueryGateway) sendToQuery1Results(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Create a chunk message for serialization
	chunkMessage := chunk.NewChunkMessage(chunkMsg)

	// Serialize the chunk message
	messageData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		fmt.Printf("Query Gateway: Failed to serialize chunk message for Query1 results: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send to Query1 results queue
	return qg.query1ResultsProducer.Send(messageData)
}

// sendToItemIdGroupBy sends a chunk message to the ItemID GroupBy worker (Query 2 MapReduce)
func (qg *QueryGateway) sendToItemIdGroupBy(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Create a chunk message for serialization
	chunkMessage := chunk.NewChunkMessage(chunkMsg)

	// Serialize the chunk message
	messageData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		fmt.Printf("Query Gateway: Failed to serialize chunk message for ItemID GroupBy: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send to ItemID GroupBy queue
	return qg.itemIdGroupByProducer.Send(messageData)
}

// sendToStoreIdGroupBy sends a chunk message to the StoreID GroupBy worker (Query 3/4 Dummy Pass-Through)
func (qg *QueryGateway) sendToStoreIdGroupBy(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Create a chunk message for serialization
	chunkMessage := chunk.NewChunkMessage(chunkMsg)

	// Serialize the chunk message
	messageData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		fmt.Printf("Query Gateway: Failed to serialize chunk message for StoreID GroupBy: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send to StoreID GroupBy queue
	return qg.storeIdGroupByProducer.Send(messageData)
}
