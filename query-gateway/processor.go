package main

import (
	"fmt"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// processMessage processes incoming messages and routes them to join workers
func (qg *QueryGateway) processMessage(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {

	// Check if already processed
	if qg.messageManager.IsProcessed(chunkMsg.ClientID, chunkMsg.ID) {
		fmt.Printf("Query Gateway: Chunk ID %s already processed, skipping\n", chunkMsg.ID)
		return 0 // Return 0 to ACK (handled by createCallback)
	}

	// Route chunk to appropriate destination based on query type
	var routeErr middleware.MessageMiddlewareError
	switch chunkMsg.QueryType {
	case 1:
		// Query 1: Send to Query1 results queue for results dispatcher
		routeErr = qg.sendToQuery1Results(chunkMsg)
		if routeErr != 0 {
			fmt.Printf("Query Gateway: Failed to send chunk to Query1 results queue: %v\n", routeErr)
			return routeErr
		}
		fmt.Printf("Query Gateway: Routed Query 1 chunk to results queue - ClientID: %s, FileID: %s, ChunkNumber: %d\n",
			chunkMsg.ClientID, chunkMsg.FileID, chunkMsg.ChunkNumber)
	case 2:
		// Query 2: Send to Query 2 GroupBy (MapReduce)
		routeErr = qg.sendToQuery2GroupBy(chunkMsg)
		if routeErr != 0 {
			fmt.Printf("Query Gateway: Failed to send chunk to Query 2 GroupBy: %v\n", routeErr)
			return routeErr
		}
		fmt.Printf("Query Gateway: Routed Query 2 chunk to Query 2 GroupBy - ClientID: %s, FileID: %s, ChunkNumber: %d\n",
			chunkMsg.ClientID, chunkMsg.FileID, chunkMsg.ChunkNumber)
	case 3:
		// Query 3: Send to Query 3 GroupBy (MapReduce)
		routeErr = qg.sendToQuery3GroupBy(chunkMsg)
		if routeErr != 0 {
			fmt.Printf("Query Gateway: Failed to send chunk to Query 3 GroupBy: %v\n", routeErr)
			return routeErr
		}
		fmt.Printf("Query Gateway: Routed Query 3 chunk to Query 3 GroupBy - ClientID: %s, FileID: %s, ChunkNumber: %d\n",
			chunkMsg.ClientID, chunkMsg.FileID, chunkMsg.ChunkNumber)
	case 4:
		// Query 4: Send to Query 4 GroupBy (MapReduce)
		routeErr = qg.sendToQuery4GroupBy(chunkMsg)
		if routeErr != 0 {
			fmt.Printf("Query Gateway: Failed to send chunk to Query 4 GroupBy: %v\n", routeErr)
			return routeErr
		}
		fmt.Printf("Query Gateway: Routed Query 4 chunk to Query 4 GroupBy - ClientID: %s, FileID: %s, ChunkNumber: %d\n",
			chunkMsg.ClientID, chunkMsg.FileID, chunkMsg.ChunkNumber)
	default:
		fmt.Printf("Query Gateway: Unknown query type %d, printing result\n", chunkMsg.QueryType)
		qg.printResult(chunkMsg)
		// For unknown query types, we still mark as processed to avoid infinite loops
		routeErr = 0
	}

	// Mark as processed (must be after successful routing)
	if routeErr == 0 {
		if err := qg.messageManager.MarkProcessed(chunkMsg.ClientID, chunkMsg.ID); err != nil {
			fmt.Printf("Query Gateway: Failed to mark chunk as processed: %v\n", err)
			return middleware.MessageMiddlewareMessageError // Will NACK and requeue
		}
	}

	return routeErr
}

// printResult prints the chunk data in a formatted way with ClientID (same format as results dispatcher)
func (qg *QueryGateway) printResult(chunkData *chunk.Chunk) {
	// Split the CSV data into individual rows and print each one
	rows := strings.Split(strings.TrimSpace(chunkData.ChunkData), "\n")
	for _, row := range rows {
		if strings.TrimSpace(row) != "" { // Skip empty rows
			fmt.Printf("%s | Q%d | %s\n", chunkData.ClientID, chunkData.QueryType, row)
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

// sendToQuery2GroupBy sends a chunk message to the Query 2 GroupBy worker (MapReduce)
func (qg *QueryGateway) sendToQuery2GroupBy(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Create a chunk message for serialization
	chunkMessage := chunk.NewChunkMessage(chunkMsg)

	// Serialize the chunk message
	messageData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		fmt.Printf("Query Gateway: Failed to serialize chunk message for Query 2 GroupBy: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send to Query 2 GroupBy queue (query2-map-queue)
	return qg.query2GroupByProducer.Send(messageData)
}

// sendToQuery3GroupBy sends a chunk message to the Query 3 GroupBy worker (MapReduce)
func (qg *QueryGateway) sendToQuery3GroupBy(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Create a chunk message for serialization
	chunkMessage := chunk.NewChunkMessage(chunkMsg)

	// Serialize the chunk message
	messageData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		fmt.Printf("Query Gateway: Failed to serialize chunk message for Query 3 GroupBy: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send to Query 3 GroupBy queue (query3-map-queue)
	return qg.query3GroupByProducer.Send(messageData)
}

// sendToQuery4GroupBy sends a chunk message to the Query 4 GroupBy worker (MapReduce)
func (qg *QueryGateway) sendToQuery4GroupBy(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Create a chunk message for serialization
	chunkMessage := chunk.NewChunkMessage(chunkMsg)

	// Serialize the chunk message
	messageData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		fmt.Printf("Query Gateway: Failed to serialize chunk message for Query 4 GroupBy: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send to Query 4 GroupBy queue (query4-map-queue)
	return qg.query4GroupByProducer.Send(messageData)
}
