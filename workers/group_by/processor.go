package main

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// processMessage processes a single message
func (gbws *GroupByWorkerService) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	fmt.Printf("GroupBy Worker Service: Received message: %s\n", string(delivery.Body))

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("GroupBy Worker Service: Failed to deserialize chunk message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Process the chunk using the distributed orchestrator
	fmt.Printf("GroupBy Worker Service: Processing chunk - QueryType: %d, Step: %d, ClientID: %s, ChunkNumber: %d\n",
		chunkMsg.QueryType, chunkMsg.Step, chunkMsg.ClientID, chunkMsg.ChunkNumber)

	// Send chunk to the distributed orchestrator for processing
	gbws.orchestrator.ProcessChunk(chunkMsg)

	// Listen for the final result from the orchestrator
	go gbws.waitForResult(chunkMsg)

	return 0
}

// waitForResult waits for the final result from the distributed orchestrator
func (gbws *GroupByWorkerService) waitForResult(originalChunk *chunk.Chunk) {
	fmt.Printf("GroupBy Worker Service: Waiting for result for ClientID: %s, ChunkNumber: %d\n",
		originalChunk.ClientID, originalChunk.ChunkNumber)

	// Listen for results from the orchestrator
	for resultChunk := range gbws.orchestrator.GetResultChannel() {
		// Check if this result matches our original chunk
		if resultChunk.ClientID == originalChunk.ClientID && resultChunk.QueryType == originalChunk.QueryType {
			fmt.Printf("GroupBy Worker Service: Received final result for ClientID: %s\n", resultChunk.ClientID)

			// Send the result back to the query orchestrator
			gbws.sendReply(resultChunk)
			break
		}
	}
}

// sendReply sends a processed chunk as a reply back to the orchestrator
func (gbws *GroupByWorkerService) sendReply(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Create a chunk message for serialization
	chunkMessage := chunk.NewChunkMessage(chunkMsg)

	// Serialize the chunk message for reply
	replyData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		fmt.Printf("GroupBy Worker Service: Failed to serialize reply message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send the reply to the orchestrator reply queue
	if err := gbws.replyProducer.Send(replyData); err != 0 {
		fmt.Printf("GroupBy Worker Service: Failed to send reply to orchestrator: %v\n", err)
		return err
	}

	fmt.Printf("GroupBy Worker Service: Reply sent successfully for ClientID: %s, ChunkNumber: %d\n",
		chunkMsg.ClientID, chunkMsg.ChunkNumber)
	return 0
}
