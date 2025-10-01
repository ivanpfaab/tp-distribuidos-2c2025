package handlers

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// MessageHandler handles message processing logic
type MessageHandler struct {
	targetRouter *TargetRouter
	producers    ProducerInterface
}

// ProducerInterface defines the interface for producers
type ProducerInterface interface {
	SendToFilter(data []byte) middleware.MessageMiddlewareError
	SendToAggregator(data []byte) middleware.MessageMiddlewareError
	SendToJoin(data []byte) middleware.MessageMiddlewareError
	SendToGroupBy(data []byte) middleware.MessageMiddlewareError
	SendToStreaming(data []byte) middleware.MessageMiddlewareError
}

// NewMessageHandler creates a new MessageHandler instance
func NewMessageHandler(producers ProducerInterface) *MessageHandler {
	return &MessageHandler{
		targetRouter: NewTargetRouter(),
		producers:    producers,
	}
}

// ProcessChunk routes a chunk message to the appropriate node based on QueryType and Step
func (mh *MessageHandler) ProcessChunk(rawChunk *chunk.Chunk) middleware.MessageMiddlewareError {
	fmt.Println("Processing chunk: ", rawChunk)

	// Determine target based on QueryType and Step
	target := mh.targetRouter.DetermineTarget(rawChunk.QueryType, rawChunk.Step)

	fmt.Println("Target: ", target)

	chunkMsg := chunk.NewChunkMessage(rawChunk)

	// Serialize the chunk message
	messageData, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
		return middleware.MessageMiddlewareMessageError
	}

	// Route to appropriate producer
	switch target {
	case "filter":
		return mh.producers.SendToFilter(messageData)
	case "aggregator":
		return mh.producers.SendToAggregator(messageData)
	case "join":
		return mh.producers.SendToJoin(messageData)
	case "groupby":
		return mh.producers.SendToGroupBy(messageData)
	case "streaming":
		return mh.producers.SendToStreaming(messageData)
	default:
		return middleware.MessageMiddlewareMessageError
	}
}

// QueryOrchestratorCallback processes incoming chunk messages from the data handler
func (mh *MessageHandler) QueryOrchestratorCallback(consumeChannel middleware.ConsumeChannel, done chan error) {
	fmt.Println("Query Orchestrator: Callback function called!")
	fmt.Println("Query Orchestrator: Starting to listen for messages...")
	for delivery := range *consumeChannel {
		fmt.Printf("Query Orchestrator: Received message: %s\n", string(delivery.Body))

		// Deserialize the chunk message
		chunkFromMsg, err := deserializer.Deserialize(delivery.Body)
		if err != nil {
			fmt.Printf("Query Orchestrator: Failed to deserialize chunk message: %v\n", err)
			delivery.Nack(false, true) // Reject and requeue
			continue
		}

		advancedChunk := chunk.AdvanceChunkStep(chunkFromMsg.(*chunk.Chunk))

		// Process the chunk (route to appropriate worker)
		if err := mh.ProcessChunk(advancedChunk); err != 0 {
			fmt.Printf("Query Orchestrator: Failed to process chunk: %v\n", err)
			delivery.Nack(false, true) // Reject and requeue
			continue
		}

		// Acknowledge the message
		delivery.Ack(false)
	}
	done <- nil
}
