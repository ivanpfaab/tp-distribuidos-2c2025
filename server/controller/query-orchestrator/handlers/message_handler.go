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

// DataHandlerCallback processes incoming chunk messages from the data handler
func (mh *MessageHandler) DataHandlerCallback(consumeChannel middleware.ConsumeChannel, done chan error) {
	fmt.Println("Query Orchestrator: Data Handler Callback function called!")
	fmt.Println("Query Orchestrator: Starting to listen for messages from data handler...")
	for delivery := range *consumeChannel {
		fmt.Printf("Query Orchestrator: Received message from data handler: %s\n", string(delivery.Body))

		// Deserialize the chunk message
		chunkFromMsg, err := deserializer.Deserialize(delivery.Body)
		if err != nil {
			fmt.Printf("Query Orchestrator: Failed to deserialize chunk message: %v\n", err)
			delivery.Nack(false, true) // Reject and requeue
			continue
		}

		// Advance step for new chunks from data handler
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

// ReplyCallback processes incoming reply messages from workers
func (mh *MessageHandler) ReplyCallback(consumeChannel middleware.ConsumeChannel, done chan error) {
	fmt.Println("Query Orchestrator: Reply Callback function called!")
	fmt.Println("Query Orchestrator: Starting to listen for replies from workers...")
	for delivery := range *consumeChannel {
		fmt.Printf("Query Orchestrator: Received reply message: %s\n", string(delivery.Body))

		// Deserialize the chunk message
		chunkFromMsg, err := deserializer.Deserialize(delivery.Body)
		if err != nil {
			fmt.Printf("Query Orchestrator: Failed to deserialize reply message: %v\n", err)
			delivery.Nack(false, true) // Reject and requeue
			continue
		}

		// Process the reply (route to next step or streaming)
		if err := mh.ProcessReply(chunkFromMsg.(*chunk.Chunk)); err != 0 {
			fmt.Printf("Query Orchestrator: Failed to process reply: %v\n", err)
			delivery.Nack(false, true) // Reject and requeue
			continue
		}

		// Acknowledge the message
		delivery.Ack(false)
	}
	done <- nil
}

// ProcessReply routes a reply message to the next step in the query pipeline
func (mh *MessageHandler) ProcessReply(replyChunk *chunk.Chunk) middleware.MessageMiddlewareError {
	fmt.Println("Processing reply: ", replyChunk)

	// Determine next target based on QueryType and current Step
	nextTarget := mh.targetRouter.DetermineNextTarget(replyChunk.QueryType, replyChunk.Step)

	fmt.Println("Next target: ", nextTarget)

	// If no next target, send to streaming (final step)
	if nextTarget == "unknown" || nextTarget == "" {
		nextTarget = "streaming"
	}

	// Advance step for the reply to move to next stage
	advancedChunk := chunk.AdvanceChunkStep(replyChunk)

	chunkMsg := chunk.NewChunkMessage(advancedChunk)

	// Serialize the chunk message
	messageData, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
		return middleware.MessageMiddlewareMessageError
	}

	// Route to appropriate producer
	switch nextTarget {
	case "filter":
		return mh.producers.SendToFilter(messageData)
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
