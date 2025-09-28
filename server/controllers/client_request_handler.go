package controllers

import (
	"fmt"
	"log"

	"tp-distribuidos-2c2025/protocol/common"
	"tp-distribuidos-2c2025/protocol/deserializer"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ClientRequestHandler handles incoming client requests
type ClientRequestHandler struct {
	// Add any necessary fields for configuration or state
}

// NewClientRequestHandler creates a new instance of ClientRequestHandler
func NewClientRequestHandler() *ClientRequestHandler {
	return &ClientRequestHandler{}
}

// HandleRequest processes a client request and sends an acknowledgment response
func (h *ClientRequestHandler) HandleRequest(delivery amqp.Delivery, publishFunc func(string, string, string) error) error {
	// Deserialize the message
	message, err := deserializer.Deserialize(delivery.Body)
	if err != nil {
		log.Printf("Client Request Handler: Failed to deserialize message: %v", err)
		delivery.Nack(false, false) // Reject and don't requeue
		return fmt.Errorf("failed to deserialize message: %w", err)
	}

	// Check if it's a Batch message (only type we handle)
	batchMsg, ok := message.(*common.BatchMessageType)
	if !ok {
		log.Printf("Client Request Handler: Received non-batch message type: %T", message)
		delivery.Nack(false, false) // Reject and don't requeue
		return fmt.Errorf("expected batch message, got %T", message)
	}

	// Log the received batch
	log.Printf("Client Request Handler: Received batch - ClientID: %s, FileID: %s, BatchNumber: %d, Data: %s",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber, batchMsg.BatchData)

	// Create acknowledgment response
	response := fmt.Sprintf("ACK: Batch received - ClientID: %s, FileID: %s, BatchNumber: %d",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber)

	// Get the reply-to queue from the message properties
	replyTo := delivery.ReplyTo
	if replyTo == "" {
		log.Printf("Client Request Handler: No reply-to queue specified, cannot send response")
		delivery.Nack(false, false) // Reject and don't requeue
		return fmt.Errorf("no reply-to queue specified")
	}

	// Send acknowledgment response back to client
	err = publishFunc(replyTo, response, "")
	if err != nil {
		log.Printf("Client Request Handler: Failed to send acknowledgment: %v", err)
		delivery.Nack(false, true) // Reject and requeue
		return fmt.Errorf("failed to send acknowledgment: %w", err)
	}

	log.Printf("Client Request Handler: Sent acknowledgment: %s", response)

	// Acknowledge the original message
	err = delivery.Ack(false)
	if err != nil {
		log.Printf("Client Request Handler: Failed to acknowledge message: %v", err)
		return fmt.Errorf("failed to acknowledge message: %w", err)
	}

	return nil
}

// Close performs any necessary cleanup
func (h *ClientRequestHandler) Close() {
	log.Printf("Client Request Handler: Closing handler")
	// Add any cleanup logic here if needed
}
