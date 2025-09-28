package controllers

import (
	"fmt"
	"log"

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
	// Extract the message content
	message := string(delivery.Body)

	// Log the received request
	log.Printf("Client Request Handler: Received request: %s", message)

	// Create acknowledgment response
	response := fmt.Sprintf("ACK: Request received - %s", message)

	// Get the reply-to queue from the message properties
	replyTo := delivery.ReplyTo
	if replyTo == "" {
		log.Printf("Client Request Handler: No reply-to queue specified, cannot send response")
		return fmt.Errorf("no reply-to queue specified")
	}

	// Send acknowledgment response back to client
	err := publishFunc(replyTo, response, "")
	if err != nil {
		log.Printf("Client Request Handler: Failed to send acknowledgment: %v", err)
		return fmt.Errorf("failed to send acknowledgment: %v", err)
	}

	log.Printf("Client Request Handler: Sent acknowledgment: %s", response)

	// Acknowledge the original message
	err = delivery.Ack(false)
	if err != nil {
		log.Printf("Client Request Handler: Failed to acknowledge message: %v", err)
		return fmt.Errorf("failed to acknowledge message: %v", err)
	}

	return nil
}

// Close performs any necessary cleanup
func (h *ClientRequestHandler) Close() {
	log.Printf("Client Request Handler: Closing handler")
	// Add any cleanup logic here if needed
}
