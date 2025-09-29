package controllers

import (
	"fmt"
	"log"
	"net"

	"github.com/tp-distribuidos-2c2025/protocol/batch"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	datahandler "github.com/tp-distribuidos-2c2025/server/controller/data-handler"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// ClientRequestHandler handles incoming client requests
type ClientRequestHandler struct {
	config *middleware.ConnectionConfig
}

// NewClientRequestHandler creates a new instance of ClientRequestHandler
func NewClientRequestHandler(config *middleware.ConnectionConfig) *ClientRequestHandler {
	return &ClientRequestHandler{
		config: config,
	}
}

// HandleConnection handles a TCP connection and creates a data handler for it
func (h *ClientRequestHandler) HandleConnection(conn net.Conn) {
	defer conn.Close()

	log.Printf("Client Request Handler: New connection from %s", conn.RemoteAddr())

	// Create a data handler for this connection
	dataHandler := datahandler.NewDataHandlerForConnection(conn, h.config)

	// Start the data handler in a goroutine
	go dataHandler.Start()

	// Keep the connection alive and process messages
	buffer := make([]byte, 4096)
	for {
		// Read data from client
		n, err := conn.Read(buffer)
		if err != nil {
			log.Printf("Client Request Handler: Connection %s closed: %v", conn.RemoteAddr(), err)
			break
		}

		// Process the batch message directly
		data := buffer[:n]
		response, err := h.processBatchMessage(data, dataHandler)
		if err != nil {
			log.Printf("Client Request Handler: Failed to process message from %s: %v", conn.RemoteAddr(), err)
			response = []byte("ERROR: " + err.Error())
		}

		// Send response back to client
		_, err = conn.Write(response)
		if err != nil {
			log.Printf("Client Request Handler: Failed to send response to %s: %v", conn.RemoteAddr(), err)
			break
		}

		log.Printf("Client Request Handler: Sent response to %s: %s", conn.RemoteAddr(), string(response))
	}
}

// processBatchMessage processes a batch message and returns a response
func (h *ClientRequestHandler) processBatchMessage(data []byte, dataHandler *datahandler.DataHandler) ([]byte, error) {
	// Deserialize the message
	message, err := deserializer.Deserialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %w", err)
	}

	// Check if it's a Batch message (only type we handle)
	batchMsg, ok := message.(*batch.Batch)
	if !ok {
		return nil, fmt.Errorf("expected batch message, got %T", message)
	}

	// Log the received batch
	log.Printf("Client Request Handler: Received batch - ClientID: %s, FileID: %s, BatchNumber: %d, Data: %s",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber, batchMsg.BatchData)

	// Process the batch message directly with the data handler
	if err := dataHandler.ProcessBatchMessage(data); err != nil {
		log.Printf("Client Request Handler: Failed to process batch with data handler: %v", err)
		return nil, fmt.Errorf("failed to process batch with data handler: %w", err)
	}

	log.Printf("Client Request Handler: Successfully processed batch - ClientID: %s, FileID: %s, BatchNumber: %d",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber)

	// Create acknowledgment response
	response := fmt.Sprintf("ACK: Batch received - ClientID: %s, FileID: %s, BatchNumber: %d",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber)

	return []byte(response), nil
}

// Close performs any necessary cleanup
func (h *ClientRequestHandler) Close() {
	log.Printf("Client Request Handler: Closing handler")
	// Add any cleanup logic here if needed
}
