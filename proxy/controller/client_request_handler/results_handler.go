package client_request_handler

import (
	"log"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/proxy/network"
)

// ResultsHandler handles results from the results dispatcher
type ResultsHandler struct {
	connectionManager *network.ConnectionManager
}

// NewResultsHandler creates a new results handler
func NewResultsHandler(connectionManager *network.ConnectionManager) *ResultsHandler {
	return &ResultsHandler{
		connectionManager: connectionManager,
	}
}

// HandleCompletionSignal handles a client completion signal
func (h *ResultsHandler) HandleCompletionSignal(signal *signals.ClientCompletionSignal) {
	log.Printf("Results Handler: Received completion signal for client %s: %s",
		signal.ClientID, signal.Message)

	// Close the connection for this client
	h.connectionManager.Close(signal.ClientID)
}

// HandleChunkData handles chunk data for a client
func (h *ResultsHandler) HandleChunkData(chunkData *chunk.Chunk) {
	// Get connection for this client
	conn, exists := h.connectionManager.Get(chunkData.ClientID)

	if !exists {
		log.Printf("Results Handler: No active connection found for client %s", chunkData.ClientID)
		return
	}

	// Send formatted data to the client
	_, err := conn.Write([]byte(chunkData.ChunkData))
	if err != nil {
		log.Printf("Results Handler: Failed to send data to client %s: %v", chunkData.ClientID, err)
		// Remove the connection if it's no longer valid
		h.connectionManager.RemoveByID(chunkData.ClientID)
	}
}

// ProcessMessage processes a message from the results consumer
func (h *ResultsHandler) ProcessMessage(messageData []byte) {
	// Deserialize the message to determine its type
	message, err := deserializer.Deserialize(messageData)
	if err != nil {
		log.Printf("Results Handler: Failed to deserialize message: %v", err)
		return
	}

	// Handle different message types
	switch msg := message.(type) {
	case *signals.ClientCompletionSignal:
		h.HandleCompletionSignal(msg)
	case *chunk.Chunk:
		h.HandleChunkData(msg)
	default:
		log.Printf("Results Handler: Unknown message type: %T", message)
	}
}

