package client_request_handler

import (
	"log"
	"os"
	"path/filepath"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/proxy/network"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
)

// ResultsHandler handles results from the results dispatcher
type ResultsHandler struct {
	connectionManager  *network.ConnectionManager
	messageManager     *messagemanager.MessageManager
	completionExchange *exchange.ExchangeMiddleware
}

// NewResultsHandler creates a new results handler
func NewResultsHandler(connectionManager *network.ConnectionManager, completionExchange *exchange.ExchangeMiddleware) *ResultsHandler {
	// Ensure worker data directory exists
	stateDir := "/app/worker-data"
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		log.Printf("Results Handler: Warning - failed to create state directory: %v", err)
	}

	// Initialize MessageManager for duplicate detection
	processedChunksPath := filepath.Join(stateDir, "processed-results.txt")
	messageManager := messagemanager.NewMessageManager(processedChunksPath)
	if err := messageManager.LoadProcessedIDs(); err != nil {
		log.Printf("Results Handler: Warning - failed to load processed results: %v (starting with empty state)", err)
	} else {
		count := messageManager.GetProcessedCount()
		log.Printf("Results Handler: Loaded %d processed result chunks", count)
	}

	return &ResultsHandler{
		connectionManager:  connectionManager,
		messageManager:     messageManager,
		completionExchange: completionExchange,
	}
}

// HandleCompletionSignal handles a client completion signal
func (h *ResultsHandler) HandleCompletionSignal(signal *signals.ClientCompletionSignal) {
	log.Printf("Results Handler: Received completion signal for client %s: %s",
		signal.ClientID, signal.Message)

	// Close the connection for this client
	h.connectionManager.Close(signal.ClientID)

	// Publish the completion signal to the fanout exchange to notify all workers
	if h.completionExchange != nil {
		// Serialize the signal
		serializedSignal, err := signals.SerializeClientCompletionSignal(signal)
		if err != nil {
			log.Printf("Results Handler: Failed to serialize completion signal for client %s: %v", signal.ClientID, err)
			return
		}

		// Publish to fanout exchange (empty routing keys for fanout)
		if err := h.completionExchange.Send(serializedSignal, []string{}); err != 0 {
			log.Printf("Results Handler: Failed to publish completion signal to exchange for client %s: %v", signal.ClientID, err)
			return
		}

		log.Printf("Results Handler: Published completion signal to cleanup exchange for client %s", signal.ClientID)
	} else {
		log.Printf("Results Handler: Warning - completion exchange not initialized, skipping signal broadcast")
	}
}

// HandleChunkData handles chunk data for a client
func (h *ResultsHandler) HandleChunkData(chunkData *chunk.Chunk) {
	// Check if chunk was already processed (duplicate detection)
	if h.messageManager.IsProcessed(chunkData.ClientID, chunkData.ID) {
		log.Printf("Results Handler: Chunk %s already processed, skipping", chunkData.ID)
		return
	}

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
		return // Don't mark as processed if send failed
	}

	// Mark chunk as processed after successful send
	if err := h.messageManager.MarkProcessed(chunkData.ClientID, chunkData.ID); err != nil {
		log.Printf("Results Handler: Failed to mark chunk as processed: %v", err)
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

// Close performs cleanup for the results handler
func (h *ResultsHandler) Close() {
	if h.messageManager != nil {
		h.messageManager.Close()
	}
}
