package client_request_handler

import (
	"bufio"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/proxy/network"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
)

const (
	completedClientsFile = "/app/worker-data/completed-clients.txt"
)

// ResultsHandler handles results from the results dispatcher
type ResultsHandler struct {
	connectionManager  *network.ConnectionManager
	messageManager     *messagemanager.MessageManager
	completionExchange *exchange.ExchangeMiddleware
	completedClients   map[string]bool // clientID -> completed
	completedMutex     sync.RWMutex    // Mutex for thread-safe access
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

	// Load completed clients from disk
	completedClients := make(map[string]bool)
	if err := loadCompletedClients(completedClients); err != nil {
		log.Printf("Results Handler: Warning - failed to load completed clients: %v (starting with empty state)", err)
	} else {
		count := len(completedClients)
		if count > 0 {
			log.Printf("Results Handler: Loaded %d completed client(s) from disk", count)
		}
	}

	return &ResultsHandler{
		connectionManager:  connectionManager,
		messageManager:     messageManager,
		completionExchange: completionExchange,
		completedClients:   completedClients,
	}
}

// HandleCompletionSignal handles a client completion signal
func (h *ResultsHandler) HandleCompletionSignal(signal *signals.ClientCompletionSignal) {
	log.Printf("Results Handler: Received completion signal for client %s: %s",
		signal.ClientID, signal.Message)

	// Mark client as completed
	h.MarkClientCompleted(signal.ClientID)

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

		if err := h.messageManager.CleanClient(signal.ClientID); err != nil {
			log.Printf("Results Handler: Failed to clean up processed messages for client %s: %v", signal.ClientID, err)
		}

		log.Printf("Results Handler: Published completion signal to cleanup exchange for client %s", signal.ClientID)
	} else {
		log.Printf("Results Handler: Warning - completion exchange not initialized, skipping signal broadcast")
	}
}

// HandleChunkData handles chunk data for a client
func (h *ResultsHandler) HandleChunkData(chunkData *chunk.Chunk) {
	// Check if client is completed - if so, mark chunk as processed to ACK it
	if h.IsClientCompleted(chunkData.ClientID) {
		// Client is completed, mark chunk as processed to ACK and drain queue
		if err := h.messageManager.MarkProcessed(chunkData.ClientID, chunkData.ID); err != nil {
			log.Printf("Results Handler: Failed to mark chunk as processed for completed client: %v", err)
		}
		return
	}

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

// IsClientCompleted checks if a client has been marked as completed
func (h *ResultsHandler) IsClientCompleted(clientID string) bool {
	h.completedMutex.RLock()
	defer h.completedMutex.RUnlock()
	return h.completedClients[clientID]
}

// MarkClientCompleted marks a client as completed and persists to disk
func (h *ResultsHandler) MarkClientCompleted(clientID string) {
	h.completedMutex.Lock()
	defer h.completedMutex.Unlock()

	// Skip if already marked
	if h.completedClients[clientID] {
		return
	}

	// Mark in memory
	h.completedClients[clientID] = true

	// Persist to disk
	if err := writeCompletedClientToFile(clientID); err != nil {
		log.Printf("Results Handler: Failed to write completed client to file: %v", err)
		return
	}

	log.Printf("Results Handler: Marked client %s as completed", clientID)
}

// loadCompletedClients loads completed client IDs from disk
func loadCompletedClients(completedClients map[string]bool) error {
	file, err := os.Open(completedClientsFile)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist (first startup), return nil
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			completedClients[line] = true
		}
	}

	return scanner.Err()
}

// writeCompletedClientToFile writes a completed client ID to disk (thread-safe via mutex in caller)
func writeCompletedClientToFile(clientID string) error {
	// Ensure directory exists
	dir := filepath.Dir(completedClientsFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Read existing client IDs to check for duplicates
	existingIDs := make(map[string]bool)
	if file, err := os.Open(completedClientsFile); err == nil {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line != "" {
				existingIDs[line] = true
			}
		}
		file.Close()
	}

	// Skip if client ID already exists
	if existingIDs[clientID] {
		return nil
	}

	// Append client ID to file
	file, err := os.OpenFile(completedClientsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.WriteString(clientID + "\n"); err != nil {
		return err
	}

	// Sync to ensure data is written to disk
	return file.Sync()
}

// Close performs cleanup for the results handler
func (h *ResultsHandler) Close() {
	if h.messageManager != nil {
		h.messageManager.Close()
	}
}
