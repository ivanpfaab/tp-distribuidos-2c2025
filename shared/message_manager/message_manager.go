package messagemanager

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	completedClientsFile = "/app/worker-data/completed-clients.txt"
)

// MessageManager manages processed message IDs using per-client persistent files
type MessageManager struct {
	baseDir          string                     // Base directory for client-specific files
	processedIDs     map[string]map[string]bool // clientID -> messageID -> bool
	completedClients map[string]bool            // clientID -> completed (loaded from disk)
}

// NewMessageManager creates a new MessageManager instance
// filePath should be a directory path where per-client files will be stored
// Files will be named: processed-ids-{clientID}.txt
func NewMessageManager(filePath string) *MessageManager {
	// Load completed clients from disk
	completedClients := make(map[string]bool)
	if err := loadCompletedClients(completedClients); err != nil {
		// On error, start with empty map (fail open)
		completedClients = make(map[string]bool)
	}

	return &MessageManager{
		baseDir:          filePath,
		processedIDs:     make(map[string]map[string]bool),
		completedClients: completedClients,
	}
}

// getClientFilePath returns the file path for a specific client
func (mm *MessageManager) getClientFilePath(clientID string) string {
	// If baseDir ends with .txt, treat it as old format and extract directory
	// Otherwise, use baseDir as directory
	dir := mm.baseDir
	if strings.HasSuffix(mm.baseDir, ".txt") {
		dir = filepath.Dir(mm.baseDir)
	}

	// Ensure directory exists
	os.MkdirAll(dir, 0755)

	return filepath.Join(dir, fmt.Sprintf("processed-ids-%s.txt", clientID))
}

// loadClientProcessedIDs loads processed IDs for a specific client (lazy loading)
func (mm *MessageManager) loadClientProcessedIDs(clientID string) error {
	// Check if already loaded
	if mm.processedIDs[clientID] != nil {
		return nil
	}

	// Initialize map for this client
	mm.processedIDs[clientID] = make(map[string]bool)

	// Load from file
	filePath := mm.getClientFilePath(clientID)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist yet (first time for this client), return nil
			return nil
		}
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			mm.processedIDs[clientID][line] = true
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	return nil
}
// IsProcessed checks if an ID has already been processed for a specific client
// Returns true if the client is completed OR if the specific ID was processed
func (mm *MessageManager) IsProcessed(clientID string, id string) bool {
	// First check if client is completed - if so, treat all messages as processed
	if mm.completedClients[clientID] {
		return true
	}

	// Load client's processed IDs if not already loaded
	if err := mm.loadClientProcessedIDs(clientID); err != nil {
		// On error, assume not processed (fail open)
		return false
	}

	clientIDs, exists := mm.processedIDs[clientID]
	if !exists {
		return false
	}

	return clientIDs[id]
}

// MarkProcessed marks an ID as processed for a specific client and appends it to the client's file
func (mm *MessageManager) MarkProcessed(clientID string, id string) error {
	// Load client's processed IDs if not already loaded
	if err := mm.loadClientProcessedIDs(clientID); err != nil {
		// Initialize if load failed (new client)
		mm.processedIDs[clientID] = make(map[string]bool)
	}

	// Add to memory
	mm.processedIDs[clientID][id] = true

	// Append to client-specific file
	filePath := mm.getClientFilePath(clientID)
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file for writing: %w", err)
	}
	defer file.Close()

	// Prepare data: ID + newline
	data := []byte(id + "\n")

	// Write with loop to handle short writes
	totalWritten := 0
	for totalWritten < len(data) {
		n, err := file.Write(data[totalWritten:])
		if err != nil {
			return fmt.Errorf("failed to write to file: %w", err)
		}
		totalWritten += n
	}

	// Sync to ensure data is written to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// CleanClient removes all processed IDs for a specific client (idempotent)
func (mm *MessageManager) CleanClient(clientID string) error {
	// Remove from memory
	delete(mm.processedIDs, clientID)

	// Delete client-specific file (idempotent - no error if file doesn't exist)
	filePath := mm.getClientFilePath(clientID)
	err := os.Remove(filePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete client file: %w", err)
	}

	return nil
}

// Close performs cleanup (currently no-op, but kept for future use)
func (mm *MessageManager) Close() error {
	return nil
}

// loadCompletedClients loads completed client IDs from disk
func loadCompletedClients(completedClients map[string]bool) error {
	file, err := os.Open(completedClientsFile)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist (first startup), return nil
			return nil
		}
		return fmt.Errorf("failed to open completed clients file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			completedClients[line] = true
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read completed clients file: %w", err)
	}

	return nil
}

// GetProcessedCount returns the total number of processed IDs across all clients
func (mm *MessageManager) GetProcessedCount() int {
	total := 0
	for _, clientIDs := range mm.processedIDs {
		total += len(clientIDs)
	}
	return total
}
