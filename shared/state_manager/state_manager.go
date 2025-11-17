package statemanager

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// StateManager manages processed message IDs using a persistent file
type StateManager struct {
	filePath     string
	processedIDs map[string]bool
}

// NewStateManager creates a new StateManager instance
func NewStateManager(filePath string) *StateManager {
	return &StateManager{
		filePath:     filePath,
		processedIDs: make(map[string]bool),
	}
}

// LoadProcessedIDs reads the file and loads all processed IDs into memory
func (sm *StateManager) LoadProcessedIDs() error {
	file, err := os.Open(sm.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist yet (first run), return nil
			return nil
		}
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			sm.processedIDs[line] = true
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	return nil
}

// IsProcessed checks if an ID has already been processed
func (sm *StateManager) IsProcessed(id string) bool {
	return sm.processedIDs[id]
}

// MarkProcessed marks an ID as processed and appends it to the file
func (sm *StateManager) MarkProcessed(id string) error {
	// Add to memory
	sm.processedIDs[id] = true

	// Append to file
	file, err := os.OpenFile(sm.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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

// Close performs cleanup (currently no-op, but kept for future use)
func (sm *StateManager) Close() error {
	return nil
}

// GetProcessedCount returns the number of processed IDs
func (sm *StateManager) GetProcessedCount() int {
	return len(sm.processedIDs)
}
