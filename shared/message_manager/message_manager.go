package messagemanager

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// MessageManager manages processed message IDs using a persistent file
type MessageManager struct {
	filePath     string
	processedIDs map[string]bool
}

// NewMessageManager creates a new MessageManager instance
func NewMessageManager(filePath string) *MessageManager {
	return &MessageManager{
		filePath:     filePath,
		processedIDs: make(map[string]bool),
	}
}

// LoadProcessedIDs reads the file and loads all processed IDs into memory
func (mm *MessageManager) LoadProcessedIDs() error {
	file, err := os.Open(mm.filePath)
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
			mm.processedIDs[line] = true
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	return nil
}

// IsProcessed checks if an ID has already been processed
func (mm *MessageManager) IsProcessed(id string) bool {
	return mm.processedIDs[id]
}

// MarkProcessed marks an ID as processed and appends it to the file
func (mm *MessageManager) MarkProcessed(id string) error {
	// Add to memory
	mm.processedIDs[id] = true

	// Append to file
	file, err := os.OpenFile(mm.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
func (mm *MessageManager) Close() error {
	return nil
}

// GetProcessedCount returns the number of processed IDs
func (mm *MessageManager) GetProcessedCount() int {
	return len(mm.processedIDs)
}

