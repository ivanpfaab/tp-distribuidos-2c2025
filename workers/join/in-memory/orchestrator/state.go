package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/workers/shared"
)

// StateManager manages the persistence and rebuilding of orchestrator state
type StateManager struct {
	metadataDir       string
	completionTracker *shared.CompletionTracker
}

// NewStateManager creates a new state manager
func NewStateManager(metadataDir string, completionTracker *shared.CompletionTracker) *StateManager {
	return &StateManager{
		metadataDir:       metadataDir,
		completionTracker: completionTracker,
	}
}

// RebuildState rebuilds completion tracker state from CSV metadata files
func (sm *StateManager) RebuildState() error {
	log.Println("Rebuilding state from CSV metadata...")

	// Read all CSV files in metadata directory
	files, err := os.ReadDir(sm.metadataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Directory doesn't exist, nothing to rebuild
		}
		return fmt.Errorf("failed to read metadata directory: %w", err)
	}

	rebuiltCount := 0
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".csv") {
			continue
		}

		// Extract clientID from filename: {clientID}.csv
		clientID := strings.TrimSuffix(file.Name(), ".csv")
		filePath := filepath.Join(sm.metadataDir, file.Name())

		// Read notifications from CSV
		notifications, err := sm.readNotificationsFromCSV(filePath)
		if err != nil {
			log.Printf("Warning: failed to read notifications from %s: %v", filePath, err)
			continue
		}

		if len(notifications) == 0 {
			continue
		}

		// Rebuild state by processing notifications directly into completion tracker
		// Note: These notifications are already in MessageManager, so they won't be processed again
		// during normal operation (duplicate check will skip them)
		for _, notification := range notifications {
			if err := sm.completionTracker.ProcessChunkNotification(notification); err != nil {
				log.Printf("Warning: failed to process notification during rebuild: %v", err)
			}
		}

		// Check if client is completed after rebuilding state
		if sm.completionTracker.IsClientCompleted(clientID) {
			// Client already completed, delete CSV file
			if err := os.Remove(filePath); err != nil {
				log.Printf("Warning: failed to delete metadata file for completed client %s: %v", clientID, err)
			} else {
				log.Printf("Deleted metadata file for already-completed client %s", clientID)
			}
		} else {
			rebuiltCount++
			log.Printf("Rebuilt state for client %s (%d notifications)", clientID, len(notifications))
		}
	}

	log.Printf("State rebuild complete: %d clients rebuilt", rebuiltCount)
	return nil
}

// AppendNotification appends a notification to the client's CSV metadata file
func (sm *StateManager) AppendNotification(notification *signals.ChunkNotification) error {
	csvPath := filepath.Join(sm.metadataDir, notification.ClientID+".csv")

	// Ensure directory exists
	if err := os.MkdirAll(sm.metadataDir, 0755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	file, err := os.OpenFile(csvPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header if file is new
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	if stat.Size() == 0 {
		header := []string{"notification_id", "client_id", "file_id", "table_id", "chunk_number", "is_last_chunk", "is_last_from_table"}
		if err := writer.Write(header); err != nil {
			return fmt.Errorf("failed to write header: %w", err)
		}
	}

	// Write data row
	isLastChunkStr := "0"
	if notification.IsLastChunk {
		isLastChunkStr = "1"
	}
	isLastFromTableStr := "0"
	if notification.IsLastFromTable {
		isLastFromTableStr = "1"
	}

	row := []string{
		notification.ID,
		notification.ClientID,
		notification.FileID,
		strconv.Itoa(notification.TableID),
		strconv.Itoa(notification.ChunkNumber),
		isLastChunkStr,
		isLastFromTableStr,
	}

	if err := writer.Write(row); err != nil {
		return fmt.Errorf("failed to write row: %w", err)
	}

	// Sync to ensure data is written to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// DeleteClientMetadata deletes the CSV metadata file for a completed client
func (sm *StateManager) DeleteClientMetadata(clientID string) error {
	csvPath := filepath.Join(sm.metadataDir, clientID+".csv")
	if err := os.Remove(csvPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete metadata file for client %s: %w", clientID, err)
	}
	return nil
}

// readNotificationsFromCSV reads notifications from a CSV file, handling incomplete last row
func (sm *StateManager) readNotificationsFromCSV(filePath string) ([]*signals.ChunkNotification, error) {
	// Check and fix incomplete last row
	hasIncomplete, err := sm.hasIncompleteLastRow(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to check incomplete row: %w", err)
	}
	if hasIncomplete {
		if err := sm.removeIncompleteLastRow(filePath); err != nil {
			return nil, fmt.Errorf("failed to remove incomplete row: %w", err)
		}
		log.Printf("Fixed incomplete last row in %s", filePath)
	}

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return []*signals.ChunkNotification{}, nil
		}
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	notifications := []*signals.ChunkNotification{}

	// Read header
	header, err := reader.Read()
	if err == io.EOF {
		return notifications, nil
	}
	if err != nil {
		return nil, err
	}

	// Validate header
	expectedHeader := []string{"notification_id", "client_id", "file_id", "table_id", "chunk_number", "is_last_chunk", "is_last_from_table"}
	if len(header) < len(expectedHeader) {
		return nil, fmt.Errorf("invalid CSV header")
	}

	// Read data rows
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if len(row) < len(expectedHeader) {
			continue // Skip malformed rows
		}

		// Parse row
		notificationID := row[0]
		clientID := row[1]
		fileID := row[2]
		tableID, err := strconv.Atoi(row[3])
		if err != nil {
			continue
		}
		chunkNumber, err := strconv.Atoi(row[4])
		if err != nil {
			continue
		}
		isLastChunk := row[5] == "1" || strings.ToLower(row[5]) == "true"
		isLastFromTable := row[6] == "1" || strings.ToLower(row[6]) == "true"

		// Create notification (MapWorkerID not persisted)
		notification := signals.NewChunkNotification(
			clientID,
			fileID,
			"", // MapWorkerID not persisted
			tableID,
			chunkNumber,
			isLastChunk,
			isLastFromTable,
		)
		// Set the ID explicitly (it's computed in NewChunkNotification, but we want the persisted one)
		notification.ID = notificationID

		notifications = append(notifications, notification)
	}

	return notifications, nil
}

// hasIncompleteLastRow checks if the last row in a CSV file is incomplete (missing \n)
func (sm *StateManager) hasIncompleteLastRow(filePath string) (bool, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return false, err
	}

	if stat.Size() == 0 {
		return false, nil
	}

	// Read the last byte to check if file ends with newline
	lastByte := make([]byte, 1)
	_, err = file.ReadAt(lastByte, stat.Size()-1)
	if err != nil {
		return false, err
	}

	return lastByte[0] != '\n', nil
}

// removeIncompleteLastRow removes the incomplete last row by truncating the file
func (sm *StateManager) removeIncompleteLastRow(filePath string) error {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	if stat.Size() == 0 {
		return nil
	}

	// Read file backward in chunks until we find a newline
	const chunkSize = int64(512)
	offset := stat.Size()

	for offset > 0 {
		readSize := chunkSize
		if offset < chunkSize {
			readSize = offset
		}

		readOffset := offset - readSize
		buffer := make([]byte, readSize)
		_, err = file.ReadAt(buffer, readOffset)
		if err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}

		// Search backward for newline
		lastNewline := -1
		for i := len(buffer) - 1; i >= 0; i-- {
			if buffer[i] == '\n' {
				lastNewline = i
				break
			}
		}

		if lastNewline != -1 {
			newSize := readOffset + int64(lastNewline) + 1
			return file.Truncate(newSize)
		}

		offset = readOffset
	}

	// No newline found, truncate to 0
	return file.Truncate(0)
}

