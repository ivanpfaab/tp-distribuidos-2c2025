package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/utils"
	"github.com/tp-distribuidos-2c2025/workers/shared"
)

// StateManager manages the persistence and rebuilding of orchestrator state
type StateManager struct {
	metadataDir       string
	completionTracker *shared.CompletionTracker
	csvHandler        *utils.CSVHandler
}

// NewStateManager creates a new state manager
func NewStateManager(metadataDir string, completionTracker *shared.CompletionTracker) *StateManager {
	return &StateManager{
		metadataDir:       metadataDir,
		completionTracker: completionTracker,
		csvHandler:        utils.NewCSVHandler(metadataDir),
	}
}

// RebuildState rebuilds completion tracker state from CSV metadata files
func (sm *StateManager) RebuildState() error {
	log.Println("Group By Orchestrator: Rebuilding state from CSV metadata...")

	if err := os.MkdirAll(sm.metadataDir, 0755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	files, err := os.ReadDir(sm.metadataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read metadata directory: %w", err)
	}

	rebuiltCount := 0
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".csv") {
			continue
		}

		clientID := strings.TrimSuffix(file.Name(), ".csv")
		filePath := filepath.Join(sm.metadataDir, file.Name())

		// Check and fix incomplete last row
		hasIncomplete, err := sm.csvHandler.HasIncompleteLastRow(filePath)
		if err != nil {
			log.Printf("Group By Orchestrator: Warning - failed to check incomplete row in %s: %v", filePath, err)
			continue
		}
		if hasIncomplete {
			if err := sm.csvHandler.RemoveIncompleteLastRow(filePath); err != nil {
				log.Printf("Group By Orchestrator: Warning - failed to remove incomplete row in %s: %v", filePath, err)
				continue
			}
			log.Printf("Group By Orchestrator: Fixed incomplete last row in %s", filePath)
		}

		// Process notifications line-by-line
		notificationCount := 0
		expectedHeader := []string{"notification_id", "client_id", "file_id", "table_id", "chunk_number", "is_last_chunk", "is_last_from_table"}
		err = sm.csvHandler.ReadFileStreamingWithHeader(filePath, expectedHeader, func(row []string) error {
			if len(row) < len(expectedHeader) {
				return nil // Skip malformed rows
			}

			notificationID := row[0]
			fileID := row[2]
			tableID, err := strconv.Atoi(row[3])
			if err != nil {
				return nil // Skip invalid rows
			}
			chunkNumber, err := strconv.Atoi(row[4])
			if err != nil {
				return nil // Skip invalid rows
			}
			isLastChunk := row[5] == "1" || strings.ToLower(row[5]) == "true"
			isLastFromTable := row[6] == "1" || strings.ToLower(row[6]) == "true"

			notification := signals.NewChunkNotification(
				clientID,
				fileID,
				"",
				tableID,
				chunkNumber,
				isLastChunk,
				isLastFromTable,
			)
			notification.ID = notificationID

			if err := sm.completionTracker.ProcessChunkNotification(notification); err != nil {
				log.Printf("Group By Orchestrator: Warning - failed to process notification during rebuild: %v", err)
			} else {
				notificationCount++
			}

			return nil
		})

		if err != nil {
			log.Printf("Group By Orchestrator: Warning - failed to read notifications from %s: %v", filePath, err)
			continue
		}

		if notificationCount == 0 {
			continue
		}

		if sm.completionTracker.IsClientCompleted(clientID) {
			if err := sm.csvHandler.DeleteFile(filePath); err != nil {
				log.Printf("Group By Orchestrator: Warning - failed to delete metadata file for completed client %s: %v", clientID, err)
			} else {
				log.Printf("Group By Orchestrator: Deleted metadata file for already-completed client %s", clientID)
			}
		} else {
			rebuiltCount++
			log.Printf("Group By Orchestrator: Rebuilt state for client %s (%d notifications)", clientID, notificationCount)
		}
	}

	log.Printf("Group By Orchestrator: State rebuild complete: %d clients rebuilt", rebuiltCount)
	return nil
}

// AppendNotification appends a notification to the client's CSV metadata file
func (sm *StateManager) AppendNotification(notification *signals.ChunkNotification) error {
	csvPath := filepath.Join(sm.metadataDir, notification.ClientID+".csv")

	columns := []string{"notification_id", "client_id", "file_id", "table_id", "chunk_number", "is_last_chunk", "is_last_from_table"}

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

	return sm.csvHandler.AppendRow(csvPath, row, columns)
}

// DeleteClientMetadata deletes the CSV metadata file for a completed client
func (sm *StateManager) DeleteClientMetadata(clientID string) error {
	csvPath := filepath.Join(sm.metadataDir, clientID+".csv")
	return sm.csvHandler.DeleteFile(csvPath)
}
