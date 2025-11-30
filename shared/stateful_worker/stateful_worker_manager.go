package statefulworker

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/utils"
)

// ClientState represents the state for a specific client
type ClientState interface {
	GetClientID() string
	IsReady() bool
	UpdateState(csvRow []string) error // Update state from CSV row (used for both messages and rebuild)
}

// StatusBuilderFunc rebuilds client state from CSV metadata rows
type StatusBuilderFunc func(clientID string, csvRows [][]string) (ClientState, error)

// MetadataExtractor extracts CSV row data from a message
type MetadataExtractor func(message interface{}) []string

// StatefulWorkerManager manages stateful workers with CSV metadata persistence
type StatefulWorkerManager struct {
	metadataDir     string
	clientStates    map[string]ClientState
	buildStatus     StatusBuilderFunc
	extractMetadata MetadataExtractor
	csvColumns      []string
	csvHandler      *utils.CSVHandler
}

// NewStatefulWorkerManager creates a new stateful worker manager
func NewStatefulWorkerManager(
	metadataDir string,
	buildStatus StatusBuilderFunc,
	extractMetadata MetadataExtractor,
	csvColumns []string,
) *StatefulWorkerManager {
	return &StatefulWorkerManager{
		metadataDir:     metadataDir,
		clientStates:    make(map[string]ClientState),
		buildStatus:     buildStatus,
		extractMetadata: extractMetadata,
		csvColumns:      csvColumns,
		csvHandler:      utils.NewCSVHandler(metadataDir),
	}
}

// RebuildState reads all CSV metadata files and rebuilds state for non-ready clients
func (swm *StatefulWorkerManager) RebuildState() error {
	// Ensure metadata directory exists
	if err := os.MkdirAll(swm.metadataDir, 0755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	// Scan metadata directory for CSV files
	files, err := os.ReadDir(swm.metadataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read metadata directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".csv") {
			continue
		}

		// Extract clientID from filename: {clientID}.csv
		clientID := strings.TrimSuffix(file.Name(), ".csv")
		filePath := filepath.Join(swm.metadataDir, file.Name())

		// Check and fix incomplete last row
		hasIncomplete, err := swm.csvHandler.HasIncompleteLastRow(filePath)
		if err != nil {
			log.Printf("StatefulWorkerManager: Warning - failed to check incomplete row in %s: %v", filePath, err)
			continue
		}
		if hasIncomplete {
			if err := swm.csvHandler.RemoveIncompleteLastRow(filePath); err != nil {
				log.Printf("StatefulWorkerManager: Warning - failed to remove incomplete row in %s: %v", filePath, err)
				continue
			}
			log.Printf("StatefulWorkerManager: Fixed incomplete last row in %s", filePath)
		}

		// Initialize empty state
		state, err := swm.buildStatus(clientID, [][]string{})
		if err != nil {
			log.Printf("StatefulWorkerManager: Failed to initialize state for client %s: %v", clientID, err)
			continue
		}

		// Read CSV file line-by-line and update state incrementally
		rowCount := 0
		err = swm.csvHandler.ReadFileStreamingWithHeader(filePath, swm.csvColumns, func(row []string) error {
			// Update state from this row (incremental)
			if err := state.UpdateState(row); err != nil {
				log.Printf("StatefulWorkerManager: Failed to update state from CSV row: %v", err)
				return nil // Continue with next row
			}
			rowCount++
			return nil
		})

		if err != nil {
			log.Printf("StatefulWorkerManager: Failed to rebuild state from CSV for client %s: %v", clientID, err)
			continue
		}

		// Only keep non-ready clients
		if !state.IsReady() {
			swm.clientStates[clientID] = state
			log.Printf("StatefulWorkerManager: Rebuilt state for client %s (%d rows)", clientID, rowCount)
		} else {
			// Client already ready, delete CSV file
			if err := swm.csvHandler.DeleteFile(filePath); err != nil {
				log.Printf("StatefulWorkerManager: Failed to delete metadata file for ready client %s: %v", clientID, err)
			} else {
				log.Printf("StatefulWorkerManager: Client %s already ready, deleted metadata file", clientID)
			}
		}
	}

	return nil
}

// ProcessMessage processes a message: checks duplicates, updates state, appends to CSV
func (swm *StatefulWorkerManager) ProcessMessage(message interface{}) error {
	// Extract clientID and msgID (assuming message has GetClientID and GetID methods)
	clientID := extractClientID(message)
	msgID := extractMessageID(message)

	// Check if already processed (read CSV to check)
	processed, err := swm.IsMessageProcessed(clientID, msgID)
	if err != nil {
		return fmt.Errorf("failed to check if message processed: %w", err)
	}
	if processed {
		// Already processed, skip
		return nil
	}

	// Get or create client state
	state := swm.getOrCreateClientState(clientID)

	// Extract metadata row from message (CSV format)
	metadataRow := swm.extractMetadata(message)

	// Update state from CSV row (same format used for rebuild)
	if err := state.UpdateState(metadataRow); err != nil {
		return fmt.Errorf("failed to update state: %w", err)
	}

	// Append to CSV metadata file
	csvPath := filepath.Join(swm.metadataDir, clientID+".csv")
	if err := swm.csvHandler.AppendRow(csvPath, metadataRow, swm.csvColumns); err != nil {
		return fmt.Errorf("failed to append metadata row: %w", err)
	}

	return nil
}

// IsMessageProcessed checks if a message ID exists in the client's CSV metadata file
func (swm *StatefulWorkerManager) IsMessageProcessed(clientID, msgID string) (bool, error) {
	csvPath := filepath.Join(swm.metadataDir, clientID+".csv")

	// If file doesn't exist, message not processed
	if _, err := os.Stat(csvPath); os.IsNotExist(err) {
		return false, nil
	}

	// Read CSV file line-by-line and check for msgID (memory efficient)
	found := false
	err := swm.csvHandler.ReadFileStreaming(csvPath, func(row []string) error {
		// Check if msgID exists (first column is msg_id)
		if len(row) > 0 && row[0] == msgID {
			found = true
		}
		return nil
	})

	if err != nil {
		return false, err
	}

	return found, nil
}

// MarkClientReady deletes CSV file and state when client completes
func (swm *StatefulWorkerManager) MarkClientReady(clientID string) error {
	// Delete CSV file
	csvPath := filepath.Join(swm.metadataDir, clientID+".csv")
	if err := swm.csvHandler.DeleteFile(csvPath); err != nil {
		return fmt.Errorf("failed to delete metadata file: %w", err)
	}

	// Delete state
	delete(swm.clientStates, clientID)

	log.Printf("StatefulWorkerManager: Marked client %s as ready, deleted metadata and state", clientID)
	return nil
}

// GetClientState returns the state for a client (or nil if not found)
func (swm *StatefulWorkerManager) GetClientState(clientID string) ClientState {
	return swm.clientStates[clientID]
}

// getOrCreateClientState gets existing state or creates new one
func (swm *StatefulWorkerManager) getOrCreateClientState(clientID string) ClientState {
	if state, exists := swm.clientStates[clientID]; exists {
		return state
	}

	// Create new state (will be built from empty rows)
	state, err := swm.buildStatus(clientID, [][]string{})
	if err != nil {
		log.Printf("StatefulWorkerManager: Failed to create new state for client %s: %v", clientID, err)
		return nil
	}

	swm.clientStates[clientID] = state
	return state
}

// extractClientID extracts client ID from message (works with chunk.Chunk)
func extractClientID(message interface{}) string {
	// Try chunk.Chunk type assertion (Chunk has ClientID field)
	if ch, ok := message.(*chunk.Chunk); ok {
		return ch.ClientID
	}

	// Try interface with GetClientID method
	type ClientIDGetter interface {
		GetClientID() string
	}
	if getter, ok := message.(ClientIDGetter); ok {
		return getter.GetClientID()
	}

	return ""
}

// extractMessageID extracts message ID from message (works with chunk.Chunk)
func extractMessageID(message interface{}) string {
	// Try chunk.Chunk type assertion (Chunk has ID field)
	if ch, ok := message.(*chunk.Chunk); ok {
		return ch.ID
	}

	// Try interface with GetID method
	type IDGetter interface {
		GetID() string
	}
	if getter, ok := message.(IDGetter); ok {
		return getter.GetID()
	}

	return ""
}
