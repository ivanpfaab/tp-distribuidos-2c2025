package statefulworker

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
)

// ClientState represents the state for a specific client
type ClientState interface {
	GetClientID() string
	IsReady() bool
	UpdateFromMessage(message interface{}) error
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
		return fmt.Errorf("failed to read metadata directory: %w", err)
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".csv") {
			continue
		}

		// Extract clientID from filename: {clientID}.csv
		clientID := strings.TrimSuffix(file.Name(), ".csv")

		// Read CSV file
		filePath := filepath.Join(swm.metadataDir, file.Name())
		rows, err := readMetadataCSV(filePath)
		if err != nil {
			log.Printf("StatefulWorkerManager: Failed to read metadata for client %s: %v", clientID, err)
			continue
		}

		// Build state from CSV rows
		state, err := swm.buildStatus(clientID, rows)
		if err != nil {
			log.Printf("StatefulWorkerManager: Failed to build state for client %s: %v", clientID, err)
			continue
		}

		// Only keep non-ready clients
		if !state.IsReady() {
			swm.clientStates[clientID] = state
			log.Printf("StatefulWorkerManager: Rebuilt state for client %s (%d unique rows)", clientID, len(rows))
		} else {
			// Client already ready, delete CSV file
			if err := os.Remove(filePath); err != nil {
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

	// Update state from message
	if err := state.UpdateFromMessage(message); err != nil {
		return fmt.Errorf("failed to update state: %w", err)
	}

	// Extract metadata row from message
	metadataRow := swm.extractMetadata(message)

	// Append to CSV metadata file
	csvPath := filepath.Join(swm.metadataDir, clientID+".csv")
	if err := appendMetadataRow(csvPath, swm.csvColumns, metadataRow); err != nil {
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

	// Read CSV file and check for msgID
	rows, err := readMetadataCSV(csvPath)
	if err != nil {
		return false, err
	}

	// Check if msgID exists (first column is msg_id)
	for _, row := range rows {
		if len(row) > 0 && row[0] == msgID {
			return true, nil
		}
	}

	return false, nil
}

// MarkClientReady deletes CSV file and state when client completes
func (swm *StatefulWorkerManager) MarkClientReady(clientID string) error {
	// Delete CSV file
	csvPath := filepath.Join(swm.metadataDir, clientID+".csv")
	if err := os.Remove(csvPath); err != nil && !os.IsNotExist(err) {
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

// readMetadataCSV reads a CSV metadata file line-by-line (memory efficient)
func readMetadataCSV(filePath string) ([][]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return [][]string{}, nil
		}
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows := [][]string{}

	// Read header (first line) - we'll skip it
	_, err = reader.Read()
	if err == io.EOF {
		return rows, nil // Empty file
	}
	if err != nil {
		return nil, err
	}

	// Read data rows line by line
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// appendMetadataRow appends a row to the CSV metadata file
func appendMetadataRow(filePath string, columns []string, row []string) error {
	// Ensure directory exists
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header if file is new (size == 0)
	stat, err := file.Stat()
	if err != nil {
		return err
	}

	if stat.Size() == 0 {
		if err := writer.Write(columns); err != nil {
			return err
		}
	}

	// Write data row
	return writer.Write(row)
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
