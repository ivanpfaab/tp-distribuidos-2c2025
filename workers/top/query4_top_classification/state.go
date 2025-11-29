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

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
)

// TopUsersStateManager manages state persistence and rebuild for top users worker
type TopUsersStateManager struct {
	metadataDir   string
	numPartitions int
}

// NewTopUsersStateManager creates a new state manager
func NewTopUsersStateManager(metadataDir string, numPartitions int) *TopUsersStateManager {
	return &TopUsersStateManager{
		metadataDir:   metadataDir,
		numPartitions: numPartitions,
	}
}

// RebuildState rebuilds client states from CSV metadata files
func (tsm *TopUsersStateManager) RebuildState(clientStates map[string]*ClientState) error {
	log.Println("Top Users Worker: Rebuilding state from CSV metadata...")

	if err := os.MkdirAll(tsm.metadataDir, 0755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	files, err := os.ReadDir(tsm.metadataDir)
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
		filePath := filepath.Join(tsm.metadataDir, file.Name())

		// Read CSV and rebuild state
		state, err := tsm.readChunkDataFromCSV(filePath, clientID)
		if err != nil {
			log.Printf("Top Users Worker: Warning - failed to read state from %s: %v", filePath, err)
			continue
		}

		// Check if client is ready (all chunks received)
		allReceived := len(state.receivedChunks) >= state.numPartitions
		if allReceived {
			for i := 0; i < state.numPartitions && allReceived; i++ {
				if !state.receivedChunks[i] {
					allReceived = false
				}
			}
		}

		if allReceived {
			if err := os.Remove(filePath); err != nil {
				log.Printf("Top Users Worker: Warning - failed to delete metadata file for completed client %s: %v", clientID, err)
			}
		} else {
			clientStates[clientID] = state
			rebuiltCount++
		}
	}

	log.Printf("Top Users Worker: State rebuild complete: %d clients rebuilt", rebuiltCount)
	return nil
}

// ProcessChunk processes a chunk message: persists CSV data and updates state
func (tsm *TopUsersStateManager) ProcessChunk(chunkMsg *chunk.Chunk, clientState *ClientState) error {
	chunkNumber := int(chunkMsg.ChunkNumber)
	clientState.receivedChunks[chunkNumber] = true

	if chunkMsg.ChunkSize == 0 || len(chunkMsg.ChunkData) == 0 {
		return nil
	}

	// Parse CSV data
	reader := csv.NewReader(strings.NewReader(chunkMsg.ChunkData))
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to parse CSV: %w", err)
	}

	// Open CSV file once for all rows
	csvPath := filepath.Join(tsm.metadataDir, chunkMsg.ClientID+".csv")
	if err := os.MkdirAll(tsm.metadataDir, 0755); err != nil {
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
		header := []string{"msg_id", "chunk_number", "user_id", "store_id", "purchase_count"}
		if err := writer.Write(header); err != nil {
			return fmt.Errorf("failed to write header: %w", err)
		}
	}

	// Process each row
	for i := 1; i < len(records); i++ {
		record := records[i]
		if len(record) < 3 {
			continue
		}

		userID := record[0]
		storeID := record[1]
		purchaseCount, err := strconv.Atoi(record[2])
		if err != nil {
			continue
		}

		// Update state
		if clientState.topUsersByStore[storeID] == nil {
			clientState.topUsersByStore[storeID] = NewStoreTopUsers(storeID)
		}
		clientState.topUsersByStore[storeID].Add(userID, purchaseCount)

		// Write row to CSV
		row := []string{
			chunkMsg.ID,
			strconv.Itoa(chunkNumber),
			userID,
			storeID,
			strconv.Itoa(purchaseCount),
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
	}

	// Sync once after all rows written
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// MarkClientReady deletes the CSV metadata file for a completed client
func (tsm *TopUsersStateManager) MarkClientReady(clientID string) error {
	csvPath := filepath.Join(tsm.metadataDir, clientID+".csv")
	if err := os.Remove(csvPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete metadata file for client %s: %w", clientID, err)
	}
	return nil
}

// readChunkDataFromCSV reads CSV file and rebuilds client state
func (tsm *TopUsersStateManager) readChunkDataFromCSV(filePath, clientID string) (*ClientState, error) {
	// Check for incomplete last row
	hasIncomplete, err := tsm.hasIncompleteLastRow(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to check incomplete row: %w", err)
	}
	if hasIncomplete {
		if err := tsm.removeIncompleteLastRow(filePath); err != nil {
			return nil, fmt.Errorf("failed to remove incomplete row: %w", err)
		}
		log.Printf("Top Users Worker: Fixed incomplete last row in %s", filePath)
	}

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return &ClientState{
				topUsersByStore: make(map[string]*StoreTopUsers),
				receivedChunks:  make(map[int]bool),
				numPartitions:   tsm.numPartitions,
			}, nil
		}
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	state := &ClientState{
		topUsersByStore: make(map[string]*StoreTopUsers),
		receivedChunks:  make(map[int]bool),
		numPartitions:   tsm.numPartitions,
	}

	// Read header
	header, err := reader.Read()
	if err == io.EOF {
		return state, nil
	}
	if err != nil {
		return nil, err
	}

	expectedHeader := []string{"msg_id", "chunk_number", "user_id", "store_id", "purchase_count"}
	if len(header) < len(expectedHeader) {
		return nil, fmt.Errorf("invalid CSV header")
	}

	// Read data rows and rebuild state
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if len(row) < len(expectedHeader) {
			continue
		}

		chunkNumber, err := strconv.Atoi(row[1])
		if err != nil {
			continue
		}

		userID := row[2]
		storeID := row[3]
		purchaseCount, err := strconv.Atoi(row[4])
		if err != nil {
			continue
		}

		state.receivedChunks[chunkNumber] = true

		if userID != "" && storeID != "" {
			if state.topUsersByStore[storeID] == nil {
				state.topUsersByStore[storeID] = NewStoreTopUsers(storeID)
			}
			state.topUsersByStore[storeID].Add(userID, purchaseCount)
		}
	}

	return state, nil
}

// hasIncompleteLastRow checks if the last row in a CSV file is incomplete (missing \n)
func (tsm *TopUsersStateManager) hasIncompleteLastRow(filePath string) (bool, error) {
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

	lastByte := make([]byte, 1)
	_, err = file.ReadAt(lastByte, stat.Size()-1)
	if err != nil {
		return false, err
	}

	return lastByte[0] != '\n', nil
}

// removeIncompleteLastRow removes the incomplete last row by truncating the file
func (tsm *TopUsersStateManager) removeIncompleteLastRow(filePath string) error {
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

	return file.Truncate(0)
}
