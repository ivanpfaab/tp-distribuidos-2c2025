package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/utils"
)

// TopUsersStateManager manages state persistence and rebuild for top users worker
type TopUsersStateManager struct {
	metadataDir   string
	numPartitions int
	csvHandler    *utils.CSVHandler
}

// NewTopUsersStateManager creates a new state manager
func NewTopUsersStateManager(metadataDir string, numPartitions int) *TopUsersStateManager {
	return &TopUsersStateManager{
		metadataDir:   metadataDir,
		numPartitions: numPartitions,
		csvHandler:    utils.NewCSVHandler(metadataDir),
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

		// Check and fix incomplete last row
		hasIncomplete, err := tsm.csvHandler.HasIncompleteLastRow(filePath)
		if err != nil {
			log.Printf("Top Users Worker: Warning - failed to check incomplete row in %s: %v", filePath, err)
			continue
		}
		if hasIncomplete {
			if err := tsm.csvHandler.RemoveIncompleteLastRow(filePath); err != nil {
				log.Printf("Top Users Worker: Warning - failed to remove incomplete row in %s: %v", filePath, err)
				continue
			}
			log.Printf("Top Users Worker: Fixed incomplete last row in %s", filePath)
		}

		// Rebuild state line-by-line
		state := &ClientState{
			topUsersByStore: make(map[string]*StoreTopUsers),
			receivedChunks:  make(map[int]bool),
			numPartitions:   tsm.numPartitions,
		}

		expectedHeader := []string{"msg_id", "chunk_number", "user_id", "store_id", "purchase_count"}
		err = tsm.csvHandler.ReadFileStreamingWithHeader(filePath, expectedHeader, func(row []string) error {
			if len(row) < len(expectedHeader) {
				return nil // Skip malformed rows
			}

			chunkNumber, err := strconv.Atoi(row[1])
			if err != nil {
				return nil // Skip invalid rows
			}

			userID := row[2]
			storeID := row[3]
			purchaseCount, err := strconv.Atoi(row[4])
			if err != nil {
				return nil // Skip invalid rows
			}

			state.receivedChunks[chunkNumber] = true

			if userID != "" && storeID != "" {
				if state.topUsersByStore[storeID] == nil {
					state.topUsersByStore[storeID] = NewStoreTopUsers(storeID)
				}
				state.topUsersByStore[storeID].Add(userID, purchaseCount)
			}

			return nil
		})

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
			if err := tsm.csvHandler.DeleteFile(filePath); err != nil {
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
// IMPORTANT: Always writes at least a marker row to ensure chunk is recorded for recovery
func (tsm *TopUsersStateManager) ProcessChunk(chunkMsg *chunk.Chunk, clientState *ClientState) error {
	chunkNumber := int(chunkMsg.ChunkNumber)
	clientState.receivedChunks[chunkNumber] = true

	csvPath := filepath.Join(tsm.metadataDir, chunkMsg.ClientID+".csv")
	columns := []string{"msg_id", "chunk_number", "user_id", "store_id", "purchase_count"}

	// Collect all rows to write in batch
	var rowsToWrite [][]string

	// If chunk has data, parse and process it
	if chunkMsg.ChunkSize > 0 && len(chunkMsg.ChunkData) > 0 {
		// Parse CSV data
		reader := csv.NewReader(strings.NewReader(chunkMsg.ChunkData))
		records, err := reader.ReadAll()
		if err != nil {
			return fmt.Errorf("failed to parse CSV: %w", err)
		}

		// Process each row (skip header if present)
		startIdx := 1
		if len(records) > 0 && len(records[0]) > 0 {
			// Check if first row looks like header
			if records[0][0] == "msg_id" || records[0][0] == "user_id" {
				startIdx = 1
			} else {
				startIdx = 0
			}
		}

		for i := startIdx; i < len(records); i++ {
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

			// Skip metadata marker rows (empty userID/storeID) - they are only for fault tolerance
			if userID == "" || storeID == "" {
				continue
			}

			// Update state
			if clientState.topUsersByStore[storeID] == nil {
				clientState.topUsersByStore[storeID] = NewStoreTopUsers(storeID)
			}
			clientState.topUsersByStore[storeID].Add(userID, purchaseCount)

			// Collect row for batch write
			row := []string{
				chunkMsg.ID,
				strconv.Itoa(chunkNumber),
				userID,
				storeID,
				strconv.Itoa(purchaseCount),
			}
			rowsToWrite = append(rowsToWrite, row)
		}
	}

	// CRITICAL: Always write at least a marker row to record that this chunk was received
	// This ensures the chunk can be recovered after a crash, even if it had no data
	if len(rowsToWrite) == 0 {
		// Write a marker row with empty values - will be recognized during rebuild
		markerRow := []string{
			chunkMsg.ID,
			strconv.Itoa(chunkNumber),
			"",  // empty user_id indicates marker
			"",  // empty store_id
			"0", // purchase_count = 0
		}
		rowsToWrite = append(rowsToWrite, markerRow)
	}

	// Write all rows (always at least one marker row)
	if err := tsm.csvHandler.AppendRows(csvPath, rowsToWrite, columns); err != nil {
		return fmt.Errorf("failed to append rows: %w", err)
	}

	return nil
}

// MarkClientReady deletes the CSV metadata file for a completed client
func (tsm *TopUsersStateManager) MarkClientReady(clientID string) error {
	csvPath := filepath.Join(tsm.metadataDir, clientID+".csv")
	return tsm.csvHandler.DeleteFile(csvPath)
}
