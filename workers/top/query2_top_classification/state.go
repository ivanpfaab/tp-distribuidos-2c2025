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

// TopItemsStateManager manages state persistence and rebuild for top items worker
type TopItemsStateManager struct {
	metadataDir   string
	numPartitions int
	csvHandler    *utils.CSVHandler
}

// NewTopItemsStateManager creates a new state manager
func NewTopItemsStateManager(metadataDir string, numPartitions int) *TopItemsStateManager {
	return &TopItemsStateManager{
		metadataDir:   metadataDir,
		numPartitions: numPartitions,
		csvHandler:    utils.NewCSVHandler(metadataDir),
	}
}

// RebuildState rebuilds client states from CSV metadata files
func (tsm *TopItemsStateManager) RebuildState(clientStates map[string]*ClientState) error {
	log.Println("Top Items Worker: Rebuilding state from CSV metadata...")

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
			log.Printf("Top Items Worker: Warning - failed to check incomplete row in %s: %v", filePath, err)
			continue
		}
		if hasIncomplete {
			if err := tsm.csvHandler.RemoveIncompleteLastRow(filePath); err != nil {
				log.Printf("Top Items Worker: Warning - failed to remove incomplete row in %s: %v", filePath, err)
				continue
			}
			log.Printf("Top Items Worker: Fixed incomplete last row in %s", filePath)
		}

		// Rebuild state line-by-line
		state := &ClientState{
			topItemsByMonth: make(map[string]*MonthTopItems),
			receivedChunks:  make(map[int]bool),
			numPartitions:   tsm.numPartitions,
		}

		expectedHeader := []string{"msg_id", "chunk_number", "year", "month", "item_id", "total_quantity", "total_subtotal"}
		err = tsm.csvHandler.ReadFileStreamingWithHeader(filePath, expectedHeader, func(row []string) error {
			if len(row) < len(expectedHeader) {
				return nil // Skip malformed rows
			}

			chunkNumber, err := strconv.Atoi(row[1])
			if err != nil {
				return nil // Skip invalid rows
			}

			year, err := strconv.Atoi(row[2])
			if err != nil {
				return nil // Skip invalid rows
			}

			month, err := strconv.Atoi(row[3])
			if err != nil {
				return nil // Skip invalid rows
			}

			itemID := row[4]
			totalQuantity, err := strconv.Atoi(row[5])
			if err != nil {
				return nil // Skip invalid rows
			}

			totalSubtotal, err := strconv.ParseFloat(row[6], 64)
			if err != nil {
				return nil // Skip invalid rows
			}

			state.receivedChunks[chunkNumber] = true

			// Update top items by month
			monthKey := fmt.Sprintf("%04d-%02d", year, month)
			if state.topItemsByMonth[monthKey] == nil {
				state.topItemsByMonth[monthKey] = &MonthTopItems{}
			}

			monthTop := state.topItemsByMonth[monthKey]
			currentItem := &ItemRecord{
				ItemID:        itemID,
				Year:          year,
				Month:         month,
				TotalQuantity: totalQuantity,
				TotalSubtotal: totalSubtotal,
			}

			// Update top by quantity
			if monthTop.TopByQuantity == nil || currentItem.TotalQuantity > monthTop.TopByQuantity.TotalQuantity {
				monthTop.TopByQuantity = currentItem
			}

			// Update top by revenue
			if monthTop.TopByRevenue == nil || currentItem.TotalSubtotal > monthTop.TopByRevenue.TotalSubtotal {
				monthTop.TopByRevenue = currentItem
			}

			return nil
		})

		if err != nil {
			log.Printf("Top Items Worker: Warning - failed to read state from %s: %v", filePath, err)
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
				log.Printf("Top Items Worker: Warning - failed to delete metadata file for completed client %s: %v", clientID, err)
			}
		} else {
			clientStates[clientID] = state
			rebuiltCount++
		}
	}

	log.Printf("Top Items Worker: State rebuild complete: %d clients rebuilt", rebuiltCount)
	return nil
}

// ProcessChunk processes a chunk message: persists CSV data and updates state
// IMPORTANT: Always writes at least a marker row to ensure chunk is recorded for recovery
func (tsm *TopItemsStateManager) ProcessChunk(chunkMsg *chunk.Chunk, clientState *ClientState) error {
	chunkNumber := int(chunkMsg.ChunkNumber)
	clientState.receivedChunks[chunkNumber] = true

	csvPath := filepath.Join(tsm.metadataDir, chunkMsg.ClientID+".csv")
	columns := []string{"msg_id", "chunk_number", "year", "month", "item_id", "total_quantity", "total_subtotal"}

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

		// Process each row (skip header rows)
		for i := 0; i < len(records); i++ {
			record := records[i]
			// Skip header rows
			if len(record) > 0 && strings.Contains(record[0], "year") {
				continue
			}

			if len(record) < 6 {
				continue
			}

			year, err := strconv.Atoi(record[0])
			if err != nil {
				continue
			}

			month, err := strconv.Atoi(record[1])
			if err != nil {
				continue
			}

			itemID := record[2]
			totalQuantity, err := strconv.Atoi(record[3])
			if err != nil {
				continue
			}

			totalSubtotal, err := strconv.ParseFloat(record[4], 64)
			if err != nil {
				continue
			}

			// Update state
			monthKey := fmt.Sprintf("%04d-%02d", year, month)
			if clientState.topItemsByMonth[monthKey] == nil {
				clientState.topItemsByMonth[monthKey] = &MonthTopItems{}
			}

			monthTop := clientState.topItemsByMonth[monthKey]
			currentItem := &ItemRecord{
				ItemID:        itemID,
				Year:          year,
				Month:         month,
				TotalQuantity: totalQuantity,
				TotalSubtotal: totalSubtotal,
			}

			// Update top by quantity
			if monthTop.TopByQuantity == nil || currentItem.TotalQuantity > monthTop.TopByQuantity.TotalQuantity {
				monthTop.TopByQuantity = currentItem
			}

			// Update top by revenue
			if monthTop.TopByRevenue == nil || currentItem.TotalSubtotal > monthTop.TopByRevenue.TotalSubtotal {
				monthTop.TopByRevenue = currentItem
			}

			// Collect row for batch write
			row := []string{
				chunkMsg.ID,
				strconv.Itoa(chunkNumber),
				strconv.Itoa(year),
				strconv.Itoa(month),
				itemID,
				strconv.Itoa(totalQuantity),
				strconv.FormatFloat(totalSubtotal, 'f', 2, 64),
			}
			rowsToWrite = append(rowsToWrite, row)
		}
	}

	// CRITICAL: Always write at least a marker row to record that this chunk was received
	// This ensures the chunk can be recovered after a crash, even if it had no data
	if len(rowsToWrite) == 0 {
		// Write a marker row with zeros - will be recognized during rebuild
		markerRow := []string{
			chunkMsg.ID,
			strconv.Itoa(chunkNumber),
			"0",  // year = 0 indicates marker
			"0",  // month = 0
			"",   // empty item_id
			"0",  // quantity = 0
			"0",  // subtotal = 0
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
func (tsm *TopItemsStateManager) MarkClientReady(clientID string) error {
	csvPath := filepath.Join(tsm.metadataDir, clientID+".csv")
	return tsm.csvHandler.DeleteFile(csvPath)
}
