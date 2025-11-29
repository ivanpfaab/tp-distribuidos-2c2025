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

// TopItemsStateManager manages state persistence and rebuild for top items worker
type TopItemsStateManager struct {
	metadataDir   string
	numPartitions int
}

// NewTopItemsStateManager creates a new state manager
func NewTopItemsStateManager(metadataDir string, numPartitions int) *TopItemsStateManager {
	return &TopItemsStateManager{
		metadataDir:   metadataDir,
		numPartitions: numPartitions,
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

		state, err := tsm.readChunkDataFromCSV(filePath, clientID)
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
			if err := os.Remove(filePath); err != nil {
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
func (tsm *TopItemsStateManager) ProcessChunk(chunkMsg *chunk.Chunk, clientState *ClientState) error {
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
		header := []string{"msg_id", "chunk_number", "year", "month", "item_id", "total_quantity", "total_subtotal"}
		if err := writer.Write(header); err != nil {
			return fmt.Errorf("failed to write header: %w", err)
		}
	}

	// Process each row
	for i := 0; i < len(records); i++ {
		record := records[i]
		// Skip header rows
		if strings.Contains(record[0], "year") {
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

		// Write row to CSV
		row := []string{
			chunkMsg.ID,
			strconv.Itoa(chunkNumber),
			strconv.Itoa(year),
			strconv.Itoa(month),
			itemID,
			strconv.Itoa(totalQuantity),
			strconv.FormatFloat(totalSubtotal, 'f', 2, 64),
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
func (tsm *TopItemsStateManager) MarkClientReady(clientID string) error {
	csvPath := filepath.Join(tsm.metadataDir, clientID+".csv")
	if err := os.Remove(csvPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete metadata file for client %s: %w", clientID, err)
	}
	return nil
}

// readChunkDataFromCSV reads CSV file and rebuilds client state
func (tsm *TopItemsStateManager) readChunkDataFromCSV(filePath, clientID string) (*ClientState, error) {
	// Check for incomplete last row
	hasIncomplete, err := tsm.hasIncompleteLastRow(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to check incomplete row: %w", err)
	}
	if hasIncomplete {
		if err := tsm.removeIncompleteLastRow(filePath); err != nil {
			return nil, fmt.Errorf("failed to remove incomplete row: %w", err)
		}
		log.Printf("Top Items Worker: Fixed incomplete last row in %s", filePath)
	}

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return &ClientState{
				topItemsByMonth: make(map[string]*MonthTopItems),
				receivedChunks:  make(map[int]bool),
				numPartitions:   tsm.numPartitions,
			}, nil
		}
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	state := &ClientState{
		topItemsByMonth: make(map[string]*MonthTopItems),
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

	expectedHeader := []string{"msg_id", "chunk_number", "year", "month", "item_id", "total_quantity", "total_subtotal"}
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

		year, err := strconv.Atoi(row[2])
		if err != nil {
			continue
		}

		month, err := strconv.Atoi(row[3])
		if err != nil {
			continue
		}

		itemID := row[4]
		totalQuantity, err := strconv.Atoi(row[5])
		if err != nil {
			continue
		}

		totalSubtotal, err := strconv.ParseFloat(row[6], 64)
		if err != nil {
			continue
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
	}

	return state, nil
}

// hasIncompleteLastRow checks if the last row in a CSV file is incomplete (missing \n)
func (tsm *TopItemsStateManager) hasIncompleteLastRow(filePath string) (bool, error) {
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
func (tsm *TopItemsStateManager) removeIncompleteLastRow(filePath string) error {
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
