package main

import (
	"fmt"
	"strconv"
	"strings"

	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
	partitionmanager "github.com/tp-distribuidos-2c2025/shared/partition_manager"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/storage"
)

// FileManager wraps the storage FileManager for worker-specific operations
type FileManager struct {
	storageManager   *storage.FileManager
	queryType        int
	partitionManager *partitionmanager.PartitionManager
}

// NewFileManager creates a new file manager for a specific query type and worker
func NewFileManager(queryType int, workerID int) *FileManager {
	return &FileManager{
		storageManager: storage.NewFileManager(queryType, workerID),
		queryType:      queryType,
	}
}

// SetPartitionManager sets the partition manager for fault-tolerant writing
func (fm *FileManager) SetPartitionManager(pm *partitionmanager.PartitionManager) {
	fm.partitionManager = pm
}

// GetFilePath returns the file path for a given client and partition
func (fm *FileManager) GetFilePath(clientID string, partition int) string {
	return fm.storageManager.GetPartitionFilePath(clientID, partition)
}

// CSVRecord defines the interface for records that can be written to CSV
type CSVRecord interface {
	ToCSVRow() []string
}

// AppendRecordsToPartitionCSV appends multiple user_id,store_id records to a Query 4 partition CSV file
// Uses batch writing for efficiency
func (fm *FileManager) AppendRecordsToPartitionCSV(clientID string, partition int, records []shared.Query4Record, chunkID string, isFirstChunk bool) error {
	// Only for Query 4
	if fm.queryType != 4 {
		return fmt.Errorf("AppendRecordsToPartitionCSV only supports Query 4")
	}

	// Convert records to CSV lines format
	lines := make([]string, 0, len(records))
	for i, record := range records {
		recordRow := record.ToCSVRow()
		// Append chunk ID as last field
		recordRow = append(recordRow, chunkID)
		recordRow = append(recordRow, strconv.Itoa(i+1))
		csvLine := strings.Join(recordRow, ",") + "\n"
		lines = append(lines, csvLine)
	}

	opts := partitionmanager.WriteOptions{
		FilePrefix: fmt.Sprintf("q%d-partition", fm.queryType),
		Header:     []string{"user_id", "store_id", "chunk_id", "row_number"},
		ClientID:   clientID,
		DebugMode:  false,
	}

	return fm.writePartitionWithFaultTolerance(partition, lines, opts, isFirstChunk)
}

// writePartitionWithFaultTolerance writes partition data with fault tolerance support
func (fm *FileManager) writePartitionWithFaultTolerance(partition int, lines []string, opts partitionmanager.WriteOptions, isFirstChunk bool) error {
	if fm.partitionManager == nil {
		return fmt.Errorf("partition manager not initialized")
	}

	partitionData := partitionmanager.PartitionData{
		Number: partition,
		Lines:  lines,
	}

	// Check if this is the first chunk after restart
	if isFirstChunk {
		// First chunk after restart - check for duplicates/incomplete writes
		filePath := fm.partitionManager.GetPartitionFilePath(opts, partition)
		linesCount := len(lines)*2 // To avoid duplicates we leave some space for the last lines
		lastLines, err := fm.partitionManager.GetLastLines(filePath, linesCount)
		if err != nil {
			return fmt.Errorf("failed to get last lines for partition %d: %w", partition, err)
		}

		testing_utils.LogInfo("File Manager", "Partition %d", partition)
		if err := fm.partitionManager.WriteOnlyMissingLines(filePath, lastLines, lines, opts); err != nil {
			return fmt.Errorf("failed to write missing lines to partition %d: %w", partition, err)
		}
	} else {
		// Normal write (WritePartition handles incomplete writes automatically)
		if err := fm.partitionManager.WritePartition(partitionData, opts); err != nil {
			return fmt.Errorf("failed to write partition %d: %w", partition, err)
		}
	}

	return nil
}

// AppendQuery2RecordsToPartitionCSV appends multiple month,item_id,quantity,subtotal records to a Query 2 partition CSV file
// More efficient than appending records one by one
func (fm *FileManager) AppendQuery2RecordsToPartitionCSV(clientID string, partition int, records []shared.Query2Record, chunkID string, isFirstChunk bool) error {
	// Only for Query 2
	if fm.queryType != 2 {
		return fmt.Errorf("AppendQuery2RecordsToPartitionCSV only supports Query 2")
	}

	// Convert records to CSV lines format
	lines := make([]string, 0, len(records))
	for i, record := range records {
		recordRow := record.ToCSVRow()
		// Append chunk ID as last field
		recordRow = append(recordRow, chunkID)
		recordRow = append(recordRow, strconv.Itoa(i+1))
		csvLine := strings.Join(recordRow, ",") + "\n"
		lines = append(lines, csvLine)
	}

	opts := partitionmanager.WriteOptions{
		FilePrefix: fmt.Sprintf("q%d-partition", fm.queryType),
		Header:     []string{"year", "month", "item_id", "quantity", "subtotal", "chunk_id", "row_number"},
		ClientID:   clientID,
		DebugMode:  false,
	}

	return fm.writePartitionWithFaultTolerance(partition, lines, opts, isFirstChunk)
}

// AppendQuery3RecordsToPartitionCSV appends multiple store_id,final_amount records to a Query 3 partition CSV file
// More efficient than appending records one by one
func (fm *FileManager) AppendQuery3RecordsToPartitionCSV(clientID string, partition int, records []shared.Query3Record, chunkID string, isFirstChunk bool) error {
	// Only for Query 3
	if fm.queryType != 3 {
		return fmt.Errorf("AppendQuery3RecordsToPartitionCSV only supports Query 3")
	}

	// Convert records to CSV lines format
	lines := make([]string, 0, len(records))
	for i, record := range records {
		recordRow := record.ToCSVRow()
		// Append chunk ID as last field
		recordRow = append(recordRow, chunkID)
		recordRow = append(recordRow, strconv.Itoa(i+1))
		csvLine := strings.Join(recordRow, ",") + "\n"
		lines = append(lines, csvLine)
	}

	opts := partitionmanager.WriteOptions{
		FilePrefix: fmt.Sprintf("q%d-partition", fm.queryType),
		Header:     []string{"year", "semester", "store_id", "final_amount", "chunk_id", "row_number"},
		ClientID:   clientID,
		DebugMode:  false,
	}

	return fm.writePartitionWithFaultTolerance(partition, lines, opts, isFirstChunk)
}
