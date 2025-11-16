package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tp-distribuidos-2c2025/workers/group_by/shared"
)

const (
	GroupByDataDir = "/app/groupby-data"
)

// FileManager handles file I/O operations for group by results
type FileManager struct {
	queryType int
	workerID  int
	baseDir   string
}

// NewFileManager creates a new file manager for a specific query type and worker
func NewFileManager(queryType int, workerID int) *FileManager {
	// Each worker has its own volume: /app/groupby-data/q{queryType}/worker-{id}/
	baseDir := filepath.Join(GroupByDataDir, fmt.Sprintf("q%d", queryType), fmt.Sprintf("worker-%d", workerID))
	return &FileManager{
		queryType: queryType,
		workerID:  workerID,
		baseDir:   baseDir,
	}
}

// GetFilePath returns the file path for a given client and partition
func (fm *FileManager) GetFilePath(clientID string, partition int) string {
	switch fm.queryType {
	case 2:
		// For Query 2: CSV file following naming convention (like Query 4)
		filename := fmt.Sprintf("%s-q2-partition-%03d.csv", clientID, partition)
		// Files are stored in baseDir, not clientDir (like Query 4 pattern)
		return filepath.Join(fm.baseDir, filename)
	case 3:
		// For Query 3: CSV file following naming convention (like Query 4)
		filename := fmt.Sprintf("%s-q3-partition-%03d.csv", clientID, partition)
		// Files are stored in baseDir, not clientDir (like Query 4 pattern)
		return filepath.Join(fm.baseDir, filename)
	case 4:
		// For Query 4: CSV file following naming convention (no locks needed)
		filename := fmt.Sprintf("%s-q4-partition-%03d.csv", clientID, partition)
		// Files are stored in baseDir, not clientDir (like in-file join pattern)
		return filepath.Join(fm.baseDir, filename)
	default:
		// Fallback for other query types (shouldn't happen in practice)
		clientDir := filepath.Join(fm.baseDir, clientID)
		filename := fmt.Sprintf("%d.json", partition)
		return filepath.Join(clientDir, filename)
	}
}

// AppendToPartitionCSV appends a user_id,store_id record to a Query 4 partition CSV file
// This follows the in-file join pattern: each worker writes to its owned partitions
func (fm *FileManager) AppendToPartitionCSV(clientID string, partition int, userID string, storeID string) error {
	// Only for Query 4
	if fm.queryType != 4 {
		return fmt.Errorf("AppendToPartitionCSV only supports Query 4")
	}

	filePath := fm.GetFilePath(clientID, partition)

	// Check if file exists to determine if we need to write header
	fileExists := true
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fileExists = false
	}

	// Ensure base directory exists (for Query 4, files are in baseDir, not clientDir)
	if err := os.MkdirAll(fm.baseDir, 0755); err != nil {
		return fmt.Errorf("failed to create base directory %s: %v", fm.baseDir, err)
	}

	// Open file in append mode
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open partition file %s: %v", filePath, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header if this is a new file
	if !fileExists {
		header := []string{"user_id", "store_id"}
		if err := writer.Write(header); err != nil {
			return fmt.Errorf("failed to write header: %v", err)
		}
	}

	// Write the record
	record := []string{userID, storeID}
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record: %v", err)
	}

	return nil
}

// CSVRecord defines the interface for records that can be written to CSV
type CSVRecord interface {
	ToCSVRow() []string
}

// appendRecordsToPartitionCSV is a generic method to append records to a partition CSV file
// header: CSV header row (e.g., []string{"month", "item_id", "quantity", "subtotal"})
// records: Slice of records implementing CSVRecord interface
func (fm *FileManager) appendRecordsToPartitionCSV(clientID string, partition int, header []string, records []CSVRecord) error {
	if len(records) == 0 {
		return nil
	}

	filePath := fm.GetFilePath(clientID, partition)

	// Check if file exists to determine if we need to write header
	fileExists := true
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fileExists = false
	}

	// Ensure base directory exists
	if err := os.MkdirAll(fm.baseDir, 0755); err != nil {
		return fmt.Errorf("failed to create base directory %s: %v", fm.baseDir, err)
	}

	// Open file in append mode
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open partition file %s: %v", filePath, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header if this is a new file
	if !fileExists {
		if err := writer.Write(header); err != nil {
			return fmt.Errorf("failed to write header: %v", err)
		}
	}

	// Write all records using ToCSVRow method
	for _, rec := range records {
		if err := writer.Write(rec.ToCSVRow()); err != nil {
			return fmt.Errorf("failed to write record: %v", err)
		}
	}

	return nil
}

// AppendRecordsToPartitionCSV appends multiple user_id,store_id records to a Query 4 partition CSV file
// More efficient than calling AppendToPartitionCSV multiple times
func (fm *FileManager) AppendRecordsToPartitionCSV(clientID string, partition int, records []shared.Query4Record) error {
	// Only for Query 4
	if fm.queryType != 4 {
		return fmt.Errorf("AppendRecordsToPartitionCSV only supports Query 4")
	}

	// Convert to CSVRecord slice
	csvRecords := make([]CSVRecord, len(records))
	for i := range records {
		csvRecords[i] = records[i]
	}

	return fm.appendRecordsToPartitionCSV(clientID, partition, []string{"user_id", "store_id"}, csvRecords)
}

// AppendQuery2RecordsToPartitionCSV appends multiple month,item_id,quantity,subtotal records to a Query 2 partition CSV file
// More efficient than appending records one by one
func (fm *FileManager) AppendQuery2RecordsToPartitionCSV(clientID string, partition int, records []shared.Query2Record) error {
	// Only for Query 2
	if fm.queryType != 2 {
		return fmt.Errorf("AppendQuery2RecordsToPartitionCSV only supports Query 2")
	}

	// Convert to CSVRecord slice
	csvRecords := make([]CSVRecord, len(records))
	for i := range records {
		csvRecords[i] = records[i]
	}

	return fm.appendRecordsToPartitionCSV(clientID, partition, []string{"month", "item_id", "quantity", "subtotal"}, csvRecords)
}

// AppendQuery3RecordsToPartitionCSV appends multiple store_id,final_amount records to a Query 3 partition CSV file
// More efficient than appending records one by one
func (fm *FileManager) AppendQuery3RecordsToPartitionCSV(clientID string, partition int, records []shared.Query3Record) error {
	// Only for Query 3
	if fm.queryType != 3 {
		return fmt.Errorf("AppendQuery3RecordsToPartitionCSV only supports Query 3")
	}

	// Convert to CSVRecord slice
	csvRecords := make([]CSVRecord, len(records))
	for i := range records {
		csvRecords[i] = records[i]
	}

	return fm.appendRecordsToPartitionCSV(clientID, partition, []string{"store_id", "final_amount"}, csvRecords)
}
