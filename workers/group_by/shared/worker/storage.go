package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/workers/group_by/shared"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/storage"
)

// FileManager wraps the storage FileManager for worker-specific operations
type FileManager struct {
	storageManager *storage.FileManager
	queryType      int
	csvWriter      *storage.CSVWriter
}

// NewFileManager creates a new file manager for a specific query type and worker
func NewFileManager(queryType int, workerID int) *FileManager {
	return &FileManager{
		storageManager: storage.NewFileManager(queryType, workerID),
		queryType:      queryType,
		csvWriter:      storage.NewCSVWriter(),
	}
}

// GetFilePath returns the file path for a given client and partition
func (fm *FileManager) GetFilePath(clientID string, partition int) string {
	return fm.storageManager.GetPartitionFilePath(clientID, partition)
}

// AppendToPartitionCSV appends a user_id,store_id record to a Query 4 partition CSV file
// This follows the in-file join pattern: each worker writes to its owned partitions
func (fm *FileManager) AppendToPartitionCSV(clientID string, partition int, userID string, storeID string) error {
	// Only for Query 4
	if fm.queryType != 4 {
		return fmt.Errorf("AppendToPartitionCSV only supports Query 4")
	}

	filePath := fm.GetFilePath(clientID, partition)

	// Ensure base directory exists
	if err := fm.storageManager.EnsureBaseDir(); err != nil {
		return err
	}

	// Append record using CSV writer
	header := []string{"user_id", "store_id"}
	record := []string{userID, storeID}
	return fm.csvWriter.AppendRecord(filePath, header, record)
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

	// Ensure base directory exists
	if err := fm.storageManager.EnsureBaseDir(); err != nil {
		return err
	}

	// Convert records to [][]string
	recordRows := make([][]string, len(records))
	for i, rec := range records {
		recordRows[i] = rec.ToCSVRow()
	}

	// Append records using CSV writer
	return fm.csvWriter.AppendRecords(filePath, header, recordRows)
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

	return fm.appendRecordsToPartitionCSV(clientID, partition, []string{"year", "month", "item_id", "quantity", "subtotal"}, csvRecords)
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

	// Header must match Query3Grouper.GetHeader(): year,semester,store_id,total_final_amount,count
	// Note: The grouper aggregates these records, so we use the raw fields here
	return fm.appendRecordsToPartitionCSV(clientID, partition, []string{"year", "semester", "store_id", "final_amount"}, csvRecords)
}
