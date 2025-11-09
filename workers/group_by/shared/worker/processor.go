package main

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
)

// ChunkProcessor handles processing of chunks and aggregation of data
type ChunkProcessor struct {
	queryType int
}

// UserStoreRecord represents a user_id, store_id pair for Query 4
type UserStoreRecord struct {
	UserID    string
	StoreID   string
	Partition int
}

// Query2Record represents a month, item_id, quantity, subtotal record for Query 2
type Query2Record struct {
	Month    string
	ItemID   string
	Quantity string
	Subtotal string
}

// Query3Record represents a store_id, final_amount record for Query 3
type Query3Record struct {
	StoreID     string
	FinalAmount string
}

// NewChunkProcessor creates a new chunk processor for a specific query type
func NewChunkProcessor(queryType int) *ChunkProcessor {
	return &ChunkProcessor{
		queryType: queryType,
	}
}

// ProcessQuery4ChunkForCSV extracts user_id, store_id pairs from chunk for CSV append strategy
// Returns records that should be appended to CSV files, grouped by partition
func (cp *ChunkProcessor) ProcessQuery4ChunkForCSV(chunkMsg *chunk.Chunk, workerPartitions []int, numPartitions int) (map[int][]UserStoreRecord, error) {
	// Parse CSV data
	records, err := cp.parseCSV(chunkMsg.ChunkData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %v", err)
	}

	// Map partition -> []UserStoreRecord
	partitionRecords := make(map[int][]UserStoreRecord)

	processedCount := 0
	skippedCount := 0

	// Create a set of worker partitions for fast lookup
	workerPartitionSet := make(map[int]bool)
	for _, p := range workerPartitions {
		workerPartitionSet[p] = true
	}

	// Expected schema: transaction_id, store_id, payment_method_id, voucher_id, user_id,
	//                  original_amount, discount_applied, final_amount, created_at
	for _, record := range records {
		if len(record) < 9 {
			testing_utils.LogWarn("ChunkProcessor", "Skipping malformed record (insufficient fields): %v", record)
			continue
		}

		// Parse fields
		storeID := strings.TrimSpace(record[1])
		userID := strings.TrimSpace(record[4])

		// Skip if user_id is empty
		if userID == "" {
			testing_utils.LogWarn("ChunkProcessor", "Skipping record with empty user_id")
			continue
		}

		// Calculate partition for this record
		recordPartition, err := cp.calculatePartition(userID, numPartitions)
		if err != nil {
			testing_utils.LogWarn("ChunkProcessor", "Skipping record with invalid user_id %s: %v", userID, err)
			continue
		}

		// Only process records belonging to partitions this worker owns
		if !workerPartitionSet[recordPartition] {
			skippedCount++
			continue
		}

		// Add to partition records
		partitionRecords[recordPartition] = append(partitionRecords[recordPartition], UserStoreRecord{
			UserID:    userID,
			StoreID:   storeID,
			Partition: recordPartition,
		})
		processedCount++
	}

	testing_utils.LogInfo("ChunkProcessor", "Query 4: Extracted %d records across %d partitions, skipped %d records",
		processedCount, len(partitionRecords), skippedCount)
	return partitionRecords, nil
}

// ProcessQuery2ChunkForCSV extracts month, item_id, quantity, subtotal from chunk for CSV append strategy
// Returns records that should be appended to CSV files, grouped by partition
// Note: For Query 2, each worker handles one partition, so all records in the chunk belong to that partition
func (cp *ChunkProcessor) ProcessQuery2ChunkForCSV(chunkMsg *chunk.Chunk, workerPartitions []int) (map[int][]Query2Record, error) {
	// Parse CSV data
	records, err := cp.parseCSV(chunkMsg.ChunkData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %v", err)
	}

	// Map partition -> []Query2Record
	// For Query 2, typically one partition per worker, but we support multiple for consistency
	partitionRecords := make(map[int][]Query2Record)

	processedCount := 0
	skippedCount := 0

	// Create a set of worker partitions for fast lookup
	workerPartitionSet := make(map[int]bool)
	for _, p := range workerPartitions {
		workerPartitionSet[p] = true
	}

	// Expected schema: transaction_id, item_id, quantity, unit_price, subtotal, created_at
	for _, record := range records {
		if len(record) < 6 {
			testing_utils.LogWarn("ChunkProcessor", "Skipping malformed record (insufficient fields): %v", record)
			skippedCount++
			continue
		}

		// Parse fields
		itemID := strings.TrimSpace(record[1])
		quantity := strings.TrimSpace(record[2])
		subtotal := strings.TrimSpace(record[4])

		// Validate quantity and subtotal are not empty
		if quantity == "" || subtotal == "" {
			testing_utils.LogWarn("ChunkProcessor", "Skipping record with empty quantity or subtotal")
			skippedCount++
			continue
		}

		// Parse created_at to extract month
		createdAt, err := cp.parseDate(strings.TrimSpace(record[5]))
		if err != nil {
			testing_utils.LogWarn("ChunkProcessor", "Skipping record with invalid date: %v", err)
			skippedCount++
			continue
		}

		month := fmt.Sprintf("%d", createdAt.Month())

		// For Query 2, partition is determined by semester from created_at
		// Calculate partition based on year and semester
		year := createdAt.Year()
		semester := 1
		if createdAt.Month() >= 7 {
			semester = 2
		}

		var partition int
		switch {
		case year == 2024 && semester == 1:
			partition = 0
		case year == 2024 && semester == 2:
			partition = 1
		case year == 2025 && semester == 1:
			partition = 2
		default:
			testing_utils.LogWarn("ChunkProcessor", "Unexpected date %s (year=%d, semester=%d), assigning to partition 0", record[5], year, semester)
			partition = 0
		}

		// Only process records belonging to partitions this worker owns
		if !workerPartitionSet[partition] {
			skippedCount++
			continue
		}

		// Add to partition records
		partitionRecords[partition] = append(partitionRecords[partition], Query2Record{
			Month:    month,
			ItemID:   itemID,
			Quantity: quantity,
			Subtotal: subtotal,
		})
		processedCount++
	}

	testing_utils.LogInfo("ChunkProcessor", "Query 2: Extracted %d records across %d partitions, skipped %d records",
		processedCount, len(partitionRecords), skippedCount)
	return partitionRecords, nil
}

// ProcessQuery3ChunkForCSV extracts store_id, final_amount from chunk for CSV append strategy
// Returns records that should be appended to CSV files, grouped by partition
// Note: For Query 3, each worker handles one partition, so all records in the chunk belong to that partition
func (cp *ChunkProcessor) ProcessQuery3ChunkForCSV(chunkMsg *chunk.Chunk, workerPartitions []int) (map[int][]Query3Record, error) {
	// Parse CSV data
	records, err := cp.parseCSV(chunkMsg.ChunkData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %v", err)
	}

	// Map partition -> []Query3Record
	// For Query 3, typically one partition per worker, but we support multiple for consistency
	partitionRecords := make(map[int][]Query3Record)

	processedCount := 0
	skippedCount := 0

	// Create a set of worker partitions for fast lookup
	workerPartitionSet := make(map[int]bool)
	for _, p := range workerPartitions {
		workerPartitionSet[p] = true
	}

	// Expected schema: transaction_id, store_id, payment_method_id, voucher_id, user_id,
	//                  original_amount, discount_applied, final_amount, created_at
	for _, record := range records {
		if len(record) < 9 {
			testing_utils.LogWarn("ChunkProcessor", "Skipping malformed record (insufficient fields): %v", record)
			skippedCount++
			continue
		}

		// Parse fields
		storeID := strings.TrimSpace(record[1])
		finalAmount := strings.TrimSpace(record[7])

		// Validate fields are not empty
		if storeID == "" || finalAmount == "" {
			testing_utils.LogWarn("ChunkProcessor", "Skipping record with empty store_id or final_amount")
			skippedCount++
			continue
		}

		// For Query 3, partition is determined by semester from created_at
		// Parse created_at to determine partition
		createdAt, err := cp.parseDate(strings.TrimSpace(record[8]))
		if err != nil {
			testing_utils.LogWarn("ChunkProcessor", "Skipping record with invalid date: %v", err)
			skippedCount++
			continue
		}

		// Calculate partition based on year and semester
		year := createdAt.Year()
		semester := 1
		if createdAt.Month() >= 7 {
			semester = 2
		}

		var partition int
		switch {
		case year == 2024 && semester == 1:
			partition = 0
		case year == 2024 && semester == 2:
			partition = 1
		case year == 2025 && semester == 1:
			partition = 2
		default:
			testing_utils.LogWarn("ChunkProcessor", "Unexpected date %s (year=%d, semester=%d), assigning to partition 0", record[8], year, semester)
			partition = 0
		}

		// Only process records belonging to partitions this worker owns
		if !workerPartitionSet[partition] {
			skippedCount++
			continue
		}

		// Add to partition records
		partitionRecords[partition] = append(partitionRecords[partition], Query3Record{
			StoreID:     storeID,
			FinalAmount: finalAmount,
		})
		processedCount++
	}

	testing_utils.LogInfo("ChunkProcessor", "Query 3: Extracted %d records across %d partitions, skipped %d records",
		processedCount, len(partitionRecords), skippedCount)
	return partitionRecords, nil
}

// calculatePartition calculates the partition number for a user_id
func (cp *ChunkProcessor) calculatePartition(userID string, numPartitions int) (int, error) {
	// Parse user_id as integer (handle both int and float formats)
	userIDFloat, err := strconv.ParseFloat(userID, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid user_id %s: %v", userID, err)
	}
	userIDInt := int(userIDFloat)

	// Calculate partition using modulo
	return userIDInt % numPartitions, nil
}

// parseCSV parses CSV data and returns records (excluding header)
func (cp *ChunkProcessor) parseCSV(csvData string) ([][]string, error) {
	if csvData == "" {
		return [][]string{}, nil
	}

	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %v", err)
	}

	// Skip header row if present
	if len(records) > 0 {
		return records[1:], nil
	}

	return [][]string{}, nil
}

// parseDate parses a date string in various formats
func (cp *ChunkProcessor) parseDate(dateStr string) (time.Time, error) {
	// Try different date formats
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02",
		"2006/01/02",
		"2006-01-02T15:04:05Z",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, dateStr); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse date: %s", dateStr)
}
