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
	UserID  string
	StoreID string
	Partition int
}

// NewChunkProcessor creates a new chunk processor for a specific query type
func NewChunkProcessor(queryType int) *ChunkProcessor {
	return &ChunkProcessor{
		queryType: queryType,
	}
}

// ProcessChunk processes a chunk and updates the aggregated data
// For Q4, filters records to only process those belonging to the specified partition
// Returns the updated data map
func (cp *ChunkProcessor) ProcessChunk(chunkMsg *chunk.Chunk, currentData map[string]interface{}, partition int, numPartitions int) (map[string]interface{}, error) {
	// testing_utils.LogInfo("ChunkProcessor", "Processing chunk %d for query %d, partition %d (%d bytes)",
	//	chunkMsg.ChunkNumber, cp.queryType, partition, len(chunkMsg.ChunkData))

	switch cp.queryType {
	case 2:
		return cp.processQuery2(chunkMsg, currentData)
	case 3:
		return cp.processQuery3(chunkMsg, currentData)
	case 4:
		return cp.processQuery4(chunkMsg, currentData, partition, numPartitions)
	default:
		return nil, fmt.Errorf("unsupported query type: %d", cp.queryType)
	}
}

// processQuery2 processes Query 2 chunks (group by month + item_id)
// Data structure: { "month": { "item_id": { "total_quantity": int, "total_subtotal": float, "count": int } } }
func (cp *ChunkProcessor) processQuery2(chunkMsg *chunk.Chunk, currentData map[string]interface{}) (map[string]interface{}, error) {
	// Parse CSV data
	records, err := cp.parseCSV(chunkMsg.ChunkData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %v", err)
	}

	// Expected schema: transaction_id, item_id, quantity, unit_price, subtotal, created_at
	for _, record := range records {
		if len(record) < 6 {
			testing_utils.LogWarn("ChunkProcessor", "Skipping malformed record (insufficient fields): %v", record)
			continue
		}

		// Parse fields
		itemID := strings.TrimSpace(record[1])
		quantity, err := strconv.Atoi(strings.TrimSpace(record[2]))
		if err != nil {
			testing_utils.LogWarn("ChunkProcessor", "Skipping record with invalid quantity: %v", err)
			continue
		}

		subtotal, err := strconv.ParseFloat(strings.TrimSpace(record[4]), 64)
		if err != nil {
			testing_utils.LogWarn("ChunkProcessor", "Skipping record with invalid subtotal: %v", err)
			continue
		}

		// Parse created_at to extract month
		createdAt, err := cp.parseDate(strings.TrimSpace(record[5]))
		if err != nil {
			testing_utils.LogWarn("ChunkProcessor", "Skipping record with invalid date: %v", err)
			continue
		}

		month := fmt.Sprintf("%d", createdAt.Month())

		// Get or create month map
		monthMap, exists := currentData[month]
		if !exists {
			monthMap = make(map[string]interface{})
			currentData[month] = monthMap
		}
		monthData := monthMap.(map[string]interface{})

		// Get or create item data
		itemData, exists := monthData[itemID]
		if !exists {
			itemData = map[string]interface{}{
				"total_quantity": 0,
				"total_subtotal": 0.0,
				"count":          0,
			}
			monthData[itemID] = itemData
		}
		item := itemData.(map[string]interface{})

		// Aggregate (handle both int and float64 from JSON unmarshaling)
		item["total_quantity"] = cp.toInt(item["total_quantity"]) + quantity
		item["total_subtotal"] = cp.toFloat64(item["total_subtotal"]) + subtotal
		item["count"] = cp.toInt(item["count"]) + 1
	}

	testing_utils.LogInfo("ChunkProcessor", "Query 2: Processed %d records", len(records))
	return currentData, nil
}

// processQuery3 processes Query 3 chunks (group by store_id)
// Data structure: { "store_id": { "total_final_amount": float, "count": int } }
func (cp *ChunkProcessor) processQuery3(chunkMsg *chunk.Chunk, currentData map[string]interface{}) (map[string]interface{}, error) {
	// Parse CSV data
	records, err := cp.parseCSV(chunkMsg.ChunkData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %v", err)
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
		finalAmount, err := strconv.ParseFloat(strings.TrimSpace(record[7]), 64)
		if err != nil {
			testing_utils.LogWarn("ChunkProcessor", "Skipping record with invalid final_amount: %v", err)
			continue
		}

		// Get or create store data
		storeData, exists := currentData[storeID]
		if !exists {
			storeData = map[string]interface{}{
				"total_final_amount": 0.0,
				"count":              0,
			}
			currentData[storeID] = storeData
		}
		store := storeData.(map[string]interface{})

		// Aggregate (handle both int and float64 from JSON unmarshaling)
		store["total_final_amount"] = cp.toFloat64(store["total_final_amount"]) + finalAmount
		store["count"] = cp.toInt(store["count"]) + 1
	}

	testing_utils.LogInfo("ChunkProcessor", "Query 3: Processed %d records", len(records))
	return currentData, nil
}

// processQuery4 processes Query 4 chunks (group by user_id + store_id)
// For Query 4, we now use CSV append strategy - this method is kept for compatibility but returns empty data
// The actual processing is done by ProcessQuery4ChunkForCSV
func (cp *ChunkProcessor) processQuery4(chunkMsg *chunk.Chunk, currentData map[string]interface{}, targetPartition int, numPartitions int) (map[string]interface{}, error) {
	// For Query 4, we use CSV append strategy, so we don't need to aggregate in memory
	// Return empty data to maintain compatibility
	return make(map[string]interface{}), nil
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

// toInt converts interface{} to int, handling both int and float64 (from JSON unmarshaling)
func (cp *ChunkProcessor) toInt(val interface{}) int {
	switch v := val.(type) {
	case int:
		return v
	case float64:
		return int(v)
	case int64:
		return int(v)
	default:
		testing_utils.LogWarn("ChunkProcessor", "Unexpected type for int conversion: %T, defaulting to 0", val)
		return 0
	}
}

// toFloat64 converts interface{} to float64, handling both float64 and int (for consistency)
func (cp *ChunkProcessor) toFloat64(val interface{}) float64 {
	switch v := val.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	default:
		testing_utils.LogWarn("ChunkProcessor", "Unexpected type for float64 conversion: %T, defaulting to 0.0", val)
		return 0.0
	}
}
