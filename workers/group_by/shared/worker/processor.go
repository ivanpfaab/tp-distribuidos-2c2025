package main

import (
	"encoding/csv"
	"fmt"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared"
)

// RecordExtractor defines how to extract and validate a record for a specific query type
type RecordExtractor interface {
	// ExtractRecord extracts a record from CSV row, returns partition and extracted record (or error)
	ExtractRecord(record []string, workerPartitions map[int]bool, numPartitions int) (partition int, extractedRecord interface{}, err error)
	// GetMinFieldCount returns minimum number of fields required
	GetMinFieldCount() int
	// GetQueryName returns query name for logging
	GetQueryName() string
}

// ChunkProcessor handles processing of chunks and aggregation of data
type ChunkProcessor struct {
	queryType int
}

// NewChunkProcessor creates a new chunk processor for a specific query type
func NewChunkProcessor(queryType int) *ChunkProcessor {
	return &ChunkProcessor{
		queryType: queryType,
	}
}

// processChunkForCSV is a generic method that processes chunks using a RecordExtractor
func (cp *ChunkProcessor) processChunkForCSV(chunkMsg *chunk.Chunk, workerPartitions []int, numPartitions int, extractor RecordExtractor) (map[int][]interface{}, error) {
	// Parse CSV data
	records, err := cp.parseCSV(chunkMsg.ChunkData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %v", err)
	}

	// Map partition -> []records
	partitionRecords := make(map[int][]interface{})

	processedCount := 0
	skippedCount := 0

	// Create a set of worker partitions for fast lookup
	workerPartitionSet := make(map[int]bool)
	for _, p := range workerPartitions {
		workerPartitionSet[p] = true
	}

	// Process each record
	for _, record := range records {
		if len(record) < extractor.GetMinFieldCount() {
			testing_utils.LogWarn("ChunkProcessor", "Skipping malformed record (insufficient fields): %v", record)
			skippedCount++
			continue
		}

		// Extract record using the extractor
		partition, rec, err := extractor.ExtractRecord(record, workerPartitionSet, numPartitions)
		if err != nil {
			// Log warning for non-critical errors (partition not owned, empty fields, etc.)
			testing_utils.LogWarn("ChunkProcessor", "Skipping record: %v", err)
			skippedCount++
			continue
		}

		// Add to partition records
		partitionRecords[partition] = append(partitionRecords[partition], rec)
		processedCount++
	}

	testing_utils.LogInfo("ChunkProcessor", "%s: Extracted %d records across %d partitions, skipped %d records",
		extractor.GetQueryName(), processedCount, len(partitionRecords), skippedCount)
	return partitionRecords, nil
}

// ProcessQuery4ChunkForCSV extracts user_id, store_id pairs from chunk for CSV append strategy
// Returns records that should be appended to CSV files, grouped by partition
func (cp *ChunkProcessor) ProcessQuery4ChunkForCSV(chunkMsg *chunk.Chunk, workerPartitions []int, numPartitions int) (map[int][]shared.Query4Record, error) {
	extractor := &Query4RecordExtractor{}
	partitionRecords, err := cp.processChunkForCSV(chunkMsg, workerPartitions, numPartitions, extractor)
	if err != nil {
		return nil, err
	}

	// Convert map[int][]interface{} to map[int][]shared.Query4Record
	result := make(map[int][]shared.Query4Record)
	for partition, records := range partitionRecords {
		result[partition] = make([]shared.Query4Record, len(records))
		for i, rec := range records {
			result[partition][i] = rec.(shared.Query4Record)
		}
	}
	return result, nil
}

// ProcessQuery2ChunkForCSV extracts month, item_id, quantity, subtotal from chunk for CSV append strategy
// Returns records that should be appended to CSV files, grouped by partition
// Note: For Query 2, each worker handles one partition, so all records in the chunk belong to that partition
func (cp *ChunkProcessor) ProcessQuery2ChunkForCSV(chunkMsg *chunk.Chunk, workerPartitions []int) (map[int][]shared.Query2Record, error) {
	extractor := &Query2RecordExtractor{}
	partitionRecords, err := cp.processChunkForCSV(chunkMsg, workerPartitions, 0, extractor) // numPartitions not used for Q2
	if err != nil {
		return nil, err
	}

	// Convert map[int][]interface{} to map[int][]shared.Query2Record
	result := make(map[int][]shared.Query2Record)
	for partition, records := range partitionRecords {
		result[partition] = make([]shared.Query2Record, len(records))
		for i, rec := range records {
			result[partition][i] = rec.(shared.Query2Record)
		}
	}
	return result, nil
}

// ProcessQuery3ChunkForCSV extracts store_id, final_amount from chunk for CSV append strategy
// Returns records that should be appended to CSV files, grouped by partition
// Note: For Query 3, each worker handles one partition, so all records in the chunk belong to that partition
func (cp *ChunkProcessor) ProcessQuery3ChunkForCSV(chunkMsg *chunk.Chunk, workerPartitions []int) (map[int][]shared.Query3Record, error) {
	extractor := &Query3RecordExtractor{}
	partitionRecords, err := cp.processChunkForCSV(chunkMsg, workerPartitions, 0, extractor) // numPartitions not used for Q3
	if err != nil {
		return nil, err
	}

	// Convert map[int][]interface{} to map[int][]shared.Query3Record
	result := make(map[int][]shared.Query3Record)
	for partition, records := range partitionRecords {
		result[partition] = make([]shared.Query3Record, len(records))
		for i, rec := range records {
			result[partition][i] = rec.(shared.Query3Record)
		}
	}
	return result, nil
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
