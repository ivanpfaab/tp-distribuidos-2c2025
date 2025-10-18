package main

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
)

// Record represents a single data record
type Record struct {
	Fields []string
}

// PartitionerProcessor handles the processing logic for partitioning chunks
type PartitionerProcessor struct {
	queryType     int
	schema        []string
	partitions    [][]Record // One buffer per partition
	numPartitions int
	maxBufferSize int
}

// NewPartitionerProcessor creates a new processor for the specified query type
func NewPartitionerProcessor(queryType, numPartitions, maxBufferSize int) (*PartitionerProcessor, error) {
	// Get the appropriate schema for the query type
	schema := getSchemaForQueryType(queryType)
	if len(schema) == 0 {
		return nil, fmt.Errorf("unsupported query type: %d", queryType)
	}

	// Initialize partitions
	partitions := make([][]Record, numPartitions)
	for i := range partitions {
		partitions[i] = make([]Record, 0, maxBufferSize)
	}

	return &PartitionerProcessor{
		queryType:     queryType,
		schema:        schema,
		partitions:    partitions,
		numPartitions: numPartitions,
		maxBufferSize: maxBufferSize,
	}, nil
}

// ProcessChunk processes a chunk and partitions its data
func (p *PartitionerProcessor) ProcessChunk(chunkMessage *chunk.Chunk) error {
	testing_utils.LogInfo("Partitioner Processor", "Processing chunk %d for query type %d", chunkMessage.ChunkNumber, p.queryType)

	// Parse the chunk data (assuming CSV format)
	records, err := p.parseChunkData(chunkMessage.ChunkData)
	if err != nil {
		return fmt.Errorf("failed to parse chunk data: %v", err)
	}

	// Partition records based on user_id modulo
	partitionStats := make(map[int]int)
	for _, record := range records {
		partition, err := p.getPartition(record)
		if err != nil {
			testing_utils.LogWarn("Partitioner Processor", "Failed to get partition for record: %v", err)
			continue
		}

		// Add to partition buffer
		p.partitions[partition] = append(p.partitions[partition], record)
		partitionStats[partition]++

		// Flush partition if buffer is full
		if len(p.partitions[partition]) >= p.maxBufferSize {
			if err := p.flushPartition(partition, chunkMessage, false); err != nil {
				return fmt.Errorf("failed to flush partition %d: %v", partition, err)
			}
		}
	}

	testing_utils.LogInfo("Partitioner Processor", "Partitioned %d records across %d partitions: %v",
		len(records), p.numPartitions, partitionStats)

	// If this is the last chunk, flush all partitions
	if chunkMessage.IsLastChunk {
		testing_utils.LogInfo("Partitioner Processor", "Last chunk received, flushing all partitions")
		return p.flushAllPartitions(chunkMessage)
	}

	return nil
}

// parseChunkData parses the chunk data into individual records
func (p *PartitionerProcessor) parseChunkData(chunkData string) ([]Record, error) {
	if chunkData == "" {
		return []Record{}, nil
	}

	reader := csv.NewReader(strings.NewReader(chunkData))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	if len(records) < 1 {
		return []Record{}, nil
	}

	// Skip header row
	result := make([]Record, 0, len(records)-1)
	for i := 1; i < len(records); i++ {
		record := records[i]
		if len(record) < len(p.schema) {
			testing_utils.LogWarn("Partitioner Processor", "Skipping malformed record: %v", record)
			continue
		}

		// Clean up each field
		cleanedFields := make([]string, len(record))
		for j, field := range record {
			cleanedFields[j] = strings.TrimSpace(field)
		}

		result = append(result, Record{Fields: cleanedFields})
	}

	return result, nil
}

// getPartition calculates the partition for a record based on user_id modulo
func (p *PartitionerProcessor) getPartition(record Record) (int, error) {
	// Find user_id field index based on query type
	userIDIndex := p.getUserIDFieldIndex()
	if userIDIndex >= len(record.Fields) {
		return 0, fmt.Errorf("record does not have enough fields for user_id")
	}

	userID := record.Fields[userIDIndex]
	return getUserPartition(userID, p.numPartitions)
}

// getUserIDFieldIndex returns the index of the user_id field based on query type
func (p *PartitionerProcessor) getUserIDFieldIndex() int {
	switch p.queryType {
	case 2:
		// Query 2 doesn't have user_id, use item_id instead
		return 1 // item_id is at index 1
	case 3, 4:
		// Query 3 and 4 have user_id at index 4
		return 4
	default:
		return 0
	}
}

// flushPartition sends buffered records from a specific partition
func (p *PartitionerProcessor) flushPartition(partition int, originalChunk *chunk.Chunk, isLastChunk bool) error {
	if len(p.partitions[partition]) == 0 {
		// Empty partition, nothing to flush
		return nil
	}

	testing_utils.LogInfo("Partitioner Processor", "Flushed %d records from partition %d (IsLastChunk=%t)",
		len(p.partitions[partition]), partition, isLastChunk)

	// Clear partition buffer
	p.partitions[partition] = p.partitions[partition][:0]

	// TODO: Send partitioned data to the appropriate group by worker
	// This will be implemented in the next step

	return nil
}

// flushAllPartitions flushes all partition buffers
func (p *PartitionerProcessor) flushAllPartitions(originalChunk *chunk.Chunk) error {
	for partition := 0; partition < p.numPartitions; partition++ {
		if err := p.flushPartition(partition, originalChunk, true); err != nil {
			return fmt.Errorf("failed to flush partition %d: %v", partition, err)
		}
	}
	return nil
}

// partitionToCSV converts a partition buffer to CSV format
func (p *PartitionerProcessor) partitionToCSV(partition int) string {
	var result strings.Builder

	// Write header
	result.WriteString(strings.Join(p.schema, ","))
	result.WriteString("\n")

	// Write records
	for _, record := range p.partitions[partition] {
		result.WriteString(strings.Join(record.Fields, ","))
		result.WriteString("\n")
	}

	return result.String()
}

// getSchemaForQueryType returns the schema for the specified query type
func getSchemaForQueryType(queryType int) []string {
	switch queryType {
	case 2:
		return []string{
			"transaction_id",
			"item_id",
			"quantity",
			"unit_price",
			"subtotal",
			"created_at",
		}
	case 3:
		return []string{
			"transaction_id",
			"store_id",
			"payment_method_id",
			"voucher_id",
			"user_id",
			"original_amount",
			"discount_applied",
			"final_amount",
			"created_at",
		}
	case 4:
		return []string{
			"transaction_id",
			"store_id",
			"payment_method_id",
			"voucher_id",
			"user_id",
			"original_amount",
			"discount_applied",
			"final_amount",
			"created_at",
		}
	default:
		return []string{}
	}
}

// getUserPartition calculates the partition for a given ID (user_id or item_id)
func getUserPartition(id string, numPartitions int) (int, error) {
	// Parse ID (handle both int and float formats)
	idFloat, err := strconv.ParseFloat(id, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid ID %s: %w", id, err)
	}
	idInt := int(idFloat)

	// Simple modulo partitioning
	return idInt % numPartitions, nil
}
