package partitioner

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared"
)

// Record represents a single data record
type Record struct {
	Fields []string
}

// PartitionerProcessor handles the processing logic for partitioning chunks
type PartitionerProcessor struct {
	QueryType     int
	Schema        []string
	Partitions    [][]Record // One buffer per partition
	NumPartitions int
	MaxBufferSize int
}

// NewPartitionerProcessor creates a new processor for the specified query type
func NewPartitionerProcessor(queryType, numPartitions, maxBufferSize int) (*PartitionerProcessor, error) {
	// Get the appropriate schema for the query type using the shared schema
	schema := shared.GetSchemaForQueryType(queryType, shared.RawData)
	if len(schema) == 0 {
		return nil, fmt.Errorf("unsupported query type: %d", queryType)
	}

	// Initialize partitions
	partitions := make([][]Record, numPartitions)
	for i := range partitions {
		partitions[i] = make([]Record, 0, maxBufferSize)
	}

	return &PartitionerProcessor{
		QueryType:     queryType,
		Schema:        schema,
		Partitions:    partitions,
		NumPartitions: numPartitions,
		MaxBufferSize: maxBufferSize,
	}, nil
}

// ProcessChunk processes a chunk and partitions its data
func (p *PartitionerProcessor) ProcessChunk(chunkMessage *chunk.Chunk) error {
	testing_utils.LogInfo("Partitioner Processor", "Processing chunk %d for query type %d", chunkMessage.ChunkNumber, p.QueryType)

	// Parse the chunk data (assuming CSV format)
	records, err := p.ParseChunkData(chunkMessage.ChunkData)
	if err != nil {
		return fmt.Errorf("failed to parse chunk data: %v", err)
	}

	// Partition records based on user_id modulo
	partitionStats := make(map[int]int)
	for _, record := range records {
		partition, err := p.GetPartition(record)
		if err != nil {
			testing_utils.LogWarn("Partitioner Processor", "Failed to get partition for record: %v", err)
			continue
		}

		// Add to partition buffer
		p.Partitions[partition] = append(p.Partitions[partition], record)
		partitionStats[partition]++

		// Flush partition if buffer is full
		if len(p.Partitions[partition]) >= p.MaxBufferSize {
			if err := p.FlushPartition(partition, chunkMessage, false); err != nil {
				return fmt.Errorf("failed to flush partition %d: %v", partition, err)
			}
		}
	}

	testing_utils.LogInfo("Partitioner Processor", "Partitioned %d records across %d partitions: %v",
		len(records), p.NumPartitions, partitionStats)

	// If this is the last chunk, flush all partitions
	if chunkMessage.IsLastChunk {
		testing_utils.LogInfo("Partitioner Processor", "Last chunk received, flushing all partitions")
		return p.flushAllPartitions(chunkMessage)
	}

	return nil
}

// parseChunkData parses the chunk data into individual records
func (p *PartitionerProcessor) ParseChunkData(chunkData string) ([]Record, error) {
	if chunkData == "" {
		return []Record{}, nil
	}

	reader := csv.NewReader(strings.NewReader(chunkData))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %v", err)
	}

	if len(records) < 1 {
		return []Record{}, nil
	}

	// Validate header row against expected schema
	if err := p.ValidateHeader(records[0]); err != nil {
		return nil, fmt.Errorf("schema validation failed: %v", err)
	}

	// Skip header row and process data records
	result := make([]Record, 0, len(records)-1)
	for i := 1; i < len(records); i++ {
		record := records[i]
		if len(record) < len(p.Schema) {
			testing_utils.LogWarn("Partitioner Processor", "Skipping malformed record (insufficient fields): %v", record)
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
func (p *PartitionerProcessor) GetPartition(record Record) (int, error) {
	// Find user_id field index based on query type
	userIDIndex := p.GetUserIDFieldIndex()
	if userIDIndex >= len(record.Fields) {
		return 0, fmt.Errorf("record does not have enough fields for user_id")
	}

	userID := record.Fields[userIDIndex]
	return GetUserPartition(userID, p.NumPartitions)
}

// validateHeader validates that the CSV header matches the expected schema
func (p *PartitionerProcessor) ValidateHeader(header []string) error {
	if len(header) != len(p.Schema) {
		testing_utils.LogError("Partitioner Processor", "Schema mismatch: expected %d fields, got %d. Expected: %v, Got: %v",
			len(p.Schema), len(header), p.Schema, header)
		return fmt.Errorf("schema field count mismatch: expected %d, got %d", len(p.Schema), len(header))
	}

	for i, expectedField := range p.Schema {
		actualField := strings.TrimSpace(header[i])
		if actualField != expectedField {
			testing_utils.LogError("Partitioner Processor", "Schema field mismatch at position %d: expected '%s', got '%s'. Expected schema: %v, Got header: %v",
				i, expectedField, actualField, p.Schema, header)
			return fmt.Errorf("schema field mismatch at position %d: expected '%s', got '%s'", i, expectedField, actualField)
		}
	}

	testing_utils.LogInfo("Partitioner Processor", "Schema validation passed for query type %d", p.QueryType)
	return nil
}

// getUserIDFieldIndex returns the index of the user_id field based on query type
func (p *PartitionerProcessor) GetUserIDFieldIndex() int {
	switch p.QueryType {
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
func (p *PartitionerProcessor) FlushPartition(partition int, originalChunk *chunk.Chunk, isLastChunk bool) error {
	if len(p.Partitions[partition]) == 0 {
		// Empty partition, nothing to flush
		return nil
	}

	testing_utils.LogInfo("Partitioner Processor", "Flushed %d records from partition %d (IsLastChunk=%t)",
		len(p.Partitions[partition]), partition, isLastChunk)

	// Clear partition buffer
	p.Partitions[partition] = p.Partitions[partition][:0]

	// TODO: Send partitioned data to the appropriate group by worker
	// This will be implemented in the next step

	return nil
}

// flushAllPartitions flushes all partition buffers
func (p *PartitionerProcessor) flushAllPartitions(originalChunk *chunk.Chunk) error {
	for partition := 0; partition < p.NumPartitions; partition++ {
		if err := p.FlushPartition(partition, originalChunk, true); err != nil {
			return fmt.Errorf("failed to flush partition %d: %v", partition, err)
		}
	}
	return nil
}

// partitionToCSV converts a partition buffer to CSV format
func (p *PartitionerProcessor) PartitionToCSV(partition int) string {
	var result strings.Builder

	// Write header
	result.WriteString(strings.Join(p.Schema, ","))
	result.WriteString("\n")

	// Write records
	for _, record := range p.Partitions[partition] {
		result.WriteString(strings.Join(record.Fields, ","))
		result.WriteString("\n")
	}

	return result.String()
}

// GetUserPartition calculates the partition for a given ID (user_id or item_id)
func GetUserPartition(id string, NumPartitions int) (int, error) {
	// Parse ID (handle both int and float formats)
	idFloat, err := strconv.ParseFloat(id, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid ID %s: %v", id, err)
	}
	idInt := int(idFloat)

	// Simple modulo partitioning
	return idInt % NumPartitions, nil
}
