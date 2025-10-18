package main

import (
	"fmt"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
)

// PartitionerProcessor handles the processing logic for partitioning chunks
type PartitionerProcessor struct {
	queryType int
	schema    []string
	records   [][]string // In-memory storage for chunk data
}

// NewPartitionerProcessor creates a new processor for the specified query type
func NewPartitionerProcessor(queryType int) (*PartitionerProcessor, error) {
	// Get the appropriate schema for the query type
	schema := getSchemaForQueryType(queryType)
	if len(schema) == 0 {
		return nil, fmt.Errorf("unsupported query type: %d", queryType)
	}

	return &PartitionerProcessor{
		queryType: queryType,
		schema:    schema,
		records:   make([][]string, 0),
	}, nil
}

// ProcessChunk processes a chunk and stores its data in memory
func (p *PartitionerProcessor) ProcessChunk(chunkMessage *chunk.Chunk) error {
	testing_utils.LogInfo("Partitioner Processor", "Processing chunk %d for query type %d", chunkMessage.ChunkNumber, p.queryType)

	// Parse the chunk data (assuming CSV format)
	records, err := p.parseChunkData(chunkMessage.ChunkData)
	if err != nil {
		return fmt.Errorf("failed to parse chunk data: %v", err)
	}

	// Store records in memory
	p.records = append(p.records, records...)

	testing_utils.LogInfo("Partitioner Processor", "Stored %d records from chunk %d. Total records in memory: %d",
		len(records), chunkMessage.ChunkNumber, len(p.records))

	return nil
}

// parseChunkData parses the chunk data into individual records
func (p *PartitionerProcessor) parseChunkData(chunkData string) ([][]string, error) {
	if chunkData == "" {
		return [][]string{}, nil
	}

	lines := strings.Split(chunkData, "\n")
	records := make([][]string, 0, len(lines))

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Split by comma and clean up each field
		fields := strings.Split(line, ",")
		cleanedFields := make([]string, len(fields))
		for i, field := range fields {
			cleanedFields[i] = strings.TrimSpace(field)
		}

		records = append(records, cleanedFields)
	}

	return records, nil
}

// GetStoredRecords returns the currently stored records
func (p *PartitionerProcessor) GetStoredRecords() [][]string {
	return p.records
}

// GetRecordCount returns the number of stored records
func (p *PartitionerProcessor) GetRecordCount() int {
	return len(p.records)
}

// ClearRecords clears the stored records
func (p *PartitionerProcessor) ClearRecords() {
	p.records = make([][]string, 0)
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
