package main

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// Record represents a single data record
type Record struct {
	Fields []string
}

// PartitionerProcessor handles the processing logic for partitioning chunks
type PartitionerProcessor struct {
	QueryType        int
	Schema           []string
	NumPartitions    int
	NumWorkers       int
	ExchangeProducer *exchange.ExchangeMiddleware
	ExchangeName     string
}

// NewPartitionerProcessor creates a new processor for the specified query type
func NewPartitionerProcessor(queryType, numPartitions, numWorkers int, connectionConfig interface{}) (*PartitionerProcessor, error) {
	// Get the appropriate schema for the query type using the shared schema
	schema := shared.GetSchemaForQueryType(queryType, shared.RawData)
	if len(schema) == 0 {
		return nil, fmt.Errorf("unsupported query type: %d", queryType)
	}

	// Get exchange name for this query
	exchangeName := queues.GetGroupByExchangeName(queryType)
	if exchangeName == "" {
		return nil, fmt.Errorf("no exchange found for query type %d", queryType)
	}

	// Create exchange producer
	var exchangeProducer *exchange.ExchangeMiddleware
	if connectionConfig != nil {
		// Type assert connectionConfig to *middleware.ConnectionConfig
		middlewareConfig, ok := connectionConfig.(*middleware.ConnectionConfig)
		if !ok {
			return nil, fmt.Errorf("connectionConfig is not of type *middleware.ConnectionConfig")
		}
		exchangeProducer = exchange.NewMessageMiddlewareExchange(exchangeName, []string{}, middlewareConfig)
		if exchangeProducer == nil {
			return nil, fmt.Errorf("failed to create exchange producer")
		}

		// Declare the topic exchange
		if err := exchangeProducer.DeclareExchange("topic", false, false, false, false); err != 0 {
			exchangeProducer.Close()
			return nil, fmt.Errorf("failed to declare topic exchange: %v", err)
		}
	}

	return &PartitionerProcessor{
		QueryType:        queryType,
		Schema:           schema,
		NumPartitions:    numPartitions,
		NumWorkers:       numWorkers,
		ExchangeProducer: exchangeProducer,
		ExchangeName:     exchangeName,
	}, nil
}

// ProcessChunk processes a chunk and sends partitioned data to workers
func (p *PartitionerProcessor) ProcessChunk(chunkMessage *chunk.Chunk) error {
	testing_utils.LogInfo("Partitioner Processor", "Processing chunk %d for query type %d", chunkMessage.ChunkNumber, p.QueryType)

	// Parse the chunk data (assuming CSV format)
	records, err := p.ParseChunkData(chunkMessage.ChunkData)
	if err != nil {
		return fmt.Errorf("failed to parse chunk data: %v", err)
	}

	// Partition records into temporary buffers (one per partition)
	partitionedRecords := make(map[int][]Record)
	for _, record := range records {
		partition, err := p.GetPartition(record)
		if err != nil {
			testing_utils.LogWarn("Partitioner Processor", "Failed to get partition for record: %v", err)
			continue
		}
		partitionedRecords[partition] = append(partitionedRecords[partition], record)
	}

	// Group partitions by worker and send one chunk per worker
	// Worker i gets partitions: i, i+numWorkers, i+2*numWorkers, ...
	for workerID := 0; workerID < p.NumWorkers; workerID++ {
		// Collect all records for this worker from its assigned partitions
		workerRecords := []Record{}
		for partition := workerID; partition < p.NumPartitions; partition += p.NumWorkers {
			if records, exists := partitionedRecords[partition]; exists {
				workerRecords = append(workerRecords, records...)
			}
		}

		// Send chunk to this worker
		if err := p.sendToWorker(workerID, workerRecords, chunkMessage); err != nil {
			return fmt.Errorf("failed to send to worker %d: %v", workerID, err)
		}
	}

	testing_utils.LogInfo("Partitioner Processor", "Sent chunk %d to %d workers (%d total records)",
		chunkMessage.ChunkNumber, p.NumWorkers, len(records))

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

	// Check if first row is a header by trying to validate it against expected schema
	// If validation fails, treat it as data instead of header
	startIndex := 0
	if err := p.ValidateHeader(records[0]); err == nil {
		// First row is a valid header, skip it
		testing_utils.LogInfo("Partitioner Processor", "Found header row, skipping it")
		startIndex = 1
	} else {
		// First row is not a header, treat all rows as data
		testing_utils.LogInfo("Partitioner Processor", "No header found, processing all rows as data")
	}

	// Process data records
	result := make([]Record, 0, len(records)-startIndex)
	for i := startIndex; i < len(records); i++ {
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
		return fmt.Errorf("schema field count mismatch: expected %d, got %d", len(p.Schema), len(header))
	}

	for i, expectedField := range p.Schema {
		actualField := strings.TrimSpace(header[i])
		if actualField != expectedField {
			return fmt.Errorf("schema field mismatch at position %d: expected '%s', got '%s'", i, expectedField, actualField)
		}
	}

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

// sendToWorker sends a chunk to a specific worker with correct metadata
func (p *PartitionerProcessor) sendToWorker(workerID int, records []Record, originalChunk *chunk.Chunk) error {
	// Convert records to CSV
	csvData := p.recordsToCSV(records)

	// Calculate chunk metadata
	// chunkNumber = (original_chunk.ChunkNumber - 1) * numWorkers + workerID
	newChunkNumber := (originalChunk.ChunkNumber-1)*p.NumWorkers + workerID

	// isLastChunk = original_chunk.isLastChunk && workerID == numWorkers - 1
	isLastChunk := originalChunk.IsLastChunk && (workerID == p.NumWorkers-1)

	// isLastFromTable = original_chunk.isLastFromTable && workerID == numWorkers - 1
	isLastFromTable := originalChunk.IsLastFromTable && (workerID == p.NumWorkers-1)

	// Create chunk for this worker
	workerChunk := chunk.NewChunk(
		originalChunk.ClientID,
		originalChunk.FileID,
		originalChunk.QueryType,
		newChunkNumber,
		isLastChunk,
		isLastFromTable,
		originalChunk.Step,
		len(csvData),
		originalChunk.TableID,
		csvData,
	)

	// Serialize the chunk
	chunkMsg := chunk.NewChunkMessage(workerChunk)
	serializedChunk, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
		return fmt.Errorf("failed to serialize chunk: %v", err)
	}

	// Get routing key for this worker
	routingKey := queues.GetGroupByWorkerRoutingKey(p.QueryType, workerID)
	if routingKey == "" {
		return fmt.Errorf("failed to get routing key for worker %d", workerID)
	}

	// Send to exchange with worker-specific routing key
	if p.ExchangeProducer != nil {
		if sendErr := p.ExchangeProducer.Send(serializedChunk, []string{routingKey}); sendErr != 0 {
			return fmt.Errorf("failed to send chunk to worker %d: error code %v", workerID, sendErr)
		}
		testing_utils.LogInfo("Partitioner Processor", "Sent chunk %d (%d records) to worker %d with routing key '%s' (IsLastChunk=%t, IsLastFromTable=%t)",
			newChunkNumber, len(records), workerID, routingKey, isLastChunk, isLastFromTable)
	}

	return nil
}

// recordsToCSV converts records to CSV format
func (p *PartitionerProcessor) recordsToCSV(records []Record) string {
	var result strings.Builder

	// Write header
	result.WriteString(strings.Join(p.Schema, ","))
	result.WriteString("\n")

	// Write records
	for _, record := range records {
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
