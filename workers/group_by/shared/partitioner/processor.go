package main

import (
	"encoding/csv"
	"fmt"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/common"
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
	// testing_utils.LogInfo("Partitioner Processor", "Processing chunk %d for query type %d", chunkMessage.ChunkNumber, p.QueryType)

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
			continue
		}
		partitionedRecords[partition] = append(partitionedRecords[partition], record)
	}

	// Group partitions by worker and send one chunk per worker
	// Modulo-based distribution for all queries (many partitions distributed across workers)
	// Worker i gets partitions where: partition % numWorkers == (workerID % numWorkers)
	// Note: workerID starts from 1 (not 0) to match docker-compose configuration
	// Worker 1: partitions 1, 4, 7, 10, ... (partition % 3 == 1)
	// Worker 2: partitions 2, 5, 8, 11, ... (partition % 3 == 2)
	// Worker 3: partitions 0, 3, 6, 9, ... (partition % 3 == 0)
	for workerID := 1; workerID <= p.NumWorkers; workerID++ {
		// Collect all records for this worker from its assigned partitions
		workerRecords := []Record{}
		targetRemainder := workerID % p.NumWorkers
		for partition := 0; partition < p.NumPartitions; partition++ {
			if partition%p.NumWorkers == targetRemainder {
				if records, exists := partitionedRecords[partition]; exists {
					workerRecords = append(workerRecords, records...)
				}
			}
		}

		// Send chunk to this worker
		if err := p.sendToWorker(workerID, workerRecords, chunkMessage); err != nil {
			return fmt.Errorf("failed to send to worker %d: %v", workerID, err)
		}
	}

	// testing_utils.LogInfo("Partitioner Processor", "Sent chunk %d to %d workers (%d total records)",
	// 	chunkMessage.ChunkNumber, p.NumWorkers, len(records))

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
		startIndex = 1
	}

	// Process data records
	result := make([]Record, 0, len(records)-startIndex)
	for i := startIndex; i < len(records); i++ {
		record := records[i]
		if len(record) < len(p.Schema) {
			testing_utils.LogWarn("Partitioner Processor", "Skipping malformed record (insufficient fields): %v", record)
			continue
		}

		// CSV parser already handles whitespace correctly, use fields directly
		result = append(result, Record{Fields: record})
	}

	return result, nil
}

// getTimeBasedPartition calculates partition based on semester from created_at field
// Uses common partition calculation utility
func (p *PartitionerProcessor) getTimeBasedPartition(record Record, createdAtIndex int) (int, error) {
	if createdAtIndex >= len(record.Fields) {
		return 0, fmt.Errorf("record does not have created_at field at index %d", createdAtIndex)
	}

	createdAtStr := record.Fields[createdAtIndex]
	if createdAtStr == "" {
		return 0, fmt.Errorf("created_at field is empty")
	}

	// Parse date using common utility
	createdAt, err := common.ParseDate(createdAtStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse created_at '%s': %v", createdAtStr, err)
	}

	// Calculate partition using common utility
	return common.CalculateTimeBasedPartition(createdAt), nil
}

// getPartition calculates the partition for a record based on query type
func (p *PartitionerProcessor) GetPartition(record Record) (int, error) {
	switch p.QueryType {
	case 2:
		// Query 2: Partition by composite key (year, month, item_id)
		// This ensures all records for the same year+month+item_id go to the same partition
		// Schema: transaction_id, item_id, quantity, unit_price, subtotal, created_at
		if len(record.Fields) < 6 {
			return 0, fmt.Errorf("record does not have enough fields for Query 2")
		}

		// Extract year and month from created_at (index 5)
		// Format: "2024-01-15 10:30:00"
		createdAt := record.Fields[5]
		if len(createdAt) < 7 {
			return 0, fmt.Errorf("invalid created_at format: %s", createdAt)
		}
		year := createdAt[0:4]  // "2024"
		month := createdAt[5:7] // "01"
		itemID := record.Fields[1]

		// Composite key partitioning
		return common.CalculateCompositeHashPartition([]string{year, month, itemID}, p.NumPartitions)

	case 3:
		// Query 3: Partition by composite key (year, semester, store_id)
		// This ensures all records for the same year+semester+store_id go to the same partition
		// Schema: transaction_id, store_id, payment_method_id, voucher_id,
		//         user_id, original_amount, discount_applied, final_amount, created_at
		if len(record.Fields) < 9 {
			return 0, fmt.Errorf("record does not have enough fields for Query 3")
		}

		// Extract year and month from created_at (index 8)
		// Format: "2024-01-15 10:30:00"
		createdAt := record.Fields[8]
		if len(createdAt) < 7 {
			return 0, fmt.Errorf("invalid created_at format: %s", createdAt)
		}
		year := createdAt[0:4]     // "2024"
		monthStr := createdAt[5:7] // "01"

		// Calculate semester (1 or 2)
		semester := "1"
		if len(monthStr) == 2 {
			if monthStr[0] == '0' && monthStr[1] >= '7' { // "07", "08", "09"
				semester = "2"
			} else if monthStr[0] == '1' { // "10", "11", "12"
				semester = "2"
			}
		}

		storeID := record.Fields[1]

		// Composite key partitioning
		return common.CalculateCompositeHashPartition([]string{year, semester, storeID}, p.NumPartitions)

	case 4:
		// Query 4: User-based partitioning (user_id is at index 4)
		if 4 >= len(record.Fields) {
			return 0, fmt.Errorf("record does not have enough fields for user_id")
		}
		userID := record.Fields[4]
		return common.CalculateUserBasedPartition(userID, p.NumPartitions)

	default:
		return 0, fmt.Errorf("unsupported query type: %d", p.QueryType)
	}
}

// validateHeader validates that the CSV header matches the expected schema
func (p *PartitionerProcessor) ValidateHeader(header []string) error {
	return common.ValidateHeader(header, p.Schema)
}

// sendToWorker sends a chunk to a specific worker with correct metadata
func (p *PartitionerProcessor) sendToWorker(workerID int, records []Record, originalChunk *chunk.Chunk) error {
	// Convert records to CSV
	csvData := p.recordsToCSV(records)

	// Calculate chunk metadata
	// chunkNumber = (original_chunk.ChunkNumber - 1) * numWorkers + workerID
	// With workerID starting from 1: chunk 1, 2, 3, 4, 5, 6, ...
	newChunkNumber := (originalChunk.ChunkNumber-1)*p.NumWorkers + workerID

	// isLastChunk = original_chunk.isLastChunk && workerID == numWorkers
	// Since workerID starts from 1, the last worker has workerID == numWorkers
	isLastChunk := originalChunk.IsLastChunk && (workerID == p.NumWorkers)

	// isLastFromTable = original_chunk.isLastFromTable && workerID == numWorkers
	isLastFromTable := originalChunk.IsLastFromTable && (workerID == p.NumWorkers)

	// Create chunk for this worker
	workerChunk := chunk.NewChunk(
		originalChunk.ClientID,
		originalChunk.FileID,
		originalChunk.QueryType,
		newChunkNumber,
		isLastChunk,
		isLastFromTable,
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
		// testing_utils.LogInfo("Partitioner Processor", "Sent chunk %d (%d records) to worker %d with routing key '%s' (IsLastChunk=%t, IsLastFromTable=%t)",
		// 	newChunkNumber, len(records), workerID, routingKey, isLastChunk, isLastFromTable)
	}

	return nil
}

// recordsToCSV converts records to CSV format
func (p *PartitionerProcessor) recordsToCSV(records []Record) string {
	if len(records) == 0 {
		var result strings.Builder
		for i, field := range p.Schema {
			if i > 0 {
				result.WriteByte(',')
			}
			result.WriteString(field)
		}
		result.WriteByte('\n')
		return result.String()
	}

	// Calculate exact size needed to avoid reallocation
	// Header size: sum of field lengths + commas + newline
	headerSize := len(p.Schema) - 1 // commas between fields
	for _, field := range p.Schema {
		headerSize += len(field)
	}
	headerSize++ // newline

	// Estimate record size from first record
	firstRecordSize := len(records[0].Fields) - 1 // commas
	for _, field := range records[0].Fields {
		firstRecordSize += len(field)
	}
	firstRecordSize++ // newline

	// Total size = header + (average record size * record count)
	estimatedSize := headerSize + (firstRecordSize * len(records))

	var result strings.Builder
	result.Grow(estimatedSize)

	// Write header - avoid strings.Join
	for i, field := range p.Schema {
		if i > 0 {
			result.WriteByte(',')
		}
		result.WriteString(field)
	}
	result.WriteByte('\n')

	// Write records - avoid strings.Join
	for _, record := range records {
		for i, field := range record.Fields {
			if i > 0 {
				result.WriteByte(',')
			}
			result.WriteString(field)
		}
		result.WriteByte('\n')
	}

	return result.String()
}

// GetUserPartition calculates the partition for a given ID (user_id or item_id)
// Deprecated: Use common.CalculateUserBasedPartition instead
func GetUserPartition(id string, NumPartitions int) (int, error) {
	return common.CalculateUserBasedPartition(id, NumPartitions)
}
