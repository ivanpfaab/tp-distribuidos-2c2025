package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/common"
)

// GroupByWorker handles group by operations for partitioned chunks
type GroupByWorker struct {
	config               *WorkerConfig
	consumer             *exchange.ExchangeConsumer
	orchestratorProducer *exchange.ExchangeMiddleware
	workerIDStr          string
	fileManager          *FileManager
	processor            *ChunkProcessor
}

// NewGroupByWorker creates a new group by worker instance
func NewGroupByWorker(config *WorkerConfig) (*GroupByWorker, error) {
	// Create exchange consumer for the routing keys this worker handles
	consumer := exchange.NewExchangeConsumer(config.ExchangeName, config.RoutingKeys, config.ConnectionConfig)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create exchange consumer")
	}

	// Declare the topic exchange
	exchangeDeclarer := exchange.NewMessageMiddlewareExchange(config.ExchangeName, []string{}, config.ConnectionConfig)
	if exchangeDeclarer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create exchange declarer")
	}
	if err := exchangeDeclarer.DeclareExchange("topic", false, false, false, false); err != 0 {
		consumer.Close()
		exchangeDeclarer.Close()
		return nil, fmt.Errorf("failed to declare topic exchange: %v", err)
	}
	exchangeDeclarer.Close()

	// Create producer for orchestrator chunk notifications (fanout exchange)
	orchestratorProducer := exchange.NewMessageMiddlewareExchange(config.OrchestratorExchange, []string{}, config.ConnectionConfig)
	if orchestratorProducer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create orchestrator exchange producer")
	}

	// Declare fanout exchange for orchestrator notifications
	if err := orchestratorProducer.DeclareExchange("fanout", false, false, false, false); err != 0 {
		consumer.Close()
		orchestratorProducer.Close()
		return nil, fmt.Errorf("failed to declare orchestrator fanout exchange: %v", err)
	}

	// Generate worker ID string
	workerIDStr := fmt.Sprintf("query%d-groupby-worker-%d", config.QueryType, config.WorkerID)

	// Create file manager and processor
	fileManager := NewFileManager(config.QueryType, config.WorkerID)
	processor := NewChunkProcessor(config.QueryType)

	return &GroupByWorker{
		config:               config,
		consumer:             consumer,
		orchestratorProducer: orchestratorProducer,
		workerIDStr:          workerIDStr,
		fileManager:          fileManager,
		processor:            processor,
	}, nil
}

// Start begins processing messages from the exchange
func (w *GroupByWorker) Start() middleware.MessageMiddlewareError {
	testing_utils.LogInfo("GroupBy Worker", "Starting worker for query %d, listening to routing keys: %v",
		w.config.QueryType, w.config.RoutingKeys)

	// Start consuming messages
	return w.consumer.StartConsuming(w.createCallback())
}

// createCallback creates the message processing callback
func (w *GroupByWorker) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		testing_utils.LogInfo("GroupBy Worker", "Callback started, waiting for messages...")
		messageCount := 0
		for delivery := range *consumeChannel {
			messageCount++
			// testing_utils.LogInfo("GroupBy Worker", "Received message #%d", messageCount)

			// Process the message
			if err := w.processMessage(delivery.Body); err != nil {
				testing_utils.LogError("GroupBy Worker", "Failed to process message: %v", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		testing_utils.LogInfo("GroupBy Worker", "Consume channel closed after processing %d messages", messageCount)
		done <- nil
	}
}

// processMessage processes a single message
func (w *GroupByWorker) processMessage(messageBody []byte) error {
	// Deserialize the message
	message, err := deserializer.Deserialize(messageBody)
	if err != nil {
		testing_utils.LogWarn("GroupBy Worker", "Failed to deserialize message: %v", err)
		return err
	}

	// Check if it's a chunk message
	chunkMessage, ok := message.(*chunk.Chunk)
	if !ok {
		testing_utils.LogWarn("GroupBy Worker", "Received non-chunk message, skipping")
		return nil
	}

	// Process the chunk (dummy group by for now)
	return w.processChunk(chunkMessage)
}

// RecordAppender defines how to append records to CSV for a specific query type
type RecordAppender interface {
	AppendRecords(clientID string, partition int, records interface{}) error
}

// Query2RecordAppender appends Query2Record to CSV
type Query2RecordAppender struct {
	fileManager *FileManager
}

func (a *Query2RecordAppender) AppendRecords(clientID string, partition int, records interface{}) error {
	return a.fileManager.AppendQuery2RecordsToPartitionCSV(clientID, partition, records.([]shared.Query2Record))
}

// Query3RecordAppender appends Query3Record to CSV
type Query3RecordAppender struct {
	fileManager *FileManager
}

func (a *Query3RecordAppender) AppendRecords(clientID string, partition int, records interface{}) error {
	return a.fileManager.AppendQuery3RecordsToPartitionCSV(clientID, partition, records.([]shared.Query3Record))
}

// Query4RecordAppender appends Query4Record to CSV
type Query4RecordAppender struct {
	fileManager *FileManager
}

func (a *Query4RecordAppender) AppendRecords(clientID string, partition int, records interface{}) error {
	return a.fileManager.AppendRecordsToPartitionCSV(clientID, partition, records.([]shared.Query4Record))
}

// processChunk processes a single chunk with file-based group by aggregation
func (w *GroupByWorker) processChunk(chunkMessage *chunk.Chunk) error {
	// testing_utils.LogInfo("GroupBy Worker", "Processing chunk %d from client %s, file %s (IsLastChunk=%t, IsLastFromTable=%t)",
	// 	chunkMessage.ChunkNumber, chunkMessage.ClientID, chunkMessage.FileID,
	// 	chunkMessage.IsLastChunk, chunkMessage.IsLastFromTable)

	// Determine partition(s) for this worker
	partitions := w.getPartitionsForWorker()

	// Query 2, 3, and 4 use CSV append strategy (no JSON load/save)
	var appender RecordAppender
	var partitionRecords interface{}
	var err error

	switch w.config.QueryType {
	case 2:
		appender = &Query2RecordAppender{fileManager: w.fileManager}
		partitionRecords, err = w.processor.ProcessQuery2ChunkForCSV(chunkMessage, partitions, w.config.NumPartitions)
	case 3:
		appender = &Query3RecordAppender{fileManager: w.fileManager}
		partitionRecords, err = w.processor.ProcessQuery3ChunkForCSV(chunkMessage, partitions, w.config.NumPartitions)
	case 4:
		appender = &Query4RecordAppender{fileManager: w.fileManager}
		partitionRecords, err = w.processor.ProcessQuery4ChunkForCSV(chunkMessage, partitions, w.config.NumPartitions)
	default:
		return fmt.Errorf("unsupported query type: %d", w.config.QueryType)
	}

	if err != nil {
		return fmt.Errorf("failed to extract records: %v", err)
	}

	// Convert to map[int]interface{} for generic processing
	recordsMap := make(map[int]interface{})
	switch v := partitionRecords.(type) {
	case map[int][]shared.Query2Record:
		for k, val := range v {
			recordsMap[k] = val
		}
	case map[int][]shared.Query3Record:
		for k, val := range v {
			recordsMap[k] = val
		}
	case map[int][]shared.Query4Record:
		for k, val := range v {
			recordsMap[k] = val
		}
	default:
		return fmt.Errorf("unknown record type: %T", partitionRecords)
	}

	// Append records to CSV files for each partition (batch write per partition)
	for partition, records := range recordsMap {
		if err := appender.AppendRecords(chunkMessage.ClientID, partition, records); err != nil {
			return fmt.Errorf("failed to append to partition %d CSV: %v", partition, err)
		}

		// Get record count for this partition for logging
		var partitionRecordCount int
		switch r := records.(type) {
		case []shared.Query2Record:
			partitionRecordCount = len(r)
		case []shared.Query3Record:
			partitionRecordCount = len(r)
		case []shared.Query4Record:
			partitionRecordCount = len(r)
		}

		testing_utils.LogInfo("GroupBy Worker", "Appended %d records to partition %d CSV for chunk %d",
			partitionRecordCount, partition, chunkMessage.ChunkNumber)
	}

	// Send chunk notification to orchestrator AFTER successful CSV writes
	return w.sendChunkNotification(chunkMessage)
}

// getPartitionsForWorker returns the list of partitions this worker handles
func (w *GroupByWorker) getPartitionsForWorker() []int {
	// Use common partition calculation utility
	partitions := common.GetPartitionsForWorker(
		w.config.QueryType,
		w.config.WorkerID,
		w.config.NumWorkers,
		w.config.NumPartitions,
	)
	return partitions
}

// sendChunkNotification sends a chunk notification to the orchestrator
func (w *GroupByWorker) sendChunkNotification(chunkMessage *chunk.Chunk) error {
	// Create chunk notification
	notification := signals.NewChunkNotification(
		chunkMessage.ClientID,
		chunkMessage.FileID,
		w.workerIDStr,
		chunkMessage.TableID,
		chunkMessage.ChunkNumber,
		chunkMessage.IsLastChunk,
		chunkMessage.IsLastFromTable,
	)

	// Serialize notification
	serializedNotification, err := signals.SerializeChunkNotification(notification)
	if err != nil {
		return fmt.Errorf("failed to serialize chunk notification: %v", err)
	}

	// Send to orchestrator via fanout exchange (empty routing keys = fanout)
	if sendErr := w.orchestratorProducer.Send(serializedNotification, []string{}); sendErr != 0 {
		return fmt.Errorf("failed to send chunk notification to orchestrator: %v", sendErr)
	}

	// testing_utils.LogInfo("GroupBy Worker", "Sent chunk notification to orchestrator for chunk %d (ClientID=%s, FileID=%s)",
	// 	chunkMessage.ChunkNumber, chunkMessage.ClientID, chunkMessage.FileID)

	return nil
}

// Close closes the worker
func (w *GroupByWorker) Close() {
	if w.consumer != nil {
		w.consumer.Close()
	}
	if w.orchestratorProducer != nil {
		w.orchestratorProducer.Close()
	}
}
