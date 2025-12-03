package main

import (
	"fmt"
	"path/filepath"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	partitionmanager "github.com/tp-distribuidos-2c2025/shared/partition_manager"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
	worker_builder "github.com/tp-distribuidos-2c2025/shared/worker_builder"
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
	messageManager       *messagemanager.MessageManager
	partitionManager     *partitionmanager.PartitionManager
	firstChunkProcessed  bool
}

// NewGroupByWorker creates a new group by worker instance
func NewGroupByWorker(config *WorkerConfig) (*GroupByWorker, error) {
	// Generate worker ID string
	workerIDStr := fmt.Sprintf("query%d-groupby-worker-%d", config.QueryType, config.WorkerID)

	// Get base directory for this worker
	baseDir := common.GetBaseDir(config.QueryType, config.WorkerID)
	stateDir := filepath.Join(baseDir, "state")
	stateFilePath := filepath.Join(stateDir, "processed-chunks.txt")

	// Use builder to create all resources
	builder := worker_builder.NewWorkerBuilder(fmt.Sprintf("GroupBy Worker (Query %d, Worker %d)", config.QueryType, config.WorkerID)).
		WithConfig(config.ConnectionConfig).
		// Exchange consumer (topic exchange)
		WithExchangeConsumer(config.ExchangeName, config.RoutingKeys, true, worker_builder.ExchangeDeclarationOptions{
			Type: "topic",
		}).
		// Exchange producer (fanout exchange for orchestrator notifications)
		WithExchangeProducer(config.OrchestratorExchange, []string{}, true, worker_builder.ExchangeDeclarationOptions{
			Type: "fanout",
		}).
		// State management
		WithDirectory(baseDir, 0755).
		WithDirectory(stateDir, 0755).
		WithMessageManager(stateFilePath).
		// PartitionManager
		WithPartitionManager(worker_builder.PartitionManagerConfig{
			PartitionsDir:           baseDir,
			NumPartitions:           config.NumPartitions,
			RecoverIncompleteWrites: true,
		})

	// Validate builder
	if err := builder.Validate(); err != nil {
		return nil, builder.CleanupOnError(err)
	}

	// Extract resources from builder
	consumer := builder.GetExchangeConsumer(config.ExchangeName)
	orchestratorProducer := builder.GetExchangeProducer(config.OrchestratorExchange)

	if consumer == nil || orchestratorProducer == nil {
		return nil, builder.CleanupOnError(fmt.Errorf("failed to get resources from builder"))
	}

	// Extract MessageManager from builder
	messageManager := builder.GetResourceTracker().Get(
		worker_builder.ResourceTypeMessageManager,
		"message-manager",
	)
	if messageManager == nil {
		return nil, builder.CleanupOnError(fmt.Errorf("failed to get message manager from builder"))
	}
	mm, ok := messageManager.(*messagemanager.MessageManager)
	if !ok {
		return nil, builder.CleanupOnError(fmt.Errorf("message manager has wrong type"))
	}

	// Extract PartitionManager from builder
	partitionManager := builder.GetPartitionManager()
	if partitionManager == nil {
		return nil, builder.CleanupOnError(fmt.Errorf("failed to get partition manager from builder"))
	}

	testing_utils.LogInfo("GroupBy Worker", "PartitionManager initialized with %d partitions", config.NumPartitions)

	// Create file manager and processor
	fileManager := NewFileManager(config.QueryType, config.WorkerID)
	processor := NewChunkProcessor(config.QueryType)

	// Pass partitionManager to fileManager
	fileManager.SetPartitionManager(partitionManager)

	return &GroupByWorker{
		config:               config,
		consumer:             consumer,
		orchestratorProducer: orchestratorProducer,
		workerIDStr:          workerIDStr,
		fileManager:          fileManager,
		processor:            processor,
		messageManager:       mm,
		partitionManager:     partitionManager,
		firstChunkProcessed:  false,
	}, nil
}

// Start begins processing messages from the exchange
func (w *GroupByWorker) Start() middleware.MessageMiddlewareError {
	testing_utils.LogInfo("GroupBy Worker", "Starting worker for query %d, listening to routing keys: %v",
		w.config.QueryType, w.config.RoutingKeys)

	// Set queue name for persistent queue
	queueName := fmt.Sprintf("query%d-groupby-worker-%d-queue", w.config.QueryType, w.config.WorkerID)
	w.consumer.SetQueueName(queueName)

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

			// Deserialize the message in the callback (middleware layer)
			message, err := deserializer.Deserialize(delivery.Body)
			if err != nil {
				testing_utils.LogWarn("GroupBy Worker", "Failed to deserialize message: %v", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}

			chunkMessage, _ := message.(*chunk.Chunk)

			if err := w.processMessage(chunkMessage); err != nil {
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
func (w *GroupByWorker) processMessage(chunkMessage *chunk.Chunk) error {

	// Check if chunk was already processed
	if w.messageManager.IsProcessed(chunkMessage.ClientID, chunkMessage.ID) {
		testing_utils.LogInfo("GroupBy Worker", "Chunk %s already processed, skipping", chunkMessage.ID)
		return nil
	}

	// Process the chunk
	if err := w.processChunk(chunkMessage); err != nil {
		return err
	}

	// Mark chunk as processed after successful write
	if err := w.messageManager.MarkProcessed(chunkMessage.ClientID, chunkMessage.ID); err != nil {
		testing_utils.LogError("GroupBy Worker", "Failed to mark chunk as processed: %v", err)
		return fmt.Errorf("failed to mark chunk as processed: %w", err)
	}

	return nil
}

// RecordAppender defines how to append records to CSV for a specific query type
type RecordAppender interface {
	AppendRecords(clientID string, partition int, records interface{}, chunkID string, isFirstChunk bool) error
}

// Query2RecordAppender appends Query2Record to CSV
type Query2RecordAppender struct {
	fileManager *FileManager
}

func (a *Query2RecordAppender) AppendRecords(clientID string, partition int, records interface{}, chunkID string, isFirstChunk bool) error {
	return a.fileManager.AppendQuery2RecordsToPartitionCSV(clientID, partition, records.([]shared.Query2Record), chunkID, isFirstChunk)
}

// Query3RecordAppender appends Query3Record to CSV
type Query3RecordAppender struct {
	fileManager *FileManager
}

func (a *Query3RecordAppender) AppendRecords(clientID string, partition int, records interface{}, chunkID string, isFirstChunk bool) error {
	return a.fileManager.AppendQuery3RecordsToPartitionCSV(clientID, partition, records.([]shared.Query3Record), chunkID, isFirstChunk)
}

// Query4RecordAppender appends Query4Record to CSV
type Query4RecordAppender struct {
	fileManager *FileManager
}

func (a *Query4RecordAppender) AppendRecords(clientID string, partition int, records interface{}, chunkID string, isFirstChunk bool) error {
	return a.fileManager.AppendRecordsToPartitionCSV(clientID, partition, records.([]shared.Query4Record), chunkID, isFirstChunk)
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
		if err := appender.AppendRecords(chunkMessage.ClientID, partition, records, chunkMessage.ID, !w.firstChunkProcessed); err != nil {
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

	// After processing the first chunk, mark it as processed
	if !w.firstChunkProcessed {
		w.firstChunkProcessed = true
		testing_utils.LogInfo("GroupBy Worker", "First chunk processed, switching to normal write mode")
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
	if w.messageManager != nil {
		w.messageManager.Close()
	}
	if w.consumer != nil {
		w.consumer.Close()
	}
	if w.orchestratorProducer != nil {
		w.orchestratorProducer.Close()
	}
}
