package main

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
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
			testing_utils.LogInfo("GroupBy Worker", "Received message #%d", messageCount)

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

// processChunk processes a single chunk with file-based group by aggregation
func (w *GroupByWorker) processChunk(chunkMessage *chunk.Chunk) error {
	testing_utils.LogInfo("GroupBy Worker", "Processing chunk %d from client %s, file %s (IsLastChunk=%t, IsLastFromTable=%t)",
		chunkMessage.ChunkNumber, chunkMessage.ClientID, chunkMessage.FileID,
		chunkMessage.IsLastChunk, chunkMessage.IsLastFromTable)

	// Determine partition(s) for this worker
	// For Q2/Q3: worker ID = partition (1:1 mapping)
	// For Q4: worker handles multiple partitions
	partitions := w.getPartitionsForWorker()

	// Query 2, 3, and 4 use CSV append strategy (no JSON load/save)
	if w.config.QueryType == 2 {
		return w.processQuery2Chunk(chunkMessage, partitions)
	}
	if w.config.QueryType == 3 {
		return w.processQuery3Chunk(chunkMessage, partitions)
	}
	if w.config.QueryType == 4 {
		return w.processQuery4Chunk(chunkMessage, partitions)
	}

	// Fallback for unknown query types
	return fmt.Errorf("unsupported query type: %d", w.config.QueryType)
}

// processQuery2Chunk processes Query 2 chunks using CSV append strategy
func (w *GroupByWorker) processQuery2Chunk(chunkMessage *chunk.Chunk, workerPartitions []int) error {
	// Extract records to append (no aggregation in memory)
	partitionRecords, err := w.processor.ProcessQuery2ChunkForCSV(chunkMessage, workerPartitions)
	if err != nil {
		return fmt.Errorf("failed to extract records: %v", err)
	}

	// Append records to CSV files for each partition (batch write per partition)
	for partition, records := range partitionRecords {
		// Convert to struct slice for batch append
		recs := make([]struct{ Month, ItemID, Quantity, Subtotal string }, len(records))
		for i, rec := range records {
			recs[i] = struct{ Month, ItemID, Quantity, Subtotal string }{
				Month:    rec.Month,
				ItemID:   rec.ItemID,
				Quantity: rec.Quantity,
				Subtotal: rec.Subtotal,
			}
		}

		if err := w.fileManager.AppendQuery2RecordsToPartitionCSV(chunkMessage.ClientID, partition, recs); err != nil {
			return fmt.Errorf("failed to append to partition %d CSV: %v", partition, err)
		}

		testing_utils.LogInfo("GroupBy Worker", "Appended %d records to partition %d CSV for chunk %d",
			len(records), partition, chunkMessage.ChunkNumber)
	}

	// Send chunk notification to orchestrator AFTER successful CSV writes
	return w.sendChunkNotification(chunkMessage)
}

// processQuery3Chunk processes Query 3 chunks using CSV append strategy
func (w *GroupByWorker) processQuery3Chunk(chunkMessage *chunk.Chunk, workerPartitions []int) error {
	// Extract records to append (no aggregation in memory)
	partitionRecords, err := w.processor.ProcessQuery3ChunkForCSV(chunkMessage, workerPartitions)
	if err != nil {
		return fmt.Errorf("failed to extract records: %v", err)
	}

	// Append records to CSV files for each partition (batch write per partition)
	for partition, records := range partitionRecords {
		// Convert to struct slice for batch append
		recs := make([]struct{ StoreID, FinalAmount string }, len(records))
		for i, rec := range records {
			recs[i] = struct{ StoreID, FinalAmount string }{
				StoreID:     rec.StoreID,
				FinalAmount: rec.FinalAmount,
			}
		}

		if err := w.fileManager.AppendQuery3RecordsToPartitionCSV(chunkMessage.ClientID, partition, recs); err != nil {
			return fmt.Errorf("failed to append to partition %d CSV: %v", partition, err)
		}

		testing_utils.LogInfo("GroupBy Worker", "Appended %d records to partition %d CSV for chunk %d",
			len(records), partition, chunkMessage.ChunkNumber)
	}

	// Send chunk notification to orchestrator AFTER successful CSV writes
	return w.sendChunkNotification(chunkMessage)
}

// processQuery4Chunk processes Query 4 chunks using CSV append strategy
func (w *GroupByWorker) processQuery4Chunk(chunkMessage *chunk.Chunk, workerPartitions []int) error {
	// Extract records to append (no aggregation in memory)
	partitionRecords, err := w.processor.ProcessQuery4ChunkForCSV(chunkMessage, workerPartitions, w.config.NumPartitions)
	if err != nil {
		return fmt.Errorf("failed to extract records: %v", err)
	}

	// Append records to CSV files for each partition (batch write per partition)
	for partition, records := range partitionRecords {
		// Convert to struct slice for batch append
		recs := make([]struct{ UserID, StoreID string }, len(records))
		for i, rec := range records {
			recs[i] = struct{ UserID, StoreID string }{UserID: rec.UserID, StoreID: rec.StoreID}
		}

		if err := w.fileManager.AppendRecordsToPartitionCSV(chunkMessage.ClientID, partition, recs); err != nil {
			return fmt.Errorf("failed to append to partition %d CSV: %v", partition, err)
		}

		testing_utils.LogInfo("GroupBy Worker", "Appended %d records to partition %d CSV for chunk %d",
			len(records), partition, chunkMessage.ChunkNumber)
	}

	// Send chunk notification to orchestrator AFTER successful CSV writes
	return w.sendChunkNotification(chunkMessage)
}

// getPartitionsForWorker returns the list of partitions this worker handles
func (w *GroupByWorker) getPartitionsForWorker() []int {
	// For Q2/Q3: worker ID maps to partition (1:1 mapping)
	// Worker IDs are 1-based (1, 2, 3), partitions are 0-based (0, 1, 2)
	// So Worker 1 → Partition 0, Worker 2 → Partition 1, Worker 3 → Partition 2
	if w.config.QueryType == 2 || w.config.QueryType == 3 {
		return []int{w.config.WorkerID - 1}
	}

	// For Q4: worker handles multiple partitions based on modulo
	// Worker i gets partitions where: partition % numWorkers == (workerID % numWorkers)
	// Since workerID starts from 1:
	// Worker 1: partitions 1, 4, 7, 10, ... (partition % 3 == 1)
	// Worker 2: partitions 2, 5, 8, 11, ... (partition % 3 == 2)
	// Worker 3: partitions 0, 3, 6, 9, ... (partition % 3 == 0)
	partitions := []int{}
	targetRemainder := w.config.WorkerID % w.config.NumWorkers
	for partition := 0; partition < w.config.NumPartitions; partition++ {
		if partition%w.config.NumWorkers == targetRemainder {
			partitions = append(partitions, partition)
		}
	}
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

	testing_utils.LogInfo("GroupBy Worker", "Sent chunk notification to orchestrator for chunk %d (ClientID=%s, FileID=%s)",
		chunkMessage.ChunkNumber, chunkMessage.ClientID, chunkMessage.FileID)

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
