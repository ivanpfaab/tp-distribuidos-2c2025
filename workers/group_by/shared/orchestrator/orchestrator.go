package main

import (
	"fmt"
	"log"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	"github.com/tp-distribuidos-2c2025/workers/shared"
)

// GroupByOrchestrator manages the coordination of map-reduce operations
type GroupByOrchestrator struct {
	config             *OrchestratorConfig
	completionTracker  *shared.CompletionTracker
	chunkConsumer      *exchange.ExchangeConsumer
	completionProducer *workerqueue.QueueMiddleware // For sending completion chunks to next step
	fileAggregator     *FileAggregator              // For aggregating partition files
}

// NewGroupByOrchestrator creates a new group by orchestrator
func NewGroupByOrchestrator(queryType int) (*GroupByOrchestrator, error) {
	log.Printf("NewGroupByOrchestrator: Query Type %d", queryType)
	config, err := NewOrchestratorConfig(queryType)
	if err != nil {
		return nil, fmt.Errorf("failed to create orchestrator config: %v", err)
	}

	log.Printf("NewGroupByOrchestrator: Worker ID %d", config.WorkerID)

	orchestrator := &GroupByOrchestrator{
		config:         config,
		fileAggregator: NewFileAggregator(queryType, config.WorkerID, config.NumPartitions, config.NumWorkers),
	}

	// Create completion tracker with callback to send termination signals
	trackerName := fmt.Sprintf("Query%d-Orchestrator-Worker%d", queryType, config.WorkerID)
	orchestrator.completionTracker = shared.NewCompletionTracker(trackerName, orchestrator.onClientCompleted)

	orchestrator.initializeQueues()
	return orchestrator, nil
}

// initializeQueues sets up all necessary queues and exchanges
func (gbo *GroupByOrchestrator) initializeQueues() {
	// Create consumer for chunk notifications from map workers (fanout exchange)
	exchangeName := queues.GetOrchestratorChunksExchangeName(gbo.config.QueryType)
	if exchangeName == "" {
		log.Fatalf("No orchestrator chunks exchange found for query type %d", gbo.config.QueryType)
	}

	gbo.chunkConsumer = exchange.NewExchangeConsumer(exchangeName, []string{}, gbo.config.RabbitMQConfig)
	if gbo.chunkConsumer == nil {
		log.Fatalf("Failed to create chunk notification consumer for exchange: %s", exchangeName)
	}

	// Declare the fanout exchange (workers will also declare it, but we ensure it exists)
	exchangeDeclarer := exchange.NewMessageMiddlewareExchange(exchangeName, []string{}, gbo.config.RabbitMQConfig)
	if exchangeDeclarer == nil {
		gbo.chunkConsumer.Close()
		log.Fatalf("Failed to create exchange declarer for exchange: %s", exchangeName)
	}
	if err := exchangeDeclarer.DeclareExchange("fanout", false, false, false, false); err != 0 {
		gbo.chunkConsumer.Close()
		exchangeDeclarer.Close()
		log.Fatalf("Failed to declare fanout exchange %s: %v", exchangeName, err)
	}
	exchangeDeclarer.Close()

	// Create completion chunk producer for next step
	completionQueueName := getCompletionQueueName(gbo.config.QueryType)
	gbo.completionProducer = workerqueue.NewMessageMiddlewareQueue(completionQueueName, gbo.config.RabbitMQConfig)
	if gbo.completionProducer == nil {
		gbo.chunkConsumer.Close()
		log.Fatalf("Failed to create completion chunk producer for queue: %s", completionQueueName)
	}

	log.Printf("Orchestrator initialized for Query %d, Worker %d (expected files will be inferred dynamically)",
		gbo.config.QueryType, gbo.config.WorkerID)
	log.Printf("Completion chunks will be sent to queue: %s", completionQueueName)
	log.Printf("Consuming chunk notifications from fanout exchange: %s", exchangeName)
}

// onClientCompleted is called when all files for a client are completed
func (gbo *GroupByOrchestrator) onClientCompleted(clientID string, clientStatus *shared.ClientStatus) {
	log.Printf("Client %s: All %d files completed! Reading grouped data files from worker %d and sending to next step...",
		clientID, clientStatus.TotalExpectedFiles, gbo.config.WorkerID)

	// Process partition files (returns multiple results for Query 4, single for Query 2/3)
	fileResults, err := gbo.fileAggregator.AggregateClientFiles(clientID)
	if err != nil {
		log.Printf("Failed to aggregate files for client %s: %v", clientID, err)
		gbo.completionTracker.ClearClientState(clientID)
		return
	}

	// Send chunks to next step
	// All queries: send multiple chunks, one per partition file
	for i, result := range fileResults {
		isLastChunk := (i == len(fileResults)-1)
		// Use partition number as chunk number (unique across all orchestrators)
		chunkNumber := result.PartitionNumber
		// For Query2/Query3 (no top worker), set IsLastFromTable=true on last chunk
		isLastFromTable := isLastChunk && (gbo.config.QueryType == 2 || gbo.config.QueryType == 3)
		if err := gbo.sendDataChunk(clientID, chunkNumber, result.CSVData, isLastChunk, isLastFromTable); err != nil {
			log.Printf("Failed to send data chunk for client %s, partition %d: %v", clientID, chunkNumber, err)
			// Continue with other chunks
			continue
		}
		log.Printf("Client %s: Sent chunk for partition %d (%d/%d)", clientID, chunkNumber, i+1, len(fileResults))
	}

	// Clean up files and clear state
	gbo.cleanupClientFiles(clientID)
}

// sendAggregatedChunk sends the aggregated CSV data as a chunk to the next processing step
func (gbo *GroupByOrchestrator) sendAggregatedChunk(clientID string, chunkNumber int, csvData string) error {
	// Send chunk to next step (IsLastChunk=true, IsLastFromTable=true for Query2/Query3)
	isLastFromTable := (gbo.config.QueryType == 2 || gbo.config.QueryType == 3)
	return gbo.sendDataChunk(clientID, chunkNumber, csvData, true, isLastFromTable)
}

// cleanupClientFiles deletes all files and clears client state from the completion tracker
func (gbo *GroupByOrchestrator) cleanupClientFiles(clientID string) {
	log.Printf("Client %s: All chunks sent, now cleaning up files...", clientID)

	// Delete all partition files for this client
	if err := gbo.fileAggregator.CleanupClientFiles(clientID); err != nil {
		log.Printf("Failed to cleanup files for client %s: %v", clientID, err)
		// Continue anyway - don't block on cleanup failures
	}

	// Clear client state from completion tracker
	gbo.completionTracker.ClearClientState(clientID)

	log.Printf("Client %s: All chunks sent and files cleaned up", clientID)
}

// sendDataChunk sends a data chunk with CSV data to the next processing step
func (gbo *GroupByOrchestrator) sendDataChunk(clientID string, chunkNumber int, csvData string, isLastChunk bool, isLastFromTable bool) error {
	// Create chunk with grouped CSV data
	// All queries: Chunk number = partition number (unique across orchestrators)
	// IsLastChunk: true only for the last chunk from this orchestrator
	// IsLastFromTable: true for Query2/Query3 (no top worker), or set by top-users worker for Query4
	dataChunk := chunk.NewChunk(
		clientID,                   // ClientID
		"01",                       // FileID - use "01" so completion tracker can parse it
		byte(gbo.config.QueryType), // QueryType
		chunkNumber,                // ChunkNumber (partition number)
		isLastChunk,                // IsLastChunk - true for last chunk from this orchestrator
		isLastFromTable,            // IsLastFromTable - true for Query2/Query3, or set by top-users worker for Query4
		len(csvData),               // ChunkSize
		0,                          // TableID
		csvData,                    // ChunkData - CSV formatted data
	)

	// Create chunk message
	chunkMessage := chunk.NewChunkMessage(dataChunk)

	// Serialize the chunk
	serializedChunk, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		return fmt.Errorf("failed to serialize data chunk: %v", err)
	}

	// Send to the appropriate queue based on query type
	if sendErr := gbo.completionProducer.Send(serializedChunk); sendErr != 0 {
		return fmt.Errorf("failed to send data chunk: error code %v", sendErr)
	}

	completionQueueName := getCompletionQueueName(gbo.config.QueryType)
	log.Printf("Sent chunk %d for client %s to queue %s (IsLastChunk=%t, %d bytes)",
		chunkNumber, clientID, completionQueueName, isLastChunk, len(csvData))

	return nil
}

// Start starts the orchestrator
func (gbo *GroupByOrchestrator) Start() {
	log.Printf("Starting Group By Orchestrator for Query %d, Worker %d...", gbo.config.QueryType, gbo.config.WorkerID)

	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Printf("Orchestrator Worker %d started consuming chunk notifications from fanout exchange", gbo.config.WorkerID)

		for delivery := range *consumeChannel {
			log.Printf("Orchestrator Worker %d received chunk notification: %d bytes", gbo.config.WorkerID, len(delivery.Body))

			// Deserialize chunk notification using protocol
			message, err := deserializer.Deserialize(delivery.Body)
			if err != nil {
				log.Printf("Failed to deserialize chunk notification: %v", err)
				delivery.Ack(false)
				continue
			}

			notification, ok := message.(*signals.ChunkNotification)
			if !ok {
				log.Printf("Received message is not a ChunkNotification: %T", message)
				delivery.Ack(false)
				continue
			}

			// Process the notification using the completion tracker
			// All orchestrators receive all notifications via fanout, so they all track the same state
			if err := gbo.completionTracker.ProcessChunkNotification(notification); err != nil {
				log.Printf("Failed to process chunk notification: %v", err)
				delivery.Ack(false)
				continue
			}

			// Acknowledge the message
			delivery.Ack(false)
		}
		done <- nil
	}

	exchangeName := queues.GetOrchestratorChunksExchangeName(gbo.config.QueryType)
	queueName := fmt.Sprintf("query%d-orchestrator-worker-%d-queue", gbo.config.QueryType, gbo.config.WorkerID)
	gbo.chunkConsumer.SetQueueName(queueName)
	if err := gbo.chunkConsumer.StartConsuming(onMessageCallback); err != 0 {
		log.Fatalf("Failed to start consuming chunk notifications: %v", err)
	}

	log.Printf("Successfully started consuming from fanout exchange: %s", exchangeName)
}

// Close closes the orchestrator
func (gbo *GroupByOrchestrator) Close() {
	if gbo.chunkConsumer != nil {
		gbo.chunkConsumer.Close()
	}
	if gbo.completionProducer != nil {
		gbo.completionProducer.Close()
	}
}

// getCompletionQueueName returns the queue name for completion chunks based on query type
func getCompletionQueueName(queryType int) string {
	switch queryType {
	case 2:
		return queues.Query2GroupByResultsQueue
	case 3:
		return queues.Query3GroupByResultsQueue
	case 4:
		return queues.Query4GroupByResultsQueue
	default:
		log.Fatalf("Unknown query type: %d", queryType)
		return ""
	}
}
