package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	completioncleaner "github.com/tp-distribuidos-2c2025/shared/completion_cleaner"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	worker_builder "github.com/tp-distribuidos-2c2025/shared/worker_builder"
	"github.com/tp-distribuidos-2c2025/workers/shared"
)

// GroupByOrchestrator manages the coordination of map-reduce operations
type GroupByOrchestrator struct {
	config             *OrchestratorConfig
	completionTracker  *shared.CompletionTracker
	chunkConsumer      *exchange.ExchangeConsumer
	completionProducer *workerqueue.QueueMiddleware // For sending completion chunks to next step
	fileAggregator     *FileAggregator              // For aggregating partition files

	// Fault tolerance components
	messageManager *messagemanager.MessageManager
	stateManager   *StateManager
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

	// Initialize fault tolerance components
	// Use existing /app/groupby-data volume, create orchestrator-state subdirectory
	stateDir := "/app/groupby-data/orchestrator-state"
	metadataDir := filepath.Join(stateDir, "metadata")
	processedNotificationsPath := filepath.Join(stateDir, "processed-notifications.txt")

	// Ensure state directory exists
	if err := os.MkdirAll(metadataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create state directory: %w", err)
	}

	// Initialize MessageManager for duplicate detection
	messageManager := messagemanager.NewMessageManager(processedNotificationsPath)

	// Initialize StateManager
	stateManager := NewStateManager(metadataDir, orchestrator.completionTracker)
	orchestrator.stateManager = stateManager

	// Use builder to create all resources
	exchangeName := queues.GetOrchestratorChunksExchangeName(config.QueryType)
	if exchangeName == "" {
		return nil, fmt.Errorf("no orchestrator chunks exchange found for query type %d", config.QueryType)
	}

	completionQueueName := getCompletionQueueName(config.QueryType)

	builder := worker_builder.NewWorkerBuilder(fmt.Sprintf("Group By Orchestrator (Query %d, Worker %d)", config.QueryType, config.WorkerID)).
		WithConfig(config.RabbitMQConfig).
		// Exchange consumer (fanout exchange for chunk notifications)
		WithExchangeConsumer(exchangeName, []string{}, true, worker_builder.ExchangeDeclarationOptions{
			Type: "fanout",
		}).
		// Queue producer (for completion chunks)
		WithQueueProducer(completionQueueName, true).
		// State management
		WithDirectory(stateDir, 0755).
		WithDirectory(metadataDir, 0755).
		WithMessageManager(processedNotificationsPath)

	// Validate builder
	if err := builder.Validate(); err != nil {
		return nil, builder.CleanupOnError(err)
	}

	// Extract resources from builder
	chunkConsumer := builder.GetExchangeConsumer(exchangeName)
	completionProducer := builder.GetQueueProducer(completionQueueName)

	if chunkConsumer == nil || completionProducer == nil {
		return nil, builder.CleanupOnError(fmt.Errorf("failed to get resources from builder"))
	}

	// Extract MessageManager from builder
	messageManagerResource := builder.GetResourceTracker().Get(
		worker_builder.ResourceTypeMessageManager,
		"message-manager",
	)
	if messageManagerResource == nil {
		return nil, builder.CleanupOnError(fmt.Errorf("failed to get message manager from builder"))
	}
	mm, ok := messageManagerResource.(*messagemanager.MessageManager)
	if !ok {
		return nil, builder.CleanupOnError(fmt.Errorf("message manager has wrong type"))
	}

	// Add CompletionCleaner with MessageManager as cleanup handler
	// Use WORKER_ID from environment (service name) for cleanup queue name
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		return nil, builder.CleanupOnError(fmt.Errorf("WORKER_ID environment variable is required"))
	}
	builder.WithCompletionCleaner(
		queues.ClientCompletionCleanupExchange,
		workerID,
		[]completioncleaner.CleanupHandler{mm},
	)

	orchestrator.chunkConsumer = chunkConsumer
	orchestrator.completionProducer = completionProducer
	orchestrator.messageManager = mm

	// Rebuild state from CSV metadata on startup
	log.Println("Group By Orchestrator: Rebuilding state from metadata...")
	if err := orchestrator.stateManager.RebuildState(); err != nil {
		log.Printf("Group By Orchestrator: Warning - failed to rebuild state: %v", err)
	} else {
		log.Println("Group By Orchestrator: State rebuilt successfully")
	}

	log.Printf("Orchestrator initialized for Query %d, Worker %d (expected files will be inferred dynamically)",
		config.QueryType, config.WorkerID)
	log.Printf("Completion chunks will be sent to queue: %s", completionQueueName)
	log.Printf("Consuming chunk notifications from fanout exchange: %s", exchangeName)

	return orchestrator, nil
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

	// Delete CSV metadata file for completed client
	if err := gbo.stateManager.DeleteClientMetadata(clientID); err != nil {
		log.Printf("Group By Orchestrator: Warning - failed to delete metadata file for client %s: %v", clientID, err)
	}
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

			// Check for duplicate notification
			if gbo.messageManager.IsProcessed(notification.ClientID, notification.ID) {
				delivery.Ack(false)
				continue
			}

			// Process the notification using the completion tracker
			if err := gbo.completionTracker.ProcessChunkNotification(notification); err != nil {
				log.Printf("Failed to process chunk notification: %v", err)
				delivery.Ack(false)
				continue
			}

			// Append notification to CSV for state rebuild
			if err := gbo.stateManager.AppendNotification(notification); err != nil {
				log.Printf("Warning - failed to append notification to CSV: %v", err)
			}

			// Mark as processed in MessageManager
			if err := gbo.messageManager.MarkProcessed(notification.ClientID, notification.ID); err != nil {
				log.Printf("Warning - failed to mark notification as processed: %v", err)
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
	if gbo.messageManager != nil {
		gbo.messageManager.Close()
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
