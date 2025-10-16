package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// FileStatus represents the status of a file being processed
type FileStatus struct {
	FileID            string
	TableID           int
	ChunksReceived    int
	LastChunkNumber   int
	IsCompleted       bool
	LastChunkReceived bool
}

// ChunkNotification represents a notification from a map worker about chunk processing
type ChunkNotification struct {
	ClientID    string
	FileID      string
	TableID     int
	ChunkNumber int
	IsLastChunk bool
	MapWorkerID string
}

// TerminationSignal represents a signal to terminate processing
type TerminationSignal struct {
	QueryType int
	ClientID  string
	Message   string
}

// GroupByOrchestrator manages the coordination of map-reduce operations
type GroupByOrchestrator struct {
	config              *OrchestratorConfig
	fileStatuses        map[string]*FileStatus
	completedFiles      int
	expectedFileCounts  map[int]int
	totalExpectedFiles  int
	chunkConsumer       *workerqueue.QueueConsumer
	terminationProducer *exchange.ExchangeMiddleware
	mutex               sync.RWMutex
}

// NewGroupByOrchestrator creates a new group by orchestrator
func NewGroupByOrchestrator(queryType int) *GroupByOrchestrator {
	config := NewOrchestratorConfig(queryType)
	expectedFileCounts := GetExpectedFileCounts()

	// Calculate total expected files for this query type
	totalExpectedFiles := 0
	for tableID, count := range expectedFileCounts {
		if tableID == queryType || queryType == 2 { // Query 2 uses transaction_items (table 2)
			totalExpectedFiles += count
		}
	}

	// For Query 2, we only process transaction_items (table 2)
	if queryType == 2 {
		totalExpectedFiles = expectedFileCounts[2] // Only transaction_items
	}

	orchestrator := &GroupByOrchestrator{
		config:             config,
		fileStatuses:       make(map[string]*FileStatus),
		completedFiles:     0,
		expectedFileCounts: expectedFileCounts,
		totalExpectedFiles: totalExpectedFiles,
	}

	orchestrator.initializeQueues()
	return orchestrator
}

// initializeQueues sets up all necessary queues and exchanges
func (gbo *GroupByOrchestrator) initializeQueues() {
	// Create consumer for chunk notifications from map workers
	queueName := getChunkNotificationQueueName(gbo.config.QueryType)
	gbo.chunkConsumer = workerqueue.NewQueueConsumer(queueName, gbo.config.RabbitMQConfig)
	if gbo.chunkConsumer == nil {
		log.Fatalf("Failed to create chunk notification consumer for queue: %s", queueName)
	}

	// Declare the chunk notification queue
	queueDeclarer := workerqueue.NewMessageMiddlewareQueue(queueName, gbo.config.RabbitMQConfig)
	if queueDeclarer == nil {
		gbo.chunkConsumer.Close()
		log.Fatalf("Failed to create queue declarer for queue: %s", queueName)
	}
	if err := queueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		gbo.chunkConsumer.Close()
		queueDeclarer.Close()
		log.Fatalf("Failed to declare chunk notification queue %s: %v", queueName, err)
	}
	queueDeclarer.Close()

	// Create fanout exchange producer for termination signals to map workers
	terminationExchangeName := getMapWorkerTerminationExchangeName(gbo.config.QueryType)
	gbo.terminationProducer = exchange.NewMessageMiddlewareExchange(terminationExchangeName, []string{}, gbo.config.RabbitMQConfig)
	if gbo.terminationProducer == nil {
		gbo.chunkConsumer.Close()
		log.Fatalf("Failed to create termination exchange producer")
	}

	// Declare the termination exchange
	if err := gbo.terminationProducer.DeclareExchange("fanout", true, false, false, false); err != 0 {
		gbo.chunkConsumer.Close()
		gbo.terminationProducer.Close()
		log.Fatalf("Failed to declare termination exchange: %v", err)
	}

	log.Printf("Orchestrator initialized for Query %d with %d expected files",
		gbo.config.QueryType, gbo.totalExpectedFiles)
}

// ProcessChunkNotification processes a chunk notification from a map worker
func (gbo *GroupByOrchestrator) ProcessChunkNotification(notification *ChunkNotification) error {
	gbo.mutex.Lock()
	defer gbo.mutex.Unlock()

	fileKey := fmt.Sprintf("%s_%d", notification.FileID, notification.TableID)

	// Initialize file status if not exists
	if gbo.fileStatuses[fileKey] == nil {
		gbo.fileStatuses[fileKey] = &FileStatus{
			FileID:            notification.FileID,
			TableID:           notification.TableID,
			ChunksReceived:    0,
			LastChunkNumber:   0,
			IsCompleted:       false,
			LastChunkReceived: false,
		}
	}

	fileStatus := gbo.fileStatuses[fileKey]

	// Increment chunks received
	fileStatus.ChunksReceived++

	log.Printf("File %s (Table %d): Received chunk %d/%d (Total chunks: %d)",
		notification.FileID, notification.TableID,
		notification.ChunkNumber, fileStatus.LastChunkNumber,
		fileStatus.ChunksReceived)

	// If this is the last chunk, update last chunk info
	if notification.IsLastChunk {
		fileStatus.LastChunkNumber = notification.ChunkNumber
		fileStatus.LastChunkReceived = true

		log.Printf("File %s (Table %d): Received LAST chunk %d",
			notification.FileID, notification.TableID, notification.ChunkNumber)

		// Check if file is completed
		if fileStatus.ChunksReceived == notification.ChunkNumber {
			gbo.completeFile(fileKey, fileStatus)
		}
	} else if fileStatus.LastChunkReceived {
		// We already received the last chunk, check if this completes the file
		if fileStatus.ChunksReceived == fileStatus.LastChunkNumber {
			gbo.completeFile(fileKey, fileStatus)
		}
	}

	// Check if all files are completed
	if gbo.completedFiles >= gbo.totalExpectedFiles {
		log.Printf("All %d files completed! Sending termination signals...", gbo.totalExpectedFiles)
		gbo.sendTerminationSignals(notification.ClientID)
	}

	return nil
}

// completeFile marks a file as completed
func (gbo *GroupByOrchestrator) completeFile(fileKey string, fileStatus *FileStatus) {
	if fileStatus.IsCompleted {
		return // Already completed
	}

	fileStatus.IsCompleted = true
	gbo.completedFiles++

	log.Printf("✅ File %s (Table %d) COMPLETED! (%d/%d files completed)",
		fileStatus.FileID, fileStatus.TableID, gbo.completedFiles, gbo.totalExpectedFiles)
}

// sendTerminationSignals sends termination signals to all map and reduce workers
func (gbo *GroupByOrchestrator) sendTerminationSignals(clientID string) {
	terminationSignal := TerminationSignal{
		QueryType: gbo.config.QueryType,
		ClientID:  clientID,
		Message:   "All data processing completed",
	}

	// Serialize termination signal
	signalData, err := json.Marshal(terminationSignal)
	if err != nil {
		log.Printf("Failed to serialize termination signal: %v", err)
		return
	}

	// Send to map workers
	log.Printf("Sending termination signal to map workers for Query %d", gbo.config.QueryType)
	if err := gbo.terminationProducer.Send(signalData, []string{}); err != 0 {
		log.Printf("Failed to send termination signal to map workers: %v", err)
	}

	log.Printf("Termination signals sent successfully for Query %d", gbo.config.QueryType)
}

// Start starts the orchestrator
func (gbo *GroupByOrchestrator) Start() {
	log.Printf("Starting Group By Orchestrator for Query %d...", gbo.config.QueryType)

	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Printf("Orchestrator started consuming chunk notifications")

		for delivery := range *consumeChannel {
			log.Printf("Received chunk notification: %d bytes", len(delivery.Body))

			// Deserialize chunk notification
			var notification ChunkNotification
			if err := json.Unmarshal(delivery.Body, &notification); err != nil {
				log.Printf("Failed to deserialize chunk notification: %v", err)
				delivery.Ack(false)
				continue
			}

			// Process the notification
			if err := gbo.ProcessChunkNotification(&notification); err != nil {
				log.Printf("Failed to process chunk notification: %v", err)
				delivery.Ack(false)
				continue
			}

			// Acknowledge the message
			delivery.Ack(false)
		}
		done <- nil
	}

	queueName := getChunkNotificationQueueName(gbo.config.QueryType)
	if err := gbo.chunkConsumer.StartConsuming(onMessageCallback); err != 0 {
		log.Fatalf("Failed to start consuming chunk notifications: %v", err)
	}

	log.Printf("Successfully started consuming from queue: %s", queueName)
}

// Close closes the orchestrator
func (gbo *GroupByOrchestrator) Close() {
	if gbo.chunkConsumer != nil {
		gbo.chunkConsumer.Close()
	}
	if gbo.terminationProducer != nil {
		gbo.terminationProducer.Close()
	}
}

// getChunkNotificationQueueName returns the queue name for chunk notifications
func getChunkNotificationQueueName(queryType int) string {
	return fmt.Sprintf("query%d-orchestrator-chunks", queryType)
}

// getMapWorkerTerminationExchangeName returns the exchange name for map worker termination
func getMapWorkerTerminationExchangeName(queryType int) string {
	return fmt.Sprintf("query%d-map-termination", queryType)
}
