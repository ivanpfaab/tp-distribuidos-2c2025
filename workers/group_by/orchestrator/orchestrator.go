package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// FileStatus represents the status of a file being processed for a specific client
type FileStatus struct {
	FileID            string
	TableID           int
	ChunksReceived    int
	LastChunkNumber   int
	IsCompleted       bool
	LastChunkReceived bool
}

// ClientStatus represents the status of a client's data processing
type ClientStatus struct {
	ClientID       string
	FileStatuses   map[string]*FileStatus // Key: "fileID_tableID"
	CompletedFiles int
	IsCompleted    bool
}

// ChunkNotification is now defined in the protocol/signals package

// TerminationSignal represents a signal to terminate processing
type TerminationSignal struct {
	QueryType int
	ClientID  string
	Message   string
}

// GroupByOrchestrator manages the coordination of map-reduce operations
type GroupByOrchestrator struct {
	config              *OrchestratorConfig
	clientStatuses      map[string]*ClientStatus // Key: clientID
	totalExpectedFiles  int
	chunkConsumer       *workerqueue.QueueConsumer
	terminationProducer *exchange.ExchangeMiddleware
	mutex               sync.RWMutex
}

// NewGroupByOrchestrator creates a new group by orchestrator
func NewGroupByOrchestrator(queryType int) *GroupByOrchestrator {
	log.Printf("[query%d-orchestrator] [Q%d] NewGroupByOrchestrator: Query Type %d", queryType, queryType, queryType)
	config := NewOrchestratorConfig(queryType)
	totalExpectedFiles := GetTotalExpectedFiles(queryType)

	orchestrator := &GroupByOrchestrator{
		config:             config,
		clientStatuses:     make(map[string]*ClientStatus),
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
	if err := gbo.terminationProducer.DeclareExchange("fanout", false, false, false, false); err != 0 {
		gbo.chunkConsumer.Close()
		gbo.terminationProducer.Close()
		log.Fatalf("Failed to declare termination exchange: %v", err)
	}

	log.Printf("[query%d-orchestrator] [Q%d] Orchestrator initialized with %d expected files",
		gbo.config.QueryType, gbo.config.QueryType, gbo.totalExpectedFiles)
}

// ProcessChunkNotification processes a chunk notification from a map worker
func (gbo *GroupByOrchestrator) ProcessChunkNotification(notification *signals.ChunkNotification) error {
	gbo.mutex.Lock()
	defer gbo.mutex.Unlock()

	// Initialize client status if not exists
	if gbo.clientStatuses[notification.ClientID] == nil {
		gbo.clientStatuses[notification.ClientID] = &ClientStatus{
			ClientID:       notification.ClientID,
			FileStatuses:   make(map[string]*FileStatus),
			CompletedFiles: 0,
			IsCompleted:    false,
		}
	}

	clientStatus := gbo.clientStatuses[notification.ClientID]
	fileKey := fmt.Sprintf("%s_%d", notification.FileID, notification.TableID)

	// Initialize file status if not exists
	if clientStatus.FileStatuses[fileKey] == nil {
		clientStatus.FileStatuses[fileKey] = &FileStatus{
			FileID:            notification.FileID,
			TableID:           notification.TableID,
			ChunksReceived:    0,
			LastChunkNumber:   0,
			IsCompleted:       false,
			LastChunkReceived: false,
		}
	}

	fileStatus := clientStatus.FileStatuses[fileKey]

	// Increment chunks received
	fileStatus.ChunksReceived++

	log.Printf("[query%d-orchestrator] [CLI%s | Q%d] File %s (Table %d): Received chunk %d/%d (Total chunks: %d)",
		gbo.config.QueryType, notification.ClientID, gbo.config.QueryType, notification.FileID, notification.TableID,
		notification.ChunkNumber, fileStatus.LastChunkNumber, fileStatus.ChunksReceived)

	// If this is the last chunk, update last chunk info
	if notification.IsLastChunk {
		fileStatus.LastChunkNumber = notification.ChunkNumber
		fileStatus.LastChunkReceived = true

		log.Printf("[query%d-orchestrator] [CLI%s | Q%d] File %s (Table %d): Received LAST chunk %d",
			gbo.config.QueryType, notification.ClientID, gbo.config.QueryType, notification.FileID, notification.TableID, notification.ChunkNumber)

		// Check if file is completed
		if fileStatus.ChunksReceived == notification.ChunkNumber {
			gbo.completeFileForClient(notification.ClientID, fileKey, fileStatus)
		}
	} else if fileStatus.LastChunkReceived {
		// We already received the last chunk, check if this completes the file
		if fileStatus.ChunksReceived == fileStatus.LastChunkNumber {
			gbo.completeFileForClient(notification.ClientID, fileKey, fileStatus)
		}
	}

	// Check if all files are completed for this client
	if clientStatus.CompletedFiles >= gbo.totalExpectedFiles {
		log.Printf("[query%d-orchestrator] [CLI%s | Q%d] ✅ All %d files completed! Sending termination signals...",
			gbo.config.QueryType, notification.ClientID, gbo.config.QueryType, gbo.totalExpectedFiles)
		gbo.sendTerminationSignals(notification.ClientID)
		clientStatus.IsCompleted = true
	}

	return nil
}

// completeFileForClient marks a file as completed for a specific client
func (gbo *GroupByOrchestrator) completeFileForClient(clientID, fileKey string, fileStatus *FileStatus) {
	if fileStatus.IsCompleted {
		return // Already completed
	}

	fileStatus.IsCompleted = true
	clientStatus := gbo.clientStatuses[clientID]
	clientStatus.CompletedFiles++

	log.Printf("[query%d-orchestrator] [CLI%s | Q%d] ✅ File %s (Table %d) COMPLETED! (%d/%d files completed)",
		gbo.config.QueryType, clientID, gbo.config.QueryType, fileStatus.FileID, fileStatus.TableID, clientStatus.CompletedFiles, gbo.totalExpectedFiles)
}

// sendTerminationSignals sends termination signals to all map and reduce workers
func (gbo *GroupByOrchestrator) sendTerminationSignals(clientID string) {
	terminationSignal := TerminationSignal{
		QueryType: gbo.config.QueryType,
		ClientID:  clientID,
		Message:   fmt.Sprintf("All data processing completed for client %s", clientID),
	}

	// Serialize termination signal
	signalData, err := json.Marshal(terminationSignal)
	if err != nil {
		log.Printf("Failed to serialize termination signal: %v", err)
		return
	}

	// Send to map workers
	log.Printf("[query%d-orchestrator] [CLI%s | Q%d] Sending termination signal to map workers", gbo.config.QueryType, clientID, gbo.config.QueryType)
	if err := gbo.terminationProducer.Send(signalData, []string{}); err != 0 {
		log.Printf("[query%d-orchestrator] [CLI%s | Q%d] Failed to send termination signal to map workers: %v", gbo.config.QueryType, clientID, gbo.config.QueryType, err)
	}

	log.Printf("[query%d-orchestrator] [CLI%s | Q%d] ✅ Termination signals sent successfully", gbo.config.QueryType, clientID, gbo.config.QueryType)
}

// Start starts the orchestrator
func (gbo *GroupByOrchestrator) Start() {
	log.Printf("[query%d-orchestrator] [Q%d] Starting Group By Orchestrator...", gbo.config.QueryType, gbo.config.QueryType)

	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Printf("[query%d-orchestrator] [Q%d] Started consuming chunk notifications", gbo.config.QueryType, gbo.config.QueryType)

		for delivery := range *consumeChannel {
			log.Printf("[query%d-orchestrator] [Q%d] Received chunk notification: %d bytes", gbo.config.QueryType, gbo.config.QueryType, len(delivery.Body))

			// Deserialize chunk notification using protocol
			message, err := deserializer.Deserialize(delivery.Body)
			if err != nil {
				log.Printf("[query%d-orchestrator] [Q%d] Failed to deserialize chunk notification: %v", gbo.config.QueryType, gbo.config.QueryType, err)
				delivery.Ack(false)
				continue
			}

			notification, ok := message.(*signals.ChunkNotification)
			if !ok {
				log.Printf("[query%d-orchestrator] [Q%d] Received message is not a ChunkNotification: %T", gbo.config.QueryType, gbo.config.QueryType, message)
				delivery.Ack(false)
				continue
			}

			// Process the notification
			if err := gbo.ProcessChunkNotification(notification); err != nil {
				log.Printf("[query%d-orchestrator] [CLI%s | Q%d] Failed to process chunk notification: %v", gbo.config.QueryType, notification.ClientID, gbo.config.QueryType, err)
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

	log.Printf("[query%d-orchestrator] [Q%d] Successfully started consuming from queue: %s", gbo.config.QueryType, gbo.config.QueryType, queueName)
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
