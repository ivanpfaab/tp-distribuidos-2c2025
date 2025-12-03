package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	completioncleaner "github.com/tp-distribuidos-2c2025/shared/completion_cleaner"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	worker_builder "github.com/tp-distribuidos-2c2025/shared/worker_builder"
)

// logWithTimestamp prints a formatted message with timestamp
func logWithTimestamp(format string, args ...interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	fmt.Printf("[%s] %s\n", timestamp, fmt.Sprintf(format, args...))
}

// JoinDataHandler encapsulates the join data handler state and dependencies
type JoinDataHandler struct {
	consumer           *workerqueue.QueueConsumer
	itemIdProducer     *exchange.ExchangeMiddleware
	storeIdProducer    *exchange.ExchangeMiddleware
	userIdProducer     *workerqueue.QueueMiddleware
	config             *middleware.ConnectionConfig
	itemIdWorkerCount  int // Number of ItemID worker instances to broadcast to
	storeIdWorkerCount int // Number of StoreID worker instances to broadcast to
	messageManager     *messagemanager.MessageManager
}

// NewJoinDataHandler creates a new JoinDataHandler instance
func NewJoinDataHandler(config *middleware.ConnectionConfig) (*JoinDataHandler, error) {
	// Get the number of ItemID workers from environment (defaults to 1)
	itemIdWorkerCountStr := os.Getenv("ITEMID_WORKER_COUNT")
	itemIdWorkerCount := 1
	if itemIdWorkerCountStr != "" {
		if count, err := strconv.Atoi(itemIdWorkerCountStr); err == nil && count > 0 {
			itemIdWorkerCount = count
		}
	}

	// Get the number of StoreID workers from environment (defaults to 1)
	storeIdWorkerCountStr := os.Getenv("STOREID_WORKER_COUNT")
	storeIdWorkerCount := 1
	if storeIdWorkerCountStr != "" {
		if count, err := strconv.Atoi(storeIdWorkerCountStr); err == nil && count > 0 {
			storeIdWorkerCount = count
		}
	}

	logWithTimestamp("Join Data Handler: Initializing with %d ItemID worker instance(s) and %d StoreID worker instance(s)",
		itemIdWorkerCount, storeIdWorkerCount)

	// Use builder to create all resources
	builder := worker_builder.NewWorkerBuilder("Join Data Handler").
		WithConfig(config).
		// Queue consumer
		WithQueueConsumer(FixedJoinDataQueue, true).
		// Exchange producers (direct type for broadcasting)
		WithExchangeProducer(JoinItemIdDictionaryExchange, []string{JoinItemIdDictionaryRoutingKey}, true,
			worker_builder.ExchangeDeclarationOptions{
				Type:       "direct",
				Durable:    false,
				AutoDelete: false,
				Internal:   false,
				NoWait:     false,
			}).
		WithExchangeProducer(JoinStoreIdDictionaryExchange, []string{JoinStoreIdDictionaryRoutingKey}, true,
			worker_builder.ExchangeDeclarationOptions{
				Type:       "direct",
				Durable:    false,
				AutoDelete: false,
				Internal:   false,
				NoWait:     false,
			}).
		// Queue producer
		WithQueueProducer(JoinUserIdDictionaryQueue, true).
		// State management
		WithStandardStateDirectory("/app/worker-data").
		WithMessageManager("/app/worker-data/processed-ids.txt")

	// Validate builder
	if err := builder.Validate(); err != nil {
		return nil, builder.CleanupOnError(err)
	}

	// Extract resources from builder
	consumer := builder.GetQueueConsumer(FixedJoinDataQueue)
	itemIdProducer := builder.GetExchangeProducer(JoinItemIdDictionaryExchange)
	storeIdProducer := builder.GetExchangeProducer(JoinStoreIdDictionaryExchange)
	userIdProducer := builder.GetQueueProducer(JoinUserIdDictionaryQueue)

	if consumer == nil || itemIdProducer == nil || storeIdProducer == nil || userIdProducer == nil {
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

	// Add CompletionCleaner with MessageManager as cleanup handler
	// Use WORKER_ID from environment (service name) for cleanup queue name
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		return nil, fmt.Errorf("WORKER_ID environment variable is required")
	}
	builder.WithCompletionCleaner(
		queues.ClientCompletionCleanupExchange,
		workerID,
		[]completioncleaner.CleanupHandler{mm},
	)

	return &JoinDataHandler{
		consumer:           consumer,
		itemIdProducer:     itemIdProducer,
		storeIdProducer:    storeIdProducer,
		userIdProducer:     userIdProducer,
		config:             config,
		itemIdWorkerCount:  itemIdWorkerCount,
		storeIdWorkerCount: storeIdWorkerCount,
		messageManager:     mm,
	}, nil
}

// Start starts the join data handler
func (jdh *JoinDataHandler) Start() middleware.MessageMiddlewareError {
	logWithTimestamp("Join Data Handler: Starting to listen for fixed join data...")
	return jdh.consumer.StartConsuming(jdh.createCallback())
}

// Close closes all connections
func (jdh *JoinDataHandler) Close() {
	if jdh.messageManager != nil {
		jdh.messageManager.Close()
	}
	if jdh.consumer != nil {
		jdh.consumer.Close()
	}
	if jdh.itemIdProducer != nil {
		jdh.itemIdProducer.Close()
	}
	if jdh.storeIdProducer != nil {
		jdh.storeIdProducer.Close()
	}
	if jdh.userIdProducer != nil {
		jdh.userIdProducer.Close()
	}
}

// createCallback creates the message processing callback
func (jdh *JoinDataHandler) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
			if err != nil {
				logWithTimestamp("Join Data Handler: Failed to deserialize chunk message: %v", err)
				delivery.Nack(false, false) // Reject the message
				continue
			}

			if err := jdh.processMessage(chunkMsg, delivery.Body); err != 0 {
				logWithTimestamp("Join Data Handler: Failed to process message: %v", err)
				if err == middleware.MessageMiddlewareMessageError {
					delivery.Nack(false, true) // Reject and requeue
				} else {
					delivery.Nack(false, false) // Reject without requeue
				}
				continue
			}
			delivery.Ack(false) // Acknowledge the original message
		}
		done <- nil
	}
}

// processMessage processes a single message and routes it to the appropriate dictionary queue/exchange
func (jdh *JoinDataHandler) processMessage(chunkMsg *chunk.Chunk, messageData []byte) middleware.MessageMiddlewareError {
	// Check if chunk was already processed
	if jdh.messageManager.IsProcessed(chunkMsg.ClientID, chunkMsg.ID) {
		logWithTimestamp("Join Data Handler: Chunk %s already processed, skipping", chunkMsg.ID)
		return 0
	}

	// Route based on FileID
	sendErr := jdh.routeAndSendByFileId(chunkMsg.FileID, messageData)
	if sendErr != 0 {
		logWithTimestamp("Join Data Handler: Failed to route chunk with FileID %s: %v", chunkMsg.FileID, sendErr)
		return sendErr
	}

	// Mark chunk as processed after successful routing
	if err := jdh.messageManager.MarkProcessed(chunkMsg.ClientID, chunkMsg.ID); err != nil {
		logWithTimestamp("Join Data Handler: Failed to mark chunk as processed: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	logWithTimestamp("Join Data Handler: Successfully routed chunk with FileID %s", chunkMsg.FileID)
	return 0
}

// routeAndSendByFileId routes and sends message based on FileID
func (jdh *JoinDataHandler) routeAndSendByFileId(fileId string, messageData []byte) middleware.MessageMiddlewareError {
	fileIdUpper := strings.ToUpper(fileId)
	logWithTimestamp("Join Data Handler: Received message with FileID %s", fileId)

	if strings.Contains(fileIdUpper, "MN") {
		logWithTimestamp("Join Data Handler: Received FileID %s, routing to %d ItemID worker instance(s)",
			fileId, jdh.itemIdWorkerCount)
		// ItemID: Send to exchange (broadcast to all worker instances)
		// Generate routing keys for all ItemID worker instances
		routingKeys := make([]string, jdh.itemIdWorkerCount)
		for i := 0; i < jdh.itemIdWorkerCount; i++ {
			instanceID := i + 1 // 1-indexed
			routingKeys[i] = fmt.Sprintf("%s-instance-%d", JoinItemIdDictionaryRoutingKey, instanceID)
		}

		logWithTimestamp("Join Data Handler: Broadcasting FileID %s to %d ItemID worker instance(s) with routing keys: %v",
			fileId, jdh.itemIdWorkerCount, routingKeys)
		return jdh.itemIdProducer.Send(messageData, routingKeys)
	} else if strings.Contains(fileIdUpper, "ST") {
		logWithTimestamp("Join Data Handler: Received FileID %s, routing to %d StoreID worker instance(s)",
			fileId, jdh.storeIdWorkerCount)
		// StoreID: Send to exchange (broadcast to all worker instances)
		// Generate routing keys for all StoreID worker instances
		routingKeys := make([]string, jdh.storeIdWorkerCount)
		for i := 0; i < jdh.storeIdWorkerCount; i++ {
			instanceID := i + 1 // 1-indexed
			routingKeys[i] = fmt.Sprintf("%s-instance-%d", JoinStoreIdDictionaryRoutingKey, instanceID)
		}

		logWithTimestamp("Join Data Handler: Broadcasting FileID %s to %d StoreID worker instance(s) with routing keys: %v",
			fileId, jdh.storeIdWorkerCount, routingKeys)
		return jdh.storeIdProducer.Send(messageData, routingKeys)
	} else if strings.Contains(fileIdUpper, "US") {
		logWithTimestamp("Join Data Handler: Received FileID %s, routing to UserID dictionary queue", fileId)
		// UserID: Send to queue (single worker)
		logWithTimestamp("Join Data Handler: Routing FileID %s to UserID dictionary queue", fileId)
		return jdh.userIdProducer.Send(messageData)
	}

	logWithTimestamp("Join Data Handler: Unknown FileID: %s, ignoring message", fileId)
	return 0 // Success - just ignore unknown file types
}
