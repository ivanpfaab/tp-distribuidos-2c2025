package main

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/tests/fault_tolerance_vanilla/utils"
)

type Worker struct {
	config          *Config
	consumer        *workerqueue.QueueConsumer
	producer        *workerqueue.QueueMiddleware
	messageManager  *messagemanager.MessageManager
	duplicateSender *utils.DuplicateSender
}

func NewWorker(config *Config) (*Worker, error) {
	// Wait for RabbitMQ to be ready
	log.Printf("Worker %d: Waiting for RabbitMQ to be ready...", config.WorkerID)
	if err := middleware.WaitForConnection(config.RabbitMQ, 30, 1*time.Second); err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	log.Printf("Worker %d: RabbitMQ is ready", config.WorkerID)

	// Create consumer for input queue
	consumer := workerqueue.NewQueueConsumer(config.InputQueue, config.RabbitMQ)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create consumer")
	}

	// Create producer for output queue
	producer := workerqueue.NewMessageMiddlewareQueue(config.OutputQueue, config.RabbitMQ)
	if producer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create producer")
	}

	// Declare input queue using a temporary QueueMiddleware (consumer doesn't have DeclareQueue)
	inputQueueDeclarer := workerqueue.NewMessageMiddlewareQueue(config.InputQueue, config.RabbitMQ)
	if inputQueueDeclarer == nil {
		consumer.Close()
		producer.Close()
		return nil, fmt.Errorf("failed to create input queue declarer")
	}
	if err := inputQueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		producer.Close()
		inputQueueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare input queue: %d", err)
	}
	inputQueueDeclarer.Close()

	// Declare output queue
	if err := producer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		producer.Close()
		return nil, fmt.Errorf("failed to declare output queue: %d", err)
	}

	// Initialize MessageManager
	messageManager := messagemanager.NewMessageManager("/app/worker-data/processed-ids.txt")
	if err := messageManager.LoadProcessedIDs(); err != nil {
		log.Printf("Worker %d: Warning - failed to load processed IDs: %v (starting with empty state)", config.WorkerID, err)
	} else {
		count := messageManager.GetProcessedCount()
		log.Printf("Worker %d: Loaded %d processed IDs", config.WorkerID, count)
	}

	// Initialize DuplicateSender
	duplicateSender := utils.NewDuplicateSender(config.DuplicateRate)
	if config.DuplicateRate > 0.0 {
		log.Printf("Worker %d: Duplicate rate enabled: %.2f%%", config.WorkerID, config.DuplicateRate*100)
	}

	return &Worker{
		config:          config,
		consumer:        consumer,
		producer:        producer,
		messageManager:  messageManager,
		duplicateSender: duplicateSender,
	}, nil
}

func (w *Worker) Start() middleware.MessageMiddlewareError {
	log.Printf("Worker %d: Starting", w.config.WorkerID)

	return w.consumer.StartConsuming(func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			err := w.processMessage(delivery)
			if err != 0 {
				log.Printf("Worker %d: Error processing message, requeuing", w.config.WorkerID)
				delivery.Nack(false, true)
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	})
}

func (w *Worker) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	// Deserialize chunk
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		log.Printf("Worker %d: Failed to deserialize chunk: %v", w.config.WorkerID, err)
		return middleware.MessageMiddlewareMessageError
	}

	// Check if already processed
	if w.messageManager.IsProcessed(chunkMsg.ID) {
		log.Printf("Worker %d: Chunk ID %s already processed, skipping", w.config.WorkerID, chunkMsg.ID)
		return 0 // Return 0 to ACK (handled by Start method)
	}

	log.Printf("Worker %d: Received chunk %s", w.config.WorkerID, chunkMsg.ChunkData)

	// Wait 3 seconds before forwarding
	time.Sleep(3 * time.Second)

	var serialized []byte

	if w.duplicateSender.ShouldSendDuplicate() {
		// Send a duplicate
		duplicateID, duplicateData, found := w.duplicateSender.GetRandomDuplicate()
		if found {
			serialized = duplicateData
			log.Printf("Worker %d: Sending DUPLICATE chunk (ID: %s)", w.config.WorkerID, duplicateID)
		} else {
			// Fallback to normal processing if no duplicates available
			chunkMessage := chunk.NewChunkMessage(chunkMsg)
			var err error
			serialized, err = chunk.SerializeChunkMessage(chunkMessage)
			if err != nil {
				log.Printf("Worker %d: Failed to serialize chunk: %v", w.config.WorkerID, err)
				return middleware.MessageMiddlewareMessageError
			}
		}

		// Send to output queue
		if sendErr := w.producer.Send(serialized); sendErr != 0 {
			log.Printf("Worker %d: Failed to send chunk: %v", w.config.WorkerID, sendErr)
		}

	}
	// Normal processing: forward the chunk
	chunkMessage := chunk.NewChunkMessage(chunkMsg)
	serialized, err = chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		log.Printf("Worker %d: Failed to serialize chunk: %v", w.config.WorkerID, err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send to output queue
	if sendErr := w.producer.Send(serialized); sendErr != 0 {
		log.Printf("Worker %d: Failed to send chunk: %v", w.config.WorkerID, sendErr)
		return sendErr
	}

	// Store chunk for potential future duplicates (only if it's a new chunk, not a duplicate)
	w.duplicateSender.StoreChunk(chunkMsg, serialized)

	// Mark as processed (must be after successful send)
	if markErr := w.messageManager.MarkProcessed(chunkMsg.ID); markErr != nil {
		log.Printf("Worker %d: Failed to mark chunk as processed: %v", w.config.WorkerID, markErr)
		return middleware.MessageMiddlewareMessageError // Will NACK and requeue
	}

	log.Printf("Worker %d: Sent chunk", w.config.WorkerID)
	return 0
}

func (w *Worker) Close() {
	if w.messageManager != nil {
		w.messageManager.Close()
	}
	if w.consumer != nil {
		w.consumer.Close()
	}
	if w.producer != nil {
		w.producer.Close()
	}
}
