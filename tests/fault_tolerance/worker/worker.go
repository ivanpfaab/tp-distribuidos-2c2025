package main

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	statemanager "github.com/tp-distribuidos-2c2025/shared/state_manager"
)

type Worker struct {
	config       *Config
	consumer     *workerqueue.QueueConsumer
	producer     *workerqueue.QueueMiddleware
	stateManager *statemanager.StateManager
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

	// Initialize StateManager
	stateManager := statemanager.NewStateManager("/app/worker-data/processed-ids.txt")
	if err := stateManager.LoadProcessedIDs(); err != nil {
		log.Printf("Worker %d: Warning - failed to load processed IDs: %v (starting with empty state)", config.WorkerID, err)
	} else {
		count := stateManager.GetProcessedCount()
		log.Printf("Worker %d: Loaded %d processed IDs", config.WorkerID, count)
	}

	return &Worker{
		config:       config,
		consumer:     consumer,
		producer:     producer,
		stateManager: stateManager,
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
	if w.stateManager.IsProcessed(chunkMsg.ID) {
		log.Printf("Worker %d: Chunk ID already processed, skipping", w.config.WorkerID)
		return 0 // Return 0 to ACK (handled by Start method)
	}

	log.Printf("Worker %d: Received chunk %s", w.config.WorkerID, chunkMsg.ChunkData)

	// Wait 3 seconds before forwarding
	time.Sleep(3 * time.Second)

	// Forward to output queue
	chunkMessage := chunk.NewChunkMessage(chunkMsg)
	serialized, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		log.Printf("Worker %d: Failed to serialize chunk: %v", w.config.WorkerID, err)
		return middleware.MessageMiddlewareMessageError
	}

	if err := w.producer.Send(serialized); err != 0 {
		log.Printf("Worker %d: Failed to send chunk: %v", w.config.WorkerID, err)
		return err
	}

	// Mark as processed (must be after successful send)
	if err := w.stateManager.MarkProcessed(chunkMsg.ID); err != nil {
		log.Printf("Worker %d: Failed to mark chunk as processed: %v", w.config.WorkerID, err)
		return middleware.MessageMiddlewareMessageError // Will NACK and requeue
	}

	log.Printf("Worker %d: Sent chunk", w.config.WorkerID)
	return 0
}

func (w *Worker) Close() {
	if w.stateManager != nil {
		w.stateManager.Close()
	}
	if w.consumer != nil {
		w.consumer.Close()
	}
	if w.producer != nil {
		w.producer.Close()
	}
}
