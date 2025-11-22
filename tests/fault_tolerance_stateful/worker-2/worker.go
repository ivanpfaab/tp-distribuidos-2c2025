package main

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	statefulworker "github.com/tp-distribuidos-2c2025/shared/stateful_worker"
)

type Worker struct {
	config       *Config
	consumer     *workerqueue.QueueConsumer
	producer     *workerqueue.QueueMiddleware
	stateManager *statefulworker.StatefulWorkerManager
}

func NewWorker(config *Config) (*Worker, error) {
	// Wait for RabbitMQ to be ready
	log.Println("Worker 2: Waiting for RabbitMQ to be ready...")
	if err := middleware.WaitForConnection(config.RabbitMQ, 30, 1*time.Second); err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	log.Println("Worker 2: RabbitMQ is ready")

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

	// Declare input queue
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

	// Initialize StatefulWorkerManager
	csvColumns := []string{"msg_id", "chunk_number", "value"}
	stateManager := statefulworker.NewStatefulWorkerManager(
		"/app/worker-data/metadata",
		buildTestStatus(config.ExpectedChunks),
		extractTestMetadataRow,
		csvColumns,
	)

	// Rebuild state from persisted metadata
	log.Println("Worker 2: Rebuilding state from metadata...")
	if err := stateManager.RebuildState(); err != nil {
		log.Printf("Worker 2: Warning - failed to rebuild state: %v", err)
	} else {
		log.Printf("Worker 2: State rebuilt successfully")
	}

	return &Worker{
		config:       config,
		consumer:     consumer,
		producer:     producer,
		stateManager: stateManager,
	}, nil
}

func (w *Worker) Start() middleware.MessageMiddlewareError {
	log.Println("Worker 2: Starting")

	return w.consumer.StartConsuming(func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			err := w.processMessage(delivery)
			if err != 0 {
				log.Printf("Worker 2: Error processing message, requeuing")
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
		log.Printf("Worker 2: Failed to deserialize chunk: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	clientID := chunkMsg.ClientID

	// Process message through state manager (handles duplicate detection, state update, CSV append)
	if err := w.stateManager.ProcessMessage(chunkMsg); err != nil {
		log.Printf("Worker 2: Failed to process message: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Get client state
	state := w.stateManager.GetClientState(clientID)
	if state == nil {
		log.Printf("Worker 2: No state found for client %s", clientID)
		return 0
	}

	testState := state.(*TestClientState)

	// Check if client is ready
	if testState.IsReady() {
		log.Printf("Worker 2: Client %s is ready! Aggregated sum: %d", clientID, testState.AggregatedSum)

		// Generate result chunk
		resultData := fmt.Sprintf("%d", testState.AggregatedSum)
		resultChunk := chunk.NewChunk(
			clientID,
			"RS01",
			1,                        // QueryType
			testState.ExpectedChunks, // ChunkNumber
			true,                     // IsLastChunk
			true,                     // IsLastFromTable
			len(resultData),
			0,
			resultData,
		)

		// Serialize and send result chunk
		chunkMessage := chunk.NewChunkMessage(resultChunk)
		serialized, err := chunk.SerializeChunkMessage(chunkMessage)
		if err != nil {
			log.Printf("Worker 2: Failed to serialize result chunk: %v", err)
			return middleware.MessageMiddlewareMessageError
		}

		if err := w.producer.Send(serialized); err != 0 {
			log.Printf("Worker 2: Failed to send result chunk: %v", err)
			return err
		}

		log.Printf("Worker 2: Sent result chunk for client %s (sum: %d)", clientID, testState.AggregatedSum)

		// Mark client as ready (deletes CSV and state)
		if err := w.stateManager.MarkClientReady(clientID); err != nil {
			log.Printf("Worker 2: Failed to mark client ready: %v", err)
		}
	} else {
		log.Printf("Worker 2: Client %s not ready yet (%d/%d chunks, sum: %d)",
			clientID, len(testState.ReceivedChunks), testState.ExpectedChunks, testState.AggregatedSum)
	}

	// Forward original chunk to next worker
	chunkMessage := chunk.NewChunkMessage(chunkMsg)
	serialized, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		log.Printf("Worker 2: Failed to serialize chunk: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	if err := w.producer.Send(serialized); err != 0 {
		log.Printf("Worker 2: Failed to forward chunk: %v", err)
		return err
	}

	return 0
}

func (w *Worker) Close() {
	if w.consumer != nil {
		w.consumer.Close()
	}
	if w.producer != nil {
		w.producer.Close()
	}
}
