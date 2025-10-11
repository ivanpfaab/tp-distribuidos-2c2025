package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/batch"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	datahandler "github.com/tp-distribuidos-2c2025/server/controller/data-handler"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

const (
	ServerBatchQueue = "server-batch-queue"
)

// RabbitMQServer handles RabbitMQ-based batch processing
type RabbitMQServer struct {
	config        *middleware.ConnectionConfig
	batchConsumer *workerqueue.QueueConsumer
	dataHandler   *datahandler.DataHandler
	ackProducers  map[string]*workerqueue.QueueMiddleware // clientID -> ack producer
}

// NewRabbitMQServer creates a new RabbitMQ-based server
func NewRabbitMQServer(config *middleware.ConnectionConfig) (*RabbitMQServer, error) {
	server := &RabbitMQServer{
		config:       config,
		ackProducers: make(map[string]*workerqueue.QueueMiddleware),
	}

	// Create data handler
	server.dataHandler = datahandler.NewDataHandler(config)
	if err := server.dataHandler.Initialize(); err != 0 {
		return nil, fmt.Errorf("failed to initialize data handler: %v", err)
	}

	// Create batch consumer
	server.batchConsumer = workerqueue.NewQueueConsumer(ServerBatchQueue, config)
	if server.batchConsumer == nil {
		return nil, fmt.Errorf("failed to create batch consumer")
	}

	log.Printf("RabbitMQ Server created")
	return server, nil
}

// Start starts the server to consume batches
func (s *RabbitMQServer) Start() error {
	// Declare the server batch queue using QueueMiddleware before consuming
	queueDeclarer := workerqueue.NewMessageMiddlewareQueue(ServerBatchQueue, s.config)
	if queueDeclarer == nil {
		return fmt.Errorf("failed to create queue declarer for server batch queue")
	}

	if err := queueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		queueDeclarer.Close()
		return fmt.Errorf("failed to declare server batch queue: %v", err)
	}
	log.Printf("RabbitMQ Server: Declared queue %s", ServerBatchQueue)

	// Close the declarer as we only needed it for queue declaration
	queueDeclarer.Close()

	// Start consuming batches
	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			err := s.processBatchMessage(delivery)
			if err != nil {
				log.Printf("RabbitMQ Server: Error processing batch: %v", err)
			}

			// Acknowledge the message
			delivery.Ack(false)
		}
		done <- nil
	}

	if err := s.batchConsumer.StartConsuming(onMessageCallback); err != 0 {
		return fmt.Errorf("failed to start consuming batches: %v", err)
	}

	log.Printf("RabbitMQ Server started, consuming from %s", ServerBatchQueue)
	return nil
}

// processBatchMessage processes a batch message and sends acknowledgment
func (s *RabbitMQServer) processBatchMessage(delivery amqp.Delivery) error {
	// Deserialize the message
	message, err := deserializer.Deserialize(delivery.Body)
	if err != nil {
		return fmt.Errorf("failed to deserialize message: %w", err)
	}

	// Check if it's a Batch message
	batchMsg, ok := message.(*batch.Batch)
	if !ok {
		return fmt.Errorf("expected batch message, got %T", message)
	}

	log.Printf("RabbitMQ Server: Received batch - ClientID: %s, FileID: %s, BatchNumber: %d",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber)

	// Process the batch message with the data handler
	if err := s.dataHandler.ProcessBatchMessage(delivery.Body); err != nil {
		log.Printf("RabbitMQ Server: Failed to process batch with data handler: %v", err)
		return fmt.Errorf("failed to process batch with data handler: %w", err)
	}

	log.Printf("RabbitMQ Server: Successfully processed batch - ClientID: %s, FileID: %s, BatchNumber: %d",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber)

	// Send acknowledgment back to the client
	ackMessage := fmt.Sprintf("ACK: Batch received - ClientID: %s, FileID: %s, BatchNumber: %d",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber)

	if err := s.sendAcknowledgment(batchMsg.ClientID, ackMessage); err != nil {
		log.Printf("RabbitMQ Server: Failed to send acknowledgment: %v", err)
		return err
	}

	log.Printf("RabbitMQ Server: Sent acknowledgment to client %s", batchMsg.ClientID)
	return nil
}

// sendAcknowledgment sends an acknowledgment to a client-specific queue
func (s *RabbitMQServer) sendAcknowledgment(clientID string, ackMessage string) error {
	// Get or create ack producer for this client
	ackProducer, exists := s.ackProducers[clientID]
	if !exists {
		// Create new ack producer for this client
		ackQueueName := fmt.Sprintf("client-%s-acks", clientID)
		ackProducer = workerqueue.NewMessageMiddlewareQueue(ackQueueName, s.config)
		if ackProducer == nil {
			return fmt.Errorf("failed to create ack producer for client %s", clientID)
		}

		// Declare the client acknowledgment queue
		if err := ackProducer.DeclareQueue(false, false, false, false); err != 0 {
			return fmt.Errorf("failed to declare ack queue for client %s: %v", clientID, err)
		}

		s.ackProducers[clientID] = ackProducer
		log.Printf("RabbitMQ Server: Created ack producer for client %s", clientID)
	}

	// Send acknowledgment
	if err := ackProducer.Send([]byte(ackMessage)); err != 0 {
		return fmt.Errorf("failed to send ack to client %s: %v", clientID, err)
	}

	return nil
}

// Close closes all connections
func (s *RabbitMQServer) Close() error {
	log.Printf("RabbitMQ Server: Closing connections")

	var lastErr error

	if s.batchConsumer != nil {
		if err := s.batchConsumer.Close(); err != 0 {
			lastErr = fmt.Errorf("failed to close batch consumer: %v", err)
		}
	}

	// Close all ack producers
	for clientID, producer := range s.ackProducers {
		if err := producer.Close(); err != 0 {
			log.Printf("RabbitMQ Server: Failed to close ack producer for client %s: %v", clientID, err)
			lastErr = fmt.Errorf("failed to close ack producer for client %s: %v", clientID, err)
		}
	}

	if s.dataHandler != nil {
		if err := s.dataHandler.Close(); err != 0 {
			lastErr = fmt.Errorf("failed to close data handler: %v", err)
		}
	}

	return lastErr
}
