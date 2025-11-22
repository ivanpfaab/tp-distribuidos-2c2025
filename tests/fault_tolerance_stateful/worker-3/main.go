package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

type Verifier struct {
	config         *Config
	consumer       *workerqueue.QueueConsumer
	expectedResults map[string]int // clientID -> expected sum
	receivedResults map[string]int  // clientID -> received sum
}

func NewVerifier(config *Config) (*Verifier, error) {
	// Wait for RabbitMQ to be ready
	log.Println("Worker 3: Waiting for RabbitMQ to be ready...")
	if err := middleware.WaitForConnection(config.RabbitMQ, 30, 1*time.Second); err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	log.Println("Worker 3: RabbitMQ is ready")

	// Create consumer
	consumer := workerqueue.NewQueueConsumer(config.InputQueue, config.RabbitMQ)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create consumer")
	}

	// Declare input queue
	inputQueueDeclarer := workerqueue.NewMessageMiddlewareQueue(config.InputQueue, config.RabbitMQ)
	if inputQueueDeclarer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create input queue declarer")
	}
	if err := inputQueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		inputQueueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare input queue: %d", err)
	}
	inputQueueDeclarer.Close()

	// Calculate expected results
	// For each client, expected sum = sum of (chunk_number * 10) for chunks 1 to numChunks
	expectedResults := make(map[string]int)
	receivedResults := make(map[string]int)

	clients := []string{"CLI1", "CLI2"}
	for _, clientID := range clients {
		sum := 0
		for i := 1; i <= config.NumChunks; i++ {
			sum += i * 10
		}
		expectedResults[clientID] = sum
		receivedResults[clientID] = 0
		log.Printf("Worker 3: Expected sum for client %s: %d", clientID, sum)
	}

	return &Verifier{
		config:          config,
		consumer:        consumer,
		expectedResults: expectedResults,
		receivedResults: receivedResults,
	}, nil
}

func (v *Verifier) Start() middleware.MessageMiddlewareError {
	log.Println("Worker 3: Starting")

	return v.consumer.StartConsuming(func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			err := v.processMessage(delivery)
			if err != 0 {
				log.Printf("Worker 3: Error processing message, requeuing")
				delivery.Nack(false, true)
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	})
}

func (v *Verifier) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	// Deserialize chunk
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		log.Printf("Worker 3: Failed to deserialize chunk: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	clientID := chunkMsg.ClientID

	// Check if this is a result chunk (FileID == "RS01")
	if chunkMsg.FileID == "RS01" {
		// This is a result chunk
		resultSum, err := strconv.Atoi(strings.TrimSpace(chunkMsg.ChunkData))
		if err != nil {
			log.Printf("Worker 3: Failed to parse result sum: %v", err)
			return middleware.MessageMiddlewareMessageError
		}

		v.receivedResults[clientID] = resultSum
		expectedSum := v.expectedResults[clientID]

		if resultSum == expectedSum {
			fmt.Printf("SUCCESS [%s] - Expected: %d, Received: %d\n", clientID, expectedSum, resultSum)
			log.Printf("Worker 3: SUCCESS [%s] - Expected: %d, Received: %d", clientID, expectedSum, resultSum)
		} else {
			fmt.Printf("FAILURE [%s] - Expected: %d, Received: %d\n", clientID, expectedSum, resultSum)
			log.Printf("Worker 3: FAILURE [%s] - Expected: %d, Received: %d", clientID, expectedSum, resultSum)
		}
	} else {
		// This is a forwarded chunk, just log it
		log.Printf("Worker 3: Received forwarded chunk for client %s (chunk %d)", clientID, chunkMsg.ChunkNumber)
	}

	return 0
}

func (v *Verifier) Close() {
	if v.consumer != nil {
		v.consumer.Close()
	}
}

func main() {
	log.Println("Worker 3: Starting Verifier...")

	// Load configuration
	config := LoadConfig()

	// Create verifier
	verifier, err := NewVerifier(config)
	if err != nil {
		log.Fatalf("Worker 3: Failed to create verifier: %v", err)
	}
	defer verifier.Close()

	// Start verifier
	if err := verifier.Start(); err != 0 {
		log.Fatalf("Worker 3: Failed to start verifier: %d", err)
	}

	log.Println("Worker 3: Started successfully. Waiting for messages...")

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Worker 3: Shutting down...")
}

