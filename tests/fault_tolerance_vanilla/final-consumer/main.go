package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

func main() {
	config := LoadConfig()

	log.Println("Final Consumer: Starting")

	// Wait for RabbitMQ to be ready
	log.Println("Final Consumer: Waiting for RabbitMQ to be ready...")
	if err := middleware.WaitForConnection(config.RabbitMQ, 30, 1*time.Second); err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	log.Println("Final Consumer: RabbitMQ is ready")

	// Create consumer for final queue
	consumer := workerqueue.NewQueueConsumer("queue-final", config.RabbitMQ)
	if consumer == nil {
		log.Fatal("Failed to create consumer")
	}
	defer consumer.Close()

	// Declare queue (non-durable, non-auto-delete)
	queueDeclarer := workerqueue.NewMessageMiddlewareQueue("queue-final", config.RabbitMQ)
	if queueDeclarer == nil {
		log.Fatal("Failed to create queue declarer")
	}
	if err := queueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		log.Fatalf("Failed to declare queue: %v", err)
	}
	queueDeclarer.Close()

	// Start consuming
	log.Println("Final Consumer: Starting to consume from queue-final...")
	if err := consumer.StartConsuming(func(consumeChannel middleware.ConsumeChannel, done chan error) {
		chunkCount := 0
		for delivery := range *consumeChannel {
			chunkCount++

			// Deserialize chunk
			chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
			if err != nil {
				log.Printf("Final Consumer: Failed to deserialize chunk: %v", err)
				delivery.Nack(false, true)
				continue
			}

			// Print the result
			log.Printf("Final Consumer: [Chunk %d] ClientID: %s, FileID: %s, ChunkNumber: %d, Data: %s",
				chunkCount, chunkMsg.ClientID, chunkMsg.FileID, chunkMsg.ChunkNumber, chunkMsg.ChunkData)

			delivery.Ack(false)
		}
		done <- nil
	}); err != 0 {
		log.Fatalf("Failed to start consuming: %d", err)
	}

	log.Println("Final Consumer: Running")

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Final Consumer: Shutting down")
}
