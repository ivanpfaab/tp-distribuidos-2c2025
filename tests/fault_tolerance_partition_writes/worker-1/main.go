package main

import (
	"log"
	"time"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

func main() {
	config := LoadConfig()

	log.Println("Worker 1: Starting")

	// Wait for RabbitMQ to be ready
	log.Println("Worker 1: Waiting for RabbitMQ to be ready...")
	if err := middleware.WaitForConnection(config.RabbitMQ, 30, 1*time.Second); err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	log.Println("Worker 1: RabbitMQ is ready")

	// Create producer
	producer := workerqueue.NewMessageMiddlewareQueue(config.OutputQueue, config.RabbitMQ)
	if producer == nil {
		log.Fatal("Failed to create producer")
	}
	defer producer.Close()

	// Declare queue
	if err := producer.DeclareQueue(false, false, false, false); err != 0 {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	// Worker 1 just forwards chunks from test-runner to Worker 2
	// The test-runner sends directly to queue-1-2, so Worker 1 is not needed
	// But we keep it for consistency with the architecture
	log.Println("Worker 1: Ready (test-runner sends directly to Worker 2)")

	// Keep running
	select {}
}

