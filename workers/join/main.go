package main

import (
	"fmt"
	"os"
	"strconv"
	"tp-distribuidos-2c2025/protocol/chunk"
	"tp-distribuidos-2c2025/shared/middleware"
	"tp-distribuidos-2c2025/shared/middleware/exchange"
)

func main() {
	// Get configuration from environment variables or use defaults
	host := getEnv("RABBITMQ_HOST", "localhost")
	port := getEnv("RABBITMQ_PORT", "5672")
	portInt, err := strconv.Atoi(port)
	if err != nil {
		fmt.Printf("Invalid port: %v\n", err)
		return
	}
	username := getEnv("RABBITMQ_USER", "admin")
	password := getEnv("RABBITMQ_PASS", "password")

	fmt.Printf("Join Worker: Connecting to RabbitMQ at %s:%s with user %s\n", host, port, username)

	// Create connection configuration
	config := &middleware.ConnectionConfig{
		Host:     host,
		Port:     portInt,
		Username: username,
		Password: password,
	}

	// Create Join consumer
	joinConsumer := exchange.NewExchangeConsumer(
		"join-exchange",
		[]string{"join"},
		config,
	)
	if joinConsumer == nil {
		fmt.Println("Failed to create join consumer")
		return
	}
	defer joinConsumer.Close()

	fmt.Println("Join Worker: Starting to listen for messages...")

	// Start consuming messages
	if err := joinConsumer.StartConsuming(joinCallback); err != 0 {
		fmt.Printf("Failed to start consuming: %v\n", err)
		return
	}

	// Keep the worker running
	select {}
}

// joinCallback processes incoming chunk messages for joining
func joinCallback(consumeChannel middleware.ConsumeChannel, done chan error) {
	fmt.Println("Join Worker: Starting to listen for messages...")
	for delivery := range *consumeChannel {
		fmt.Printf("Join Worker: Received message: %s\n", string(delivery.Body))

		// Deserialize the chunk message
		chunkMsg, err := chunk.DeserializeChunkMessage(delivery.Body)
		if err != nil {
			fmt.Printf("Join Worker: Failed to deserialize chunk message: %v\n", err)
			delivery.Nack(false, true) // Reject and requeue
			continue
		}

		// Process the chunk (join logic would go here)
		fmt.Printf("Join Worker: Processing chunk - QueryType: %d, Step: %d, ClientID: %s, ChunkNumber: %d\n",
			chunkMsg.QueryType, chunkMsg.Step, chunkMsg.ClientID, chunkMsg.ChunkNumber)

		// TODO: Implement actual join logic here
		// For now, just acknowledge the message
		delivery.Ack(false)
	}
	done <- nil
}

// Helper function to get environment variable with default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
