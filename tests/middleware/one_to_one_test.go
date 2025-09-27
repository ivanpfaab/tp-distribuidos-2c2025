package middleware

import (
	"testing"
	"time"
	"tp-distribuidos-2c2025/shared/middleware"
	"tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"tp-distribuidos-2c2025/shared/middleware/exchange"
)

// TestExchangeOneToOne tests 1 producer 1 consumer using exchange
func TestExchangeOneToOne(t *testing.T) {
	// Init connection
	config := &middleware.ConnectionConfig{
		URL: "amqp://admin:password@rabbitmq:5672/",
	}
	err := middleware.WaitForConnection(config, 10, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	// Init middleware
	producer := exchange.NewMessageMiddlewareExchange("test-exchange-1to1", []string{"test.key"}, config)
	consumer := exchange.NewExchangeConsumer("test-exchange-1to1", []string{"test.key"}, config)
	
	if producer == nil || consumer == nil {
		t.Fatal("Failed to create middleware")
	}

	// Declare exchange
	errCode := producer.DeclareExchange("topic", true, false, false, false)
	if errCode != 0 {
		t.Fatalf("Failed to declare exchange: %v", errCode)
	}

	// Send message
	message := []byte("Hello from exchange 1to1")
	errCode = producer.Send(message)
	if errCode != 0 {
		t.Fatalf("Failed to send message: %v", errCode)
	}

	// Check if message was received
	received := false
	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		delivery := <-*consumeChannel
		if string(delivery.Body) == string(message) {
			received = true
		}
		delivery.Ack(false)
		close(done)
	}

	errCode = consumer.StartConsuming(onMessageCallback)
	if errCode != 0 {
		t.Fatalf("Failed to start consuming: %v", errCode)
	}

	// Wait for message
	time.Sleep(10 * time.Second)

	// Close
	producer.Close()
	consumer.Close()

	if !received {
		t.Error("Message was not received")
	}
}

// TestWorkerQueueOneToOne tests 1 producer 1 consumer using worker queue
func TestWorkerQueueOneToOne(t *testing.T) {
	// Init connection
	config := &middleware.ConnectionConfig{
		URL: "amqp://admin:password@rabbitmq:5672/",
	}
	err := middleware.WaitForConnection(config, 10, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	// Init middleware
	producer := workerqueue.NewMessageMiddlewareQueue("test-queue-1to1", config)
	consumer := workerqueue.NewQueueConsumer("test-queue-1to1", config)
	
	if producer == nil || consumer == nil {
		t.Fatal("Failed to create middleware")
	}

	// Declare queue
	errCode := producer.DeclareQueue(true, false, false, false)
	if errCode != 0 {
		t.Fatalf("Failed to declare queue: %v", errCode)
	}

	// Send message
	message := []byte("Hello from worker queue 1to1")
	errCode = producer.Send(message)
	if errCode != 0 {
		t.Fatalf("Failed to send message: %v", errCode)
	}

	// Check if message was received
	received := false
	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		delivery := <-*consumeChannel
		if string(delivery.Body) == string(message) {
			received = true
		}
		delivery.Ack(false)
		close(done)
	}

	errCode = consumer.StartConsuming(onMessageCallback)
	if errCode != 0 {
		t.Fatalf("Failed to start consuming: %v", errCode)
	}

	// Wait for message
	time.Sleep(2 * time.Second)

	// Close
	producer.Close()
	consumer.Close()

	if !received {
		t.Error("Message was not received")
	}
}
