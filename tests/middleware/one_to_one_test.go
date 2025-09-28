package middleware

import (
	"testing"
	"time"
	"tp-distribuidos-2c2025/shared/middleware"
	"tp-distribuidos-2c2025/shared/middleware/exchange"
	"tp-distribuidos-2c2025/shared/middleware/workerqueue"
	testing_utils "tp-distribuidos-2c2025/shared/testing"
)

// TestExchangeOneToOne tests 1 producer 1 consumer using exchange
func TestExchangeOneToOne(t *testing.T) {
	testing_utils.InitLogger()
	testing_utils.LogTest("Testing Exchange One-to-One pattern")

	// Init connection
	config := &middleware.ConnectionConfig{
		URL: "amqp://admin:password@rabbitmq:5672/",
	}
	err := middleware.WaitForConnection(config, 10, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	testing_utils.LogStep("Connected to RabbitMQ")

	// Init middleware
	testing_utils.LogStep("Creating exchange producer and consumer")
	producer := exchange.NewMessageMiddlewareExchange("test-exchange-1to1", []string{"test.key"}, config)
	consumer := exchange.NewExchangeConsumer("test-exchange-1to1", []string{"test.key"}, config)

	if producer == nil || consumer == nil {
		t.Fatal("Failed to create middleware")
	}

	// Declare exchange
	testing_utils.LogStep("Declaring exchange")
	errCode := producer.DeclareExchange("topic", true, false, false, false)
	if errCode != 0 {
		t.Fatalf("Failed to declare exchange: %v", errCode)
	}

	// Define message content
	message := []byte("Hello from exchange 1to1")

	// Check if message was received
	received := false
	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		delivery := <-*consumeChannel
		if string(delivery.Body) == string(message) {
			received = true
			testing_utils.LogStep("Consumer received message: %s", string(delivery.Body))
		}
		delivery.Ack(false)
		close(done)
	}

	testing_utils.LogStep("Starting consumer")
	errCode = consumer.StartConsuming(onMessageCallback)
	if errCode != 0 {
		t.Fatalf("Failed to start consuming: %v", errCode)
	}

	// Small delay to ensure consumer is ready
	time.Sleep(100 * time.Millisecond)

	// Send message
	testing_utils.LogStep("Sending message")
	errCode = producer.Send(message, []string{"test.key"})
	if errCode != 0 {
		t.Fatalf("Failed to send message: %v", errCode)
	}

	// Wait for message
	testing_utils.LogStep("Waiting for message (10 seconds)")
	time.Sleep(10 * time.Second)

	// Close
	testing_utils.LogStep("Closing connections")
	producer.Close()
	consumer.Close()

	if !received {
		t.Error("Message was not received")
	} else {
		testing_utils.LogSuccess("Message received successfully")
	}
}

// TestWorkerQueueOneToOne tests 1 producer 1 consumer using worker queue
func TestWorkerQueueOneToOne(t *testing.T) {
	testing_utils.InitLogger()
	testing_utils.LogTest("Testing Worker Queue One-to-One pattern")

	// Init connection
	config := &middleware.ConnectionConfig{
		URL: "amqp://admin:password@rabbitmq:5672/",
	}
	err := middleware.WaitForConnection(config, 10, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	testing_utils.LogStep("Connected to RabbitMQ")

	// Init middleware
	testing_utils.LogStep("Creating queue producer and consumer")
	producer := workerqueue.NewMessageMiddlewareQueue("test-queue-1to1", config)
	consumer := workerqueue.NewQueueConsumer("test-queue-1to1", config)

	if producer == nil || consumer == nil {
		t.Fatal("Failed to create middleware")
	}

	// Declare queue
	testing_utils.LogStep("Declaring queue")
	errCode := producer.DeclareQueue(true, false, false, false)
	if errCode != 0 {
		t.Fatalf("Failed to declare queue: %v", errCode)
	}

	// Send message
	testing_utils.LogStep("Sending message")
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
			testing_utils.LogStep("Consumer received message: %s", string(delivery.Body))
		}
		delivery.Ack(false)
		close(done)
	}

	testing_utils.LogStep("Starting consumer")
	errCode = consumer.StartConsuming(onMessageCallback)
	if errCode != 0 {
		t.Fatalf("Failed to start consuming: %v", errCode)
	}

	// Wait for message
	testing_utils.LogStep("Waiting for message (2 seconds)")
	time.Sleep(2 * time.Second)

	// Close
	testing_utils.LogStep("Closing connections")
	producer.Close()
	consumer.Close()

	if !received {
		t.Error("Message was not received")
	} else {
		testing_utils.LogSuccess("Message received successfully")
	}
}
