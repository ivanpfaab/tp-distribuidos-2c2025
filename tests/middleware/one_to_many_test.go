package middleware

import (
	"testing"
	"time"
	"tp-distribuidos-2c2025/shared/middleware"
	"tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"tp-distribuidos-2c2025/shared/middleware/exchange"
)

// TestExchangeOneToMany tests 1 producer multiple consumers using exchange
func TestExchangeOneToMany(t *testing.T) {
	// Init connection
	config := &middleware.ConnectionConfig{
		URL: "amqp://admin:password@rabbitmq:5672/",
	}
	err := middleware.WaitForConnection(config, 10, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	// Init middleware
	producer := exchange.NewMessageMiddlewareExchange("test-exchange-1tomany", []string{"test.broadcast"}, config)
	consumer1 := exchange.NewExchangeConsumer("test-exchange-1tomany", []string{"test.broadcast"}, config)
	consumer2 := exchange.NewExchangeConsumer("test-exchange-1tomany", []string{"test.broadcast"}, config)
	consumer3 := exchange.NewExchangeConsumer("test-exchange-1tomany", []string{"test.broadcast"}, config)
	
	if producer == nil || consumer1 == nil || consumer2 == nil || consumer3 == nil {
		t.Fatal("Failed to create middleware")
	}

	// Declare exchange
	errCode := producer.DeclareExchange("topic", true, false, false, false)
	if errCode != 0 {
		t.Fatalf("Failed to declare exchange: %v", errCode)
	}

	// Send message
	message := []byte("Hello from exchange 1tomany")
	errCode = producer.Send(message)
	if errCode != 0 {
		t.Fatalf("Failed to send message: %v", errCode)
	}

	// Check if all consumers received the message
	received1 := false
	received2 := false
	received3 := false

	onMessageCallback1 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		delivery := <-*consumeChannel
		if string(delivery.Body) == string(message) {
			received1 = true
		}
		delivery.Ack(false)
		close(done)
	}

	onMessageCallback2 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		delivery := <-*consumeChannel
		if string(delivery.Body) == string(message) {
			received2 = true
		}
		delivery.Ack(false)
		close(done)
	}

	onMessageCallback3 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		delivery := <-*consumeChannel
		if string(delivery.Body) == string(message) {
			received3 = true
		}
		delivery.Ack(false)
		close(done)
	}

	// Start all consumers
	consumer1.StartConsuming(onMessageCallback1)
	consumer2.StartConsuming(onMessageCallback2)
	consumer3.StartConsuming(onMessageCallback3)

	// Wait for messages
	time.Sleep(10 * time.Second)

	// Close
	producer.Close()
	consumer1.Close()
	consumer2.Close()
	consumer3.Close()

	// Check results
	if !received1 {
		t.Error("Consumer 1 did not receive message")
	}
	if !received2 {
		t.Error("Consumer 2 did not receive message")
	}
	if !received3 {
		t.Error("Consumer 3 did not receive message")
	}
}

// TestWorkerQueueOneToMany tests 1 producer multiple consumers using worker queue
func TestWorkerQueueOneToMany(t *testing.T) {
	// Init connection
	config := &middleware.ConnectionConfig{
		URL: "amqp://admin:password@rabbitmq:5672/",
	}
	err := middleware.WaitForConnection(config, 10, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	// Init middleware
	producer := workerqueue.NewMessageMiddlewareQueue("test-queue-1tomany", config)
	consumer1 := workerqueue.NewQueueConsumer("test-queue-1tomany", config)
	consumer2 := workerqueue.NewQueueConsumer("test-queue-1tomany", config)
	consumer3 := workerqueue.NewQueueConsumer("test-queue-1tomany", config)
	
	if producer == nil || consumer1 == nil || consumer2 == nil || consumer3 == nil {
		t.Fatal("Failed to create middleware")
	}

	// Declare queue
	errCode := producer.DeclareQueue(true, false, false, false)
	if errCode != 0 {
		t.Fatalf("Failed to declare queue: %v", errCode)
	}

	// Send 3 messages (one for each consumer)
	messages := [][]byte{
		[]byte("Message 1 for worker queue 1tomany"),
		[]byte("Message 2 for worker queue 1tomany"),
		[]byte("Message 3 for worker queue 1tomany"),
	}

	for _, message := range messages {
		errCode = producer.Send(message)
		if errCode != 0 {
			t.Fatalf("Failed to send message: %v", errCode)
		}
	}

	// Check if consumers received messages
	received1 := false
	received2 := false
	received3 := false

	onMessageCallback1 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		delivery := <-*consumeChannel
		received1 = true
		delivery.Ack(false)
		close(done)
	}

	onMessageCallback2 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		delivery := <-*consumeChannel
		received2 = true
		delivery.Ack(false)
		close(done)
	}

	onMessageCallback3 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		delivery := <-*consumeChannel
		received3 = true
		delivery.Ack(false)
		close(done)
	}

	// Start all consumers
	consumer1.StartConsuming(onMessageCallback1)
	consumer2.StartConsuming(onMessageCallback2)
	consumer3.StartConsuming(onMessageCallback3)

	// Wait for messages
	time.Sleep(3 * time.Second)

	// Close
	producer.Close()
	consumer1.Close()
	consumer2.Close()
	consumer3.Close()

	// Check results
	if !received1 {
		t.Error("Consumer 1 did not receive message")
	}
	if !received2 {
		t.Error("Consumer 2 did not receive message")
	}
	if !received3 {
		t.Error("Consumer 3 did not receive message")
	}
}
