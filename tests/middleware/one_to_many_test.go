package middleware

import (
	"fmt"
	"testing"
	"time"
	"tp-distribuidos-2c2025/shared/middleware"
	"tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"tp-distribuidos-2c2025/shared/middleware/exchange"
)

// TestExchangeOneToMany tests 1 producer multiple consumers using exchange
func TestExchangeOneToMany(t *testing.T) {
	middleware.InitLogger()
	middleware.LogTest("Testing Exchange One-to-Many pattern")
	
	// Init connection
	config := &middleware.ConnectionConfig{
		URL: "amqp://admin:password@rabbitmq:5672/",
	}
	err := middleware.WaitForConnection(config, 10, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	middleware.LogStep("Connected to RabbitMQ")

	// Init middleware
	middleware.LogStep("Creating exchange producer and consumers")
	producer := exchange.NewMessageMiddlewareExchange("test-exchange-1tomany", []string{"test.broadcast1", "test.broadcast2", "test.broadcast3"}, config)
	consumer1 := exchange.NewExchangeConsumer("test-exchange-1tomany", []string{"test.broadcast1"}, config)
	consumer2 := exchange.NewExchangeConsumer("test-exchange-1tomany", []string{"test.broadcast2"}, config)
	consumer3 := exchange.NewExchangeConsumer("test-exchange-1tomany", []string{"test.broadcast3"}, config)
	
	if producer == nil || consumer1 == nil || consumer2 == nil || consumer3 == nil {
		t.Fatal("Failed to create middleware")
	}

	// Declare exchange
	middleware.LogStep("Declaring exchange")
	errCode := producer.DeclareExchange("topic", true, false, false, false)
	if errCode != 0 {
		t.Fatalf("Failed to declare exchange: %v", errCode)
	}

	// Check if all consumers received the message
	received1 := false
	received2 := false
	received3 := false

	onMessageCallback1 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		delivery := <-*consumeChannel
		message := delivery.Body
		middleware.LogStep("Consumer 1 received message: %s", string(message))
		received1 = true
		delivery.Ack(false)
		close(done)
	}

	onMessageCallback2 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		delivery := <-*consumeChannel
		message := delivery.Body
		middleware.LogStep("Consumer 2 received message: %s", string(message))
		received2 = true
		delivery.Ack(false)
		close(done)
	}

	onMessageCallback3 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		delivery := <-*consumeChannel
		message := delivery.Body
		middleware.LogStep("Consumer 3 received message: %s", string(message))
		received3 = true
		delivery.Ack(false)
		close(done)
	}

	// Start all consumers
	middleware.LogStep("Starting all consumers")
	consumer1.StartConsuming(onMessageCallback1)
	consumer2.StartConsuming(onMessageCallback2)
	consumer3.StartConsuming(onMessageCallback3)

	// Small delay to ensure consumers are ready
	time.Sleep(100 * time.Millisecond)

	// Send message
	middleware.LogStep("Sending 10 messages")
	for i := 0; i < 10; i++ {
		message := []byte(fmt.Sprintf("Hello from exchange 1tomany %d", i))
		errCode = producer.Send(message)
		if errCode != 0 {
			t.Fatalf("Failed to send message: %v", errCode)
		}
	}

	// Wait for messages
	middleware.LogStep("Waiting for messages (10 seconds)")
	time.Sleep(10 * time.Second)

	// Close
	middleware.LogStep("Closing connections")
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
	
	if received1 && received2 && received3 {
		middleware.LogSuccess("All consumers received messages successfully")
	}
}

// TestWorkerQueueOneToMany tests 1 producer multiple consumers using worker queue
func TestWorkerQueueOneToMany(t *testing.T) {
	middleware.InitLogger()
	middleware.LogTest("Testing Worker Queue One-to-Many pattern")
	
	// Init connection
	config := &middleware.ConnectionConfig{
		URL: "amqp://admin:password@rabbitmq:5672/",
	}
	err := middleware.WaitForConnection(config, 10, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	middleware.LogStep("Connected to RabbitMQ")

	// Init middleware
	middleware.LogStep("Creating queue producer and consumers")
	producer := workerqueue.NewMessageMiddlewareQueue("test-queue-1tomany", config)
	consumer1 := workerqueue.NewQueueConsumer("test-queue-1tomany", config)
	consumer2 := workerqueue.NewQueueConsumer("test-queue-1tomany", config)
	consumer3 := workerqueue.NewQueueConsumer("test-queue-1tomany", config)
	
	if producer == nil || consumer1 == nil || consumer2 == nil || consumer3 == nil {
		t.Fatal("Failed to create middleware")
	}

	// Declare queue
	middleware.LogStep("Declaring queue")
	errCode := producer.DeclareQueue(true, false, false, false)
	if errCode != 0 {
		t.Fatalf("Failed to declare queue: %v", errCode)
	}

	// Check if consumers received messages
	received1 := false
	received2 := false
	received3 := false

	onMessageCallback1 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		delivery := <-*consumeChannel
		message := delivery.Body
		middleware.LogStep("Consumer 1 received message: %s", string(message))
		received1 = true
		delivery.Ack(false)
		close(done)
	}

	onMessageCallback2 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		delivery := <-*consumeChannel
		message := delivery.Body
		middleware.LogStep("Consumer 2 received message: %s", string(message))
		received2 = true
		delivery.Ack(false)
		close(done)
	}

	onMessageCallback3 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		delivery := <-*consumeChannel
		message := delivery.Body
		middleware.LogStep("Consumer 3 received message: %s", string(message))
		received3 = true
		delivery.Ack(false)
		close(done)
	}

	// Start all consumers
	middleware.LogStep("Starting all consumers")
	consumer1.StartConsuming(onMessageCallback1)
	consumer2.StartConsuming(onMessageCallback2)
	consumer3.StartConsuming(onMessageCallback3)


	// Send 3 messages (one for each consumer)
	middleware.LogStep("Sending 3 messages")
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

	// Wait for messages
	middleware.LogStep("Waiting for messages (3 seconds)")
	time.Sleep(3 * time.Second)

	// Close
	middleware.LogStep("Closing connections")
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
	
	if received1 && received2 && received3 {
		middleware.LogSuccess("All consumers received messages successfully")
	}
}
