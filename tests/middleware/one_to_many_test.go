package middleware

import (
	"fmt"
	"testing"
	"time"
	"tp-distribuidos-2c2025/shared/middleware"
	"tp-distribuidos-2c2025/shared/middleware/exchange"
	"tp-distribuidos-2c2025/shared/middleware/workerqueue"
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
	producerBroadcast := exchange.NewMessageMiddlewareExchange("fanout-exchange", []string{}, config)
	producer := exchange.NewMessageMiddlewareExchange("topic-exchange", []string{"key1"}, config)
	consumer1_key1 := exchange.NewExchangeConsumer("topic-exchange", []string{"key1"}, config)
	consumer1_key2 := exchange.NewExchangeConsumer("topic-exchange", []string{"key2"}, config)
	consumer1_key3 := exchange.NewExchangeConsumer("topic-exchange", []string{"key3"}, config)
	consumer2_key1 := exchange.NewExchangeConsumer("topic-exchange", []string{"key1"}, config)
	consumer2_key2 := exchange.NewExchangeConsumer("topic-exchange", []string{"key2"}, config)
	consumer2_key3 := exchange.NewExchangeConsumer("topic-exchange", []string{"key3"}, config)
	consumerbroadcast1 := exchange.NewExchangeConsumer("fanout-exchange", []string{"key"}, config)
	consumerbroadcast2 := exchange.NewExchangeConsumer("fanout-exchange", []string{"key"}, config)
	consumerbroadcast3 := exchange.NewExchangeConsumer("fanout-exchange", []string{"different_key"}, config)

	if producerBroadcast == nil || producer == nil || consumer1_key1 == nil || consumer1_key2 == nil || consumer1_key3 == nil || consumer2_key1 == nil || consumer2_key2 == nil || consumer2_key3 == nil || consumerbroadcast1 == nil || consumerbroadcast2 == nil || consumerbroadcast3 == nil {
		t.Fatal("Failed to create middleware")
	}

	// Declare exchange
	middleware.LogStep("Declaring exchanges")
	errCode := producerBroadcast.DeclareExchange("fanout", true, false, false, false)
	if errCode != 0 {
		t.Fatalf("Failed to declare exchange: %v", errCode)
	}

	errCode = producer.DeclareExchange("topic", true, false, false, false)
	if errCode != 0 {
		t.Fatalf("Failed to declare exchange: %v", errCode)
	}

	// Check if all consumers received the message
	received1 := false
	received2 := false
	received3 := false
	received4 := false
	received5 := false
	received6 := false
	received7 := false
	received8 := false
	received9 := false

	onMessageCallback_consumer1_key1 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer 1 Key 1 received message: %s", string(message))
			received1 = true
			delivery.Ack(false)
		}
	}

	onMessageCallback_consumer1_key2 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer 1 key 2 received message: %s", string(message))
			received2 = true
			delivery.Ack(false)
		}
	}

	onMessageCallback_consumer1_key3 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer 1 key 3 received message: %s", string(message))
			received3 = true
			delivery.Ack(false)
		}
	}

	onMessageCallback_consumer2_key1 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer 2 key 1 received message: %s", string(message))
			received4 = true
			delivery.Ack(false)
		}
	}

	onMessageCallback_consumer2_key2 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer 2 key 2 received message: %s", string(message))
			received5 = true
			delivery.Ack(false)
		}
	}

	onMessageCallback_consumer2_key3 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer 2 key 3 received message: %s", string(message))
			received6 = true
			delivery.Ack(false)
		}
	}

	onMessageCallback_consumerbroadcast1 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer broadcast 1 received message: %s", string(message))
			received7 = true
			delivery.Ack(false)
		}
	}

	onMessageCallback_consumerbroadcast2 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer broadcast 2 received message: %s", string(message))
			received8 = true
			delivery.Ack(false)
		}
	}

	onMessageCallback_consumerbroadcast3 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer broadcast 3 received message: %s", string(message))
			received9 = true
			delivery.Ack(false)
		}
	}

	// Start all consumers
	middleware.LogStep("Starting all consumers")
	consumer1_key1.StartConsuming(onMessageCallback_consumer1_key1)
	consumer1_key2.StartConsuming(onMessageCallback_consumer1_key2)
	consumer1_key3.StartConsuming(onMessageCallback_consumer1_key3)
	consumer2_key1.StartConsuming(onMessageCallback_consumer2_key1)
	consumer2_key2.StartConsuming(onMessageCallback_consumer2_key2)
	consumer2_key3.StartConsuming(onMessageCallback_consumer2_key3)
	consumerbroadcast1.StartConsuming(onMessageCallback_consumerbroadcast1)
	consumerbroadcast2.StartConsuming(onMessageCallback_consumerbroadcast2)
	consumerbroadcast3.StartConsuming(onMessageCallback_consumerbroadcast3)

	// Small delay to ensure consumers are ready
	time.Sleep(100 * time.Millisecond)

	// Send message
	middleware.LogStep("Sending 10 messages")
	for i := 0; i < 10; i++ {
		message := []byte(fmt.Sprintf("Hello from fanout exchange %d", i))
		errCode = producerBroadcast.Send(message, []string{})
		if errCode != 0 {
			t.Fatalf("Failed to send message: %v", errCode)
		}

		if i%3 == 0 {
			message := []byte(fmt.Sprintf("Hello from topic exchange %d", i))
			errCode = producer.Send(message, []string{"key1"})
			if errCode != 0 {
				t.Fatalf("Failed to send message: %v", errCode)
			}
		} else if i%3 == 1 {
			message := []byte(fmt.Sprintf("Hello from topic exchange %d", i))
			errCode = producer.Send(message, []string{"key2"})
			if errCode != 0 {
				t.Fatalf("Failed to send message: %v", errCode)
			}
		} else {
			message := []byte(fmt.Sprintf("Hello from topic exchange %d", i))
			errCode = producer.Send(message, []string{"key3"})
			if errCode != 0 {
				t.Fatalf("Failed to send message: %v", errCode)
			}
		}
	}

	// Wait for messages
	middleware.LogStep("Waiting for messages (10 seconds)")
	time.Sleep(20 * time.Second)

	// Close
	middleware.LogStep("Closing connections")
	producerBroadcast.Close()
	producer.Close()
	consumer1_key1.Close()
	consumer1_key2.Close()
	consumer1_key3.Close()
	consumer2_key1.Close()
	consumer2_key2.Close()
	consumer2_key3.Close()
	consumerbroadcast1.Close()
	consumerbroadcast2.Close()
	consumerbroadcast3.Close()
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
	if !received4 {
		t.Error("Consumer 4 did not receive message")
	}
	if !received5 {
		t.Error("Consumer 5 did not receive message")
	}
	if !received6 {
		t.Error("Consumer 6 did not receive message")
	}
	if !received7 {
		t.Error("Consumer 7 did not receive message")
	}
	if !received8 {
		t.Error("Consumer 8 did not receive message")
	}
	if !received9 {
		t.Error("Consumer 9 did not receive message")
	}
	if received1 && received2 && received3 && received4 && received5 && received6 && received7 && received8 && received9 {
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
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer 1 received message: %s", string(message))
			received1 = true
			delivery.Ack(false)
		}
	}

	onMessageCallback2 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer 2 received message: %s", string(message))
			received2 = true
			delivery.Ack(false)
		}
	}

	onMessageCallback3 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer 3 received message: %s", string(message))
			received3 = true
			delivery.Ack(false)
		}
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
		[]byte("Message 4 for worker queue 1tomany"),
		[]byte("Message 5 for worker queue 1tomany"),
		[]byte("Message 6 for worker queue 1tomany"),
		[]byte("Message 7 for worker queue 1tomany"),
		[]byte("Message 8 for worker queue 1tomany"),
		[]byte("Message 9 for worker queue 1tomany"),
		[]byte("Message 10 for worker queue 1tomany"),
	}

	for _, message := range messages {
		errCode = producer.Send(message)
		if errCode != 0 {
			t.Fatalf("Failed to send message: %v", errCode)
		}
	}

	// Wait for messages
	middleware.LogStep("Waiting for messages (3 seconds)")
	time.Sleep(20 * time.Second)

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
