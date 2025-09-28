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
		t.Fatalf("Failed to declare fanout exchange: %v", errCode)
	}

	errCode = producer.DeclareExchange("topic", true, false, false, false)
	if errCode != 0 {
		t.Fatalf("Failed to declare topic exchange: %v", errCode)
	}

	// Message counters for validation
	var (
		// Topic exchange counters
		consumer1_key1_count = 0
		consumer1_key2_count = 0
		consumer1_key3_count = 0
		consumer2_key1_count = 0
		consumer2_key2_count = 0
		consumer2_key3_count = 0

		// Fanout exchange counters
		consumerbroadcast1_count = 0
		consumerbroadcast2_count = 0
		consumerbroadcast3_count = 0
	)

	// Expected message counts
	expected_key1_total := 4    // 4 messages for key1 (i%3 == 0: 0,3,6,9)
	expected_key2_total := 3    // 3 messages for key2 (i%3 == 1: 1,4,7)
	expected_key3_total := 3    // 3 messages for key3 (i%3 == 2: 2,5,8)
	expected_fanout_total := 10 // 10 fanout messages

	onMessageCallback_consumer1_key1 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer 1 Key 1 received message: %s", string(message))
			consumer1_key1_count++
			delivery.Ack(false)
		}
	}

	onMessageCallback_consumer1_key2 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer 1 Key 2 received message: %s", string(message))
			consumer1_key2_count++
			delivery.Ack(false)
		}
	}

	onMessageCallback_consumer1_key3 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer 1 Key 3 received message: %s", string(message))
			consumer1_key3_count++
			delivery.Ack(false)
		}
	}

	onMessageCallback_consumer2_key1 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer 2 Key 1 received message: %s", string(message))
			consumer2_key1_count++
			delivery.Ack(false)
		}
	}

	onMessageCallback_consumer2_key2 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer 2 Key 2 received message: %s", string(message))
			consumer2_key2_count++
			delivery.Ack(false)
		}
	}

	onMessageCallback_consumer2_key3 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer 2 Key 3 received message: %s", string(message))
			consumer2_key3_count++
			delivery.Ack(false)
		}
	}

	onMessageCallback_consumerbroadcast1 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer broadcast 1 received message: %s", string(message))
			consumerbroadcast1_count++
			delivery.Ack(false)
		}
	}

	onMessageCallback_consumerbroadcast2 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer broadcast 2 received message: %s", string(message))
			consumerbroadcast2_count++
			delivery.Ack(false)
		}
	}

	onMessageCallback_consumerbroadcast3 := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			message := delivery.Body
			middleware.LogStep("Consumer broadcast 3 received message: %s", string(message))
			consumerbroadcast3_count++
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

	// Wait for messages to be processed
	middleware.LogStep("Waiting for messages to be processed (5 seconds)")
	time.Sleep(5 * time.Second)

	// Close connections
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

	// Validate results
	middleware.LogStep("Validating message distribution")

	// Calculate totals for each key
	key1_total := consumer1_key1_count + consumer2_key1_count
	key2_total := consumer1_key2_count + consumer2_key2_count
	key3_total := consumer1_key3_count + consumer2_key3_count

	// Calculate fanout totals
	fanout_key_total := consumerbroadcast1_count + consumerbroadcast2_count
	fanout_different_key_total := consumerbroadcast3_count

	// Test topic exchange load balancing
	middleware.LogStep("Testing topic exchange load balancing")
	middleware.LogStep("Key1: Consumer1=%d, Consumer2=%d, Total=%d (expected=%d)",
		consumer1_key1_count, consumer2_key1_count, key1_total, expected_key1_total)
	middleware.LogStep("Key2: Consumer1=%d, Consumer2=%d, Total=%d (expected=%d)",
		consumer1_key2_count, consumer2_key2_count, key2_total, expected_key2_total)
	middleware.LogStep("Key3: Consumer1=%d, Consumer2=%d, Total=%d (expected=%d)",
		consumer1_key3_count, consumer2_key3_count, key3_total, expected_key3_total)

	// Validate topic exchange results
	if key1_total != expected_key1_total {
		t.Errorf("Key1 total messages: got %d, expected %d", key1_total, expected_key1_total)
	}
	if key2_total != expected_key2_total {
		t.Errorf("Key2 total messages: got %d, expected %d", key2_total, expected_key2_total)
	}
	if key3_total != expected_key3_total {
		t.Errorf("Key3 total messages: got %d, expected %d", key3_total, expected_key3_total)
	}

	// Test fanout exchange behavior
	middleware.LogStep("Testing fanout exchange behavior")
	middleware.LogStep("Fanout 'key' queue: Consumer1=%d, Consumer2=%d, Total=%d (expected=%d)",
		consumerbroadcast1_count, consumerbroadcast2_count, fanout_key_total, expected_fanout_total)
	middleware.LogStep("Fanout 'different_key' queue: Consumer3=%d (expected=%d)",
		fanout_different_key_total, expected_fanout_total)

	// Validate fanout exchange results
	if fanout_key_total != expected_fanout_total {
		t.Errorf("Fanout 'key' queue total: got %d, expected %d", fanout_key_total, expected_fanout_total)
	}
	if fanout_different_key_total != expected_fanout_total {
		t.Errorf("Fanout 'different_key' queue total: got %d, expected %d", fanout_different_key_total, expected_fanout_total)
	}

	// Test load balancing within each key (consumers should compete for messages)
	middleware.LogStep("Testing load balancing within keys")
	if consumer1_key1_count == 0 && consumer2_key1_count == 0 {
		t.Error("No consumers received key1 messages")
	}
	if consumer1_key2_count == 0 && consumer2_key2_count == 0 {
		t.Error("No consumers received key2 messages")
	}
	if consumer1_key3_count == 0 && consumer2_key3_count == 0 {
		t.Error("No consumers received key3 messages")
	}

	// Test fanout load balancing
	if consumerbroadcast1_count == 0 && consumerbroadcast2_count == 0 {
		t.Error("No consumers received fanout 'key' messages")
	}

	// Success message
	if key1_total == expected_key1_total && key2_total == expected_key2_total && key3_total == expected_key3_total &&
		fanout_key_total == expected_fanout_total && fanout_different_key_total == expected_fanout_total {
		middleware.LogSuccess("All exchange middleware functionality validated successfully!")
		middleware.LogSuccess("Topic exchange routing works correctly")
		middleware.LogSuccess("Fanout exchange broadcasting works correctly")
		middleware.LogSuccess("Load balancing between consumers works correctly")
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
