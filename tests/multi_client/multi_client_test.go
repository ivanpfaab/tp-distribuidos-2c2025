package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"tp-distribuidos-2c2025/shared/rabbitmq"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMultiClientConnection tests multiple clients connecting to the server via RabbitMQ
func TestMultiClientConnection(t *testing.T) {
	// Configuration for test
	numClients := 3
	messagesPerClient := 5
	timeout := 30 * time.Second

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Start server in a goroutine
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- runTestServer(ctx)
	}()

	// Wait a bit for server to start
	time.Sleep(2 * time.Second)

	// Test multiple clients
	t.Run("MultipleClients", func(t *testing.T) {
		testMultipleClients(t, numClients, messagesPerClient, ctx)
	})

	// Test concurrent message handling
	t.Run("ConcurrentMessages", func(t *testing.T) {
		testConcurrentMessages(t, numClients, messagesPerClient, ctx)
	})

	// Test client disconnection and reconnection
	t.Run("ClientReconnection", func(t *testing.T) {
		testClientReconnection(t, ctx)
	})

	// Cancel context to stop server
	cancel()

	// Wait for server to finish
	select {
	case err := <-serverDone:
		if err != nil && err != context.Canceled {
			t.Logf("Server finished with error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Log("Server did not finish within timeout")
	}
}

// testMultipleClients tests that multiple clients can connect and send messages
func testMultipleClients(t *testing.T, numClients, messagesPerClient int, ctx context.Context) {
	var wg sync.WaitGroup
	results := make(chan ClientResult, numClients*messagesPerClient)

	// Start multiple clients concurrently
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			client := NewTestClient(clientID)
			defer client.Close()

			// Send messages
			for j := 0; j < messagesPerClient; j++ {
				message := fmt.Sprintf("Client%d-Message%d", clientID, j)
				response, err := client.SendMessage(message)

				result := ClientResult{
					ClientID: clientID,
					Message:  message,
					Response: response,
					Error:    err,
				}

				select {
				case results <- result:
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	// Wait for all clients to finish
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect and validate results
	receivedResults := 0
	for result := range results {
		receivedResults++

		// Validate that we got a response
		assert.NoError(t, result.Error, "Client %d should not have errors", result.ClientID)
		assert.NotEmpty(t, result.Response, "Client %d should receive a response", result.ClientID)
		assert.Contains(t, result.Response, "Echo:", "Response should contain 'Echo:' prefix")
		assert.Contains(t, result.Response, result.Message, "Response should contain original message")

		t.Logf("Client %d: Sent '%s' -> Received '%s'", result.ClientID, result.Message, result.Response)
	}

	assert.Equal(t, numClients*messagesPerClient, receivedResults, "Should receive all expected results")
}

// testConcurrentMessages tests that the server can handle concurrent messages from multiple clients
func testConcurrentMessages(t *testing.T, numClients, messagesPerClient int, ctx context.Context) {
	var wg sync.WaitGroup
	results := make(chan ClientResult, numClients*messagesPerClient)

	// Start all clients and send messages concurrently
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			client := NewTestClient(clientID)
			defer client.Close()

			// Send all messages at once (concurrently)
			for j := 0; j < messagesPerClient; j++ {
				wg.Add(1)
				// We start a new goroutine for each message to simulate concurrent processing
				go func(msgNum int) {
					defer wg.Done()
					message := fmt.Sprintf("Concurrent-Client%d-Message%d", clientID, msgNum)
					response, err := client.SendMessage(message)

					result := ClientResult{
						ClientID: clientID,
						Message:  message,
						Response: response,
						Error:    err,
					}

					select {
					case results <- result:
					case <-ctx.Done():
						return
					}
				}(j)
			}
		}(i)
	}

	// Wait for all clients to finish
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect and validate results
	receivedResults := 0
	for result := range results {
		receivedResults++

		// Validate that we got a response
		assert.NoError(t, result.Error, "Client %d should not have errors", result.ClientID)
		assert.NotEmpty(t, result.Response, "Client %d should receive a response", result.ClientID)
		assert.Contains(t, result.Response, "Echo:", "Response should contain 'Echo:' prefix")

		t.Logf("Concurrent Client %d: Sent '%s' -> Received '%s'", result.ClientID, result.Message, result.Response)
	}

	assert.Equal(t, numClients*messagesPerClient, receivedResults, "Should receive all expected concurrent results")
}

// testClientReconnection tests client disconnection and reconnection
func testClientReconnection(t *testing.T, ctx context.Context) {
	// Create first client
	client1 := NewTestClient(1)

	// Send a message
	response1, err := client1.SendMessage("First message")
	require.NoError(t, err)
	assert.Contains(t, response1, "Echo: First message")

	// Close client
	client1.Close()

	// Wait a bit
	time.Sleep(1 * time.Second)

	// Create new client (simulating reconnection)
	client2 := NewTestClient(2)
	defer client2.Close()

	// Send message with new client
	response2, err := client2.SendMessage("Reconnected message")
	require.NoError(t, err)
	assert.Contains(t, response2, "Echo: Reconnected message")

	t.Logf("Reconnection test: First client sent 'First message' -> '%s'", response1)
	t.Logf("Reconnection test: Second client sent 'Reconnected message' -> '%s'", response2)
}

// ClientResult represents the result of a client message
type ClientResult struct {
	ClientID int
	Message  string
	Response string
	Error    error
}

// TestClient simulates a client for testing
type TestClient struct {
	ID            int
	conn          *rabbitmq.Connection
	requestQueue  string
	responseQueue string
	responseChan  <-chan amqp.Delivery
}

// NewTestClient creates a new test client
func NewTestClient(id int) *TestClient {
	config := rabbitmq.DefaultConfig()
	conn, err := rabbitmq.NewConnection(config)
	if err != nil {
		log.Fatalf("Failed to create test client %d: %v", id, err)
	}

	// Declare queues
	requestQueue, err := conn.DeclareQueue("echo_requests")
	if err != nil {
		conn.Close()
		log.Fatalf("Failed to declare request queue for client %d: %v", id, err)
	}

	responseQueue, err := conn.DeclareQueue("echo_responses")
	if err != nil {
		conn.Close()
		log.Fatalf("Failed to declare response queue for client %d: %v", id, err)
	}

	// Set up consumer for responses
	msgs, err := conn.ConsumeMessages(responseQueue.Name)
	if err != nil {
		conn.Close()
		log.Fatalf("Failed to register consumer for client %d: %v", id, err)
	}

	return &TestClient{
		ID:            id,
		conn:          conn,
		requestQueue:  requestQueue.Name,
		responseQueue: responseQueue.Name,
		responseChan:  msgs,
	}
}

// SendMessage sends a message and waits for response
func (c *TestClient) SendMessage(message string) (string, error) {
	// Send message
	err := c.conn.PublishMessage(c.requestQueue, message, c.responseQueue)
	if err != nil {
		return "", fmt.Errorf("failed to publish message: %v", err)
	}

	// Wait for response with timeout
	timeout := time.After(5 * time.Second)
	for {
		select {
		case d := <-c.responseChan:
			response := string(d.Body)
			// Check if this response is for our message
			if len(response) > 5 && response[5:] == message {
				return response, nil
			}
		case <-timeout:
			return "", fmt.Errorf("timeout waiting for response to message: %s", message)
		}
	}
}

// Close closes the test client
func (c *TestClient) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

// runTestServer runs the server for testing
func runTestServer(ctx context.Context) error {
	// Connect to RabbitMQ
	config := rabbitmq.DefaultConfig()
	conn, err := rabbitmq.NewConnection(config)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Declare queues
	requestQueue, err := conn.DeclareQueue("echo_requests")
	if err != nil {
		return fmt.Errorf("failed to declare request queue: %v", err)
	}

	responseQueue, err := conn.DeclareQueue("echo_responses")
	if err != nil {
		return fmt.Errorf("failed to declare response queue: %v", err)
	}

	// Set up consumer for requests
	msgs, err := conn.ConsumeMessages(requestQueue.Name)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %v", err)
	}

	log.Printf("Test server listening for messages on queue: %s", requestQueue.Name)

	// Process messages
	for {
		select {
		case <-ctx.Done():
			log.Println("Test server shutting down")
			return ctx.Err()
		case d := <-msgs:
			message := string(d.Body)
			log.Printf("Test server received: %s", message)

			// Echo the message back
			response := fmt.Sprintf("Echo: %s", message)

			// Get the reply-to queue from the message properties
			replyTo := d.ReplyTo
			if replyTo == "" {
				replyTo = responseQueue.Name
			}

			// Publish response
			err = conn.PublishMessage(replyTo, response, "")
			if err != nil {
				log.Printf("Failed to publish response: %v", err)
			} else {
				log.Printf("Test server sent response: %s", response)
			}
		}
	}
}
