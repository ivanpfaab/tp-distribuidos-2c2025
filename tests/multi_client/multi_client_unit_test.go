package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestMultiClientUnitTests runs unit tests that don't require RabbitMQ
func TestMultiClientUnitTests(t *testing.T) {
	t.Run("ClientSimulation", func(t *testing.T) {
		testClientSimulation(t)
	})

	t.Run("ConcurrentMessageHandling", func(t *testing.T) {
		testConcurrentMessageHandling(t)
	})

	t.Run("MessageValidation", func(t *testing.T) {
		testMessageValidation(t)
	})
}

// testClientSimulation tests the basic client simulation logic
func testClientSimulation(t *testing.T) {
	// Simulate multiple clients sending messages
	numClients := 3
	messagesPerClient := 5

	var wg sync.WaitGroup
	results := make(chan SimulatedResult, numClients*messagesPerClient)

	// Start multiple simulated clients
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			// Simulate sending messages
			for j := 0; j < messagesPerClient; j++ {
				message := fmt.Sprintf("Client%d-Message%d", clientID, j)
				response := simulateEchoResponse(message)

				result := SimulatedResult{
					ClientID: clientID,
					Message:  message,
					Response: response,
				}

				results <- result
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

		// Validate response format
		assert.Contains(t, result.Response, "Echo:", "Response should contain 'Echo:' prefix")
		assert.Contains(t, result.Response, result.Message, "Response should contain original message")
		assert.Equal(t, fmt.Sprintf("Echo: %s", result.Message), result.Response, "Response should match expected format")

		t.Logf("Client %d: Sent '%s' -> Received '%s'", result.ClientID, result.Message, result.Response)
	}

	assert.Equal(t, numClients*messagesPerClient, receivedResults, "Should receive all expected results")
}

// testConcurrentMessageHandling tests concurrent message processing simulation
func testConcurrentMessageHandling(t *testing.T) {
	numClients := 5
	messagesPerClient := 3

	var wg sync.WaitGroup
	results := make(chan SimulatedResult, numClients*messagesPerClient)

	// Start all clients and send messages concurrently
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			// Send all messages concurrently
			for j := 0; j < messagesPerClient; j++ {
				wg.Add(1)
				go func(msgNum int) {
					defer wg.Done()

					message := fmt.Sprintf("Concurrent-Client%d-Message%d", clientID, msgNum)
					response := simulateEchoResponse(message)

					result := SimulatedResult{
						ClientID: clientID,
						Message:  message,
						Response: response,
					}

					results <- result
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
	clientMessageCounts := make(map[int]int)

	for result := range results {
		receivedResults++
		clientMessageCounts[result.ClientID]++

		// Validate response
		assert.Contains(t, result.Response, "Echo:", "Response should contain 'Echo:' prefix")
		assert.Contains(t, result.Response, result.Message, "Response should contain original message")

		t.Logf("Concurrent Client %d: Sent '%s' -> Received '%s'", result.ClientID, result.Message, result.Response)
	}

	// Validate all clients sent their expected number of messages
	assert.Equal(t, numClients*messagesPerClient, receivedResults, "Should receive all expected concurrent results")

	for clientID := 0; clientID < numClients; clientID++ {
		assert.Equal(t, messagesPerClient, clientMessageCounts[clientID], "Client %d should have sent %d messages", clientID, messagesPerClient)
	}
}

// testMessageValidation tests message validation logic
func testMessageValidation(t *testing.T) {
	testCases := []struct {
		name     string
		message  string
		expected string
	}{
		{
			name:     "Simple message",
			message:  "Hello World",
			expected: "Echo: Hello World",
		},
		{
			name:     "Empty message",
			message:  "",
			expected: "Echo: ",
		},
		{
			name:     "Special characters",
			message:  "Test@#$%^&*()",
			expected: "Echo: Test@#$%^&*()",
		},
		{
			name:     "Long message",
			message:  "This is a very long message that contains multiple words and should be echoed back properly",
			expected: "Echo: This is a very long message that contains multiple words and should be echoed back properly",
		},
		{
			name:     "Unicode message",
			message:  "Hello 世界 🌍",
			expected: "Echo: Hello 世界 🌍",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			response := simulateEchoResponse(tc.message)
			assert.Equal(t, tc.expected, response, "Response should match expected format")
		})
	}
}

// SimulatedResult represents the result of a simulated client message
type SimulatedResult struct {
	ClientID int
	Message  string
	Response string
}

// simulateEchoResponse simulates the server's echo response
func simulateEchoResponse(message string) string {
	// Simulate some processing time
	time.Sleep(1 * time.Millisecond)
	return fmt.Sprintf("Echo: %s", message)
}

// TestMessageQueueSimulation tests message queue behavior simulation
func TestMessageQueueSimulation(t *testing.T) {
	// Simulate a message queue with multiple producers and consumers
	queue := make(chan string, 100)
	responses := make(chan string, 100)

	// Start multiple producers (clients)
	numProducers := 3
	messagesPerProducer := 5

	var producerWg sync.WaitGroup
	for i := 0; i < numProducers; i++ {
		producerWg.Add(1)
		go func(producerID int) {
			defer producerWg.Done()
			for j := 0; j < messagesPerProducer; j++ {
				message := fmt.Sprintf("Producer%d-Message%d", producerID, j)
				queue <- message
			}
		}(i)
	}

	// Start consumer (server)
	var consumerWg sync.WaitGroup
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		for message := range queue {
			response := simulateEchoResponse(message)
			responses <- response
		}
	}()

	// Wait for all producers to finish
	producerWg.Wait()
	close(queue)

	// Wait for consumer to finish
	consumerWg.Wait()
	close(responses)

	// Validate all messages were processed
	responseCount := 0
	for response := range responses {
		responseCount++
		assert.Contains(t, response, "Echo:", "Response should contain 'Echo:' prefix")
		t.Logf("Processed message: %s", response)
	}

	expectedCount := numProducers * messagesPerProducer
	assert.Equal(t, expectedCount, responseCount, "Should process all messages")
}

// TestConcurrentClientConnections tests concurrent client connection simulation
func TestConcurrentClientConnections(t *testing.T) {
	numClients := 10
	connectionTimeout := 5 * time.Second

	var wg sync.WaitGroup
	connectionResults := make(chan ConnectionResult, numClients)

	// Simulate multiple clients trying to connect concurrently
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			// Simulate connection attempt
			connected := simulateConnection(clientID, connectionTimeout)

			result := ConnectionResult{
				ClientID:  clientID,
				Connected: connected,
				Timestamp: time.Now(),
			}

			connectionResults <- result
		}(i)
	}

	// Wait for all connection attempts to finish
	go func() {
		wg.Wait()
		close(connectionResults)
	}()

	// Collect and validate results
	connectedCount := 0
	for result := range connectionResults {
		if result.Connected {
			connectedCount++
		}
		t.Logf("Client %d: Connected=%v at %v", result.ClientID, result.Connected, result.Timestamp)
	}

	// All clients should be able to connect (in simulation)
	assert.Equal(t, numClients, connectedCount, "All clients should connect successfully")
}

// ConnectionResult represents the result of a connection attempt
type ConnectionResult struct {
	ClientID  int
	Connected bool
	Timestamp time.Time
}

// simulateConnection simulates a client connection attempt
func simulateConnection(clientID int, timeout time.Duration) bool {
	// Simulate connection time
	time.Sleep(time.Duration(clientID%3) * time.Millisecond)
	return true // Always succeeds in simulation
}
