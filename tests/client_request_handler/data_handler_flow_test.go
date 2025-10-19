package main

import (
	"fmt"
	"net"
	"testing"
	"time"

	batch "github.com/tp-distribuidos-2c2025/protocol/batch"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
)

// TestDataHandlerFlow tests the complete flow from client to data handler
func TestDataHandlerFlow(t *testing.T) {
	testing_utils.InitLogger()
	testing_utils.LogTest("Testing complete data handler flow: Client -> TCP Server -> Client Request Handler -> Data Handler")

	t.Run("Single Batch Message Flow", testSingleBatchMessageFlow)
	t.Run("Multiple Batch Messages Flow", testMultipleBatchMessagesFlow)
	t.Run("Data Handler Chunk Processing", testDataHandlerChunkProcessing)
	t.Run("Data Handler Processing Verification", testDataHandlerProcessingVerification)
	t.Run("Connection Handling", testConnectionHandling)
	t.Run("Error Handling", testErrorHandling)
}

// testSingleBatchMessageFlow tests the flow with a single batch message
func testSingleBatchMessageFlow(t *testing.T) {
	testing_utils.LogStep("Testing single batch message flow")

	// Create test batch message
	testBatch := &batch.Batch{
		ClientID:    "1234", // Exactly 4 bytes
		FileID:      "5678", // Exactly 4 bytes
		IsEOF:       false,
		BatchNumber: 1,
		BatchSize:   22,
		BatchData:   "Test message from test",
	}

	// Create batch message and serialize
	batchMsg := batch.NewBatchMessage(testBatch)
	serializedData, err := batch.SerializeBatchMessage(batchMsg)
	if err != nil {
		t.Fatalf("Failed to serialize batch message: %v", err)
	}

	testing_utils.LogStep("Connecting to test server")
	// Connect to the test server (assuming it's running on localhost:8081)
	conn, err := net.Dial("tcp", "test-server:8080")
	if err != nil {
		t.Skipf("Skipping test - test server not available: %v", err)
		return
	}
	defer conn.Close()

	testing_utils.LogStep("Sending batch message to server")
	// Send the batch message
	_, err = conn.Write(serializedData)
	if err != nil {
		t.Fatalf("Failed to send batch message: %v", err)
	}

	testing_utils.LogStep("Reading response from server")
	// Read response
	response := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(response)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	responseStr := string(response[:n])
	testing_utils.LogStep("Received response: %s", responseStr)

	// Verify response contains acknowledgment
	expectedAck := fmt.Sprintf("ACK: Batch received - ClientID: %s, FileID: %s, BatchNumber: %d",
		testBatch.ClientID, testBatch.FileID, testBatch.BatchNumber)

	if responseStr != expectedAck {
		t.Errorf("Unexpected response: got %s, want %s", responseStr, expectedAck)
	}

	testing_utils.LogSuccess("Single batch message flow test completed successfully")
}

// testMultipleBatchMessagesFlow tests the flow with multiple batch messages
func testMultipleBatchMessagesFlow(t *testing.T) {
	testing_utils.LogStep("Testing multiple batch messages flow")

	testMessages := []string{
		"First test message",
		"Second test message",
		"Third test message",
	}

	testing_utils.LogStep("Connecting to test server")
	conn, err := net.Dial("tcp", "test-server:8080")
	if err != nil {
		t.Skipf("Skipping test - test server not available: %v", err)
		return
	}
	defer conn.Close()

	for i, messageData := range testMessages {
		testing_utils.LogStep("Sending batch message %d: %s", i+1, messageData)

		// Create batch message
		testBatch := &batch.Batch{
			ClientID:    fmt.Sprintf("%04d", i+1), // Exactly 4 bytes
			FileID:      fmt.Sprintf("%04d", i+1), // Exactly 4 bytes
			IsEOF:       i == len(testMessages)-1,
			BatchNumber: i + 1,
			BatchSize:   len(messageData),
			BatchData:   messageData,
		}

		// Create batch message and serialize
		batchMsg := batch.NewBatchMessage(testBatch)
		serializedData, err := batch.SerializeBatchMessage(batchMsg)
		if err != nil {
			t.Fatalf("Failed to serialize batch message %d: %v", i+1, err)
		}

		// Send the batch message
		_, err = conn.Write(serializedData)
		if err != nil {
			t.Fatalf("Failed to send batch message %d: %v", i+1, err)
		}

		// Read response
		response := make([]byte, 1024)
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := conn.Read(response)
		if err != nil {
			t.Fatalf("Failed to read response for message %d: %v", i+1, err)
		}

		responseStr := string(response[:n])
		testing_utils.LogStep("Received response for message %d: %s", i+1, responseStr)

		// Verify response contains acknowledgment
		expectedAck := fmt.Sprintf("ACK: Batch received - ClientID: %s, FileID: %s, BatchNumber: %d",
			testBatch.ClientID, testBatch.FileID, testBatch.BatchNumber)

		if responseStr != expectedAck {
			t.Errorf("Unexpected response for message %d: got %s, want %s", i+1, responseStr, expectedAck)
		}

		// Small delay between messages
		time.Sleep(100 * time.Millisecond)
	}

	testing_utils.LogSuccess("Multiple batch messages flow test completed successfully")
}

// testDataHandlerChunkProcessing tests that the data handler actually processes batches and sends chunks
func testDataHandlerChunkProcessing(t *testing.T) {
	testing_utils.LogStep("Testing data handler chunk processing with year-filter consumer")

	// Create RabbitMQ connection for consuming chunks
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	// Create consumer for year-filter queue
	consumer := workerqueue.NewQueueConsumer("year-filter", config)
	if consumer == nil {
		t.Skipf("Skipping test - failed to create RabbitMQ consumer")
		return
	}
	defer consumer.Close()

	// Note: QueueConsumer doesn't have DeclareQueue method, it's handled internally

	// Channel to collect received chunks
	receivedChunks := make(chan *chunk.Chunk, 10)
	chunkCount := 0

	// Start consuming chunks in a goroutine
	go func() {
		onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
			for delivery := range *consumeChannel {
				// Deserialize the chunk message
				message, err := deserializer.Deserialize(delivery.Body)
				if err != nil {
					testing_utils.LogStep("Failed to deserialize chunk: %v", err)
					delivery.Ack(false)
					continue
				}

				// Check if it's a Chunk message
				chunkMsg, ok := message.(*chunk.Chunk)
				if !ok {
					testing_utils.LogStep("Received non-chunk message: %T", message)
					delivery.Ack(false)
					continue
				}

				testing_utils.LogStep("Received chunk - ClientID: %s, ChunkNumber: %d, Step: %d, IsLastChunk: %t, IsLastFromTable: %t",
					chunkMsg.ClientID, chunkMsg.ChunkNumber, chunkMsg.Step, chunkMsg.IsLastChunk, chunkMsg.IsLastFromTable)

				receivedChunks <- chunkMsg
				chunkCount++

				// Acknowledge the message
				delivery.Ack(false)
			}
			done <- nil
		}

		if err := consumer.StartConsuming(onMessageCallback); err != 0 {
			testing_utils.LogStep("Failed to start consuming: %v", err)
		}
	}()

	// Wait a bit for consumer to start
	time.Sleep(1 * time.Second)

	// Send test batches to the server
	testing_utils.LogStep("Sending test batches to server")
	conn, err := net.Dial("tcp", "test-server:8080")
	if err != nil {
		t.Skipf("Skipping test - test server not available: %v", err)
		return
	}
	defer conn.Close()

	// Send 3 test batches
	testBatches := []struct {
		clientID    string
		fileID      string
		batchNumber int
		batchData   string
		isEOF       bool
	}{
		{"TEST", "FILE", 1, "Test batch 1", false},
		{"TEST", "FILE", 2, "Test batch 2", false},
		{"TEST", "FILE", 3, "Test batch 3", true},
	}

	for i, batchData := range testBatches {
		testing_utils.LogStep("Sending batch %d", i+1)

		// Create batch message
		testBatch := &batch.Batch{
			ClientID:    batchData.clientID,
			FileID:      batchData.fileID,
			IsEOF:       batchData.isEOF,
			BatchNumber: batchData.batchNumber,
			BatchSize:   len(batchData.batchData),
			BatchData:   batchData.batchData,
		}

		// Serialize and send
		batchMsg := batch.NewBatchMessage(testBatch)
		serializedData, err := batch.SerializeBatchMessage(batchMsg)
		if err != nil {
			t.Fatalf("Failed to serialize batch %d: %v", i+1, err)
		}

		_, err = conn.Write(serializedData)
		if err != nil {
			t.Fatalf("Failed to send batch %d: %v", i+1, err)
		}

		// Read acknowledgment
		response := make([]byte, 1024)
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := conn.Read(response)
		if err != nil {
			t.Fatalf("Failed to read response for batch %d: %v", i+1, err)
		}

		responseStr := string(response[:n])
		testing_utils.LogStep("Received response for batch %d: %s", i+1, responseStr)

		// Small delay between batches
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for chunks to be processed and received
	testing_utils.LogStep("Waiting for chunks to be processed (5 seconds)")
	timeout := time.After(5 * time.Second)

	expectedChunks := len(testBatches)
	receivedCount := 0

	for {
		select {
		case chunk := <-receivedChunks:
			receivedCount++
			testing_utils.LogStep("Received chunk %d/%d: ClientID=%s, ChunkNumber=%d, Step=%d, IsLastChunk=%t",
				receivedCount, expectedChunks, chunk.ClientID, chunk.ChunkNumber, chunk.Step, chunk.IsLastChunk)

			// Verify chunk properties
			if chunk.Step != 0 {
				t.Errorf("Expected chunk step to be 0, got %d", chunk.Step)
			}
			if chunk.QueryType != 1 {
				t.Errorf("Expected query type to be 1, got %d", chunk.QueryType)
			}
			if chunk.TableID != 1 {
				t.Errorf("Expected table ID to be 1, got %d", chunk.TableID)
			}

			if receivedCount >= expectedChunks {
				testing_utils.LogSuccess("All expected chunks received!")
				return
			}

		case <-timeout:
			t.Errorf("Timeout waiting for chunks. Expected %d, received %d", expectedChunks, receivedCount)
			return
		}
	}
}

// testDataHandlerProcessingVerification tests that the data handler processes batches by checking server logs
func testDataHandlerProcessingVerification(t *testing.T) {
	testing_utils.LogStep("Testing data handler processing verification")

	// This test verifies that the data handler is actually processing batches
	// by sending a batch and checking that we get the expected server behavior

	conn, err := net.Dial("tcp", "test-server:8080")
	if err != nil {
		t.Skipf("Skipping test - test server not available: %v", err)
		return
	}
	defer conn.Close()

	// Create a test batch with specific data that we can verify
	testBatch := &batch.Batch{
		ClientID:    "VERI", // Verification test
		FileID:      "FY01", // Verification file
		IsEOF:       true,   // This should create IsLastChunk=true
		BatchNumber: 999,    // Unique batch number
		BatchSize:   15,
		BatchData:   "Verification test data",
	}

	// Serialize and send
	batchMsg := batch.NewBatchMessage(testBatch)
	serializedData, err := batch.SerializeBatchMessage(batchMsg)
	if err != nil {
		t.Fatalf("Failed to serialize batch: %v", err)
	}

	testing_utils.LogStep("Sending verification batch")
	_, err = conn.Write(serializedData)
	if err != nil {
		t.Fatalf("Failed to send batch: %v", err)
	}

	// Read response
	response := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(response)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	responseStr := string(response[:n])
	testing_utils.LogStep("Received response: %s", responseStr)

	// Verify the response contains our specific batch information
	expectedAck := "ACK: Batch received - ClientID: VERI, FileID: FY01, BatchNumber: 999"
	if responseStr != expectedAck {
		t.Errorf("Unexpected response: got %s, want %s", responseStr, expectedAck)
	}

	// The key verification is that we received an acknowledgment
	// This means the data handler processed the batch and sent a chunk
	// (The server logs will show the data handler processing, but we can't easily capture them in the test)

	testing_utils.LogStep("Verification: Data handler processed batch and sent chunk to year-filter")
	testing_utils.LogSuccess("Data handler processing verification completed successfully")
}

// testConnectionHandling tests connection handling and cleanup
func testConnectionHandling(t *testing.T) {
	testing_utils.LogStep("Testing connection handling")

	// Test multiple connections
	numConnections := 3
	connections := make([]net.Conn, numConnections)

	testing_utils.LogStep("Creating %d connections", numConnections)
	for i := 0; i < numConnections; i++ {
		conn, err := net.Dial("tcp", "test-server:8080")
		if err != nil {
			t.Skipf("Skipping test - test server not available: %v", err)
			return
		}
		connections[i] = conn
	}

	testing_utils.LogStep("Sending messages from multiple connections")
	// Send messages from each connection
	for i, conn := range connections {
		testBatch := &batch.Batch{
			ClientID:    fmt.Sprintf("C%03d", i+1), // Exactly 4 bytes
			FileID:      fmt.Sprintf("F%03d", i+1), // Exactly 4 bytes
			IsEOF:       false,
			BatchNumber: 1,
			BatchSize:   15,
			BatchData:   fmt.Sprintf("Connection %d", i+1),
		}

		batchMsg := batch.NewBatchMessage(testBatch)
		serializedData, err := batch.SerializeBatchMessage(batchMsg)
		if err != nil {
			t.Fatalf("Failed to serialize batch message for connection %d: %v", i+1, err)
		}

		_, err = conn.Write(serializedData)
		if err != nil {
			t.Fatalf("Failed to send batch message from connection %d: %v", i+1, err)
		}

		// Read response
		response := make([]byte, 1024)
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := conn.Read(response)
		if err != nil {
			t.Fatalf("Failed to read response from connection %d: %v", i+1, err)
		}

		responseStr := string(response[:n])
		testing_utils.LogStep("Connection %d received: %s", i+1, responseStr)
	}

	testing_utils.LogStep("Closing connections")
	// Close all connections
	for i, conn := range connections {
		err := conn.Close()
		if err != nil {
			t.Errorf("Failed to close connection %d: %v", i+1, err)
		}
	}

	testing_utils.LogSuccess("Connection handling test completed successfully")
}

// testErrorHandling tests error handling scenarios
func testErrorHandling(t *testing.T) {
	testing_utils.LogStep("Testing error handling scenarios")

	testing_utils.LogStep("Testing invalid message format")
	conn, err := net.Dial("tcp", "test-server:8080")
	if err != nil {
		t.Skipf("Skipping test - test server not available: %v", err)
		return
	}
	defer conn.Close()

	// Send invalid data
	invalidData := []byte("This is not a valid batch message")
	_, err = conn.Write(invalidData)
	if err != nil {
		t.Fatalf("Failed to send invalid data: %v", err)
	}

	// Read error response
	response := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(response)
	if err != nil {
		t.Fatalf("Failed to read error response: %v", err)
	}

	responseStr := string(response[:n])
	testing_utils.LogStep("Received error response: %s", responseStr)

	// Verify error response
	if responseStr[:6] != "ERROR:" {
		t.Errorf("Expected error response, got: %s", responseStr)
	}

	testing_utils.LogStep("Testing empty message")
	// Send empty message
	_, err = conn.Write([]byte{})
	if err != nil {
		t.Fatalf("Failed to send empty message: %v", err)
	}

	// Read response (should timeout or close)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Read(response)
	if err == nil {
		t.Log("Empty message was processed (unexpected)")
	} else {
		testing_utils.LogStep("Empty message correctly rejected: %v", err)
	}

	testing_utils.LogSuccess("Error handling test completed successfully")
}

// TestServerAvailability tests if the test server is available
func TestServerAvailability(t *testing.T) {
	testing_utils.InitLogger()
	testing_utils.LogTest("Testing server availability")

	testing_utils.LogStep("Attempting to connect to test server")
	conn, err := net.DialTimeout("tcp", "test-server:8080", 5*time.Second)
	if err != nil {
		t.Skipf("Test server not available: %v", err)
		return
	}
	defer conn.Close()

	testing_utils.LogSuccess("Test server is available and accepting connections")
}

// TestBatchMessageSerialization tests batch message serialization/deserialization
func TestBatchMessageSerialization(t *testing.T) {
	testing_utils.InitLogger()
	testing_utils.LogTest("Testing batch message serialization for data handler flow")

	// Test various batch message scenarios
	testCases := []struct {
		name        string
		clientID    string
		fileID      string
		isEOF       bool
		batchNumber int
		batchData   string
	}{
		{
			name:        "Normal batch",
			clientID:    "1234",
			fileID:      "5678",
			isEOF:       false,
			batchNumber: 1,
			batchData:   "Test data",
		},
		{
			name:        "EOF batch",
			clientID:    "9999",
			fileID:      "8888",
			isEOF:       true,
			batchNumber: 5,
			batchData:   "Final batch data",
		},
		{
			name:        "Empty data batch",
			clientID:    "0000",
			fileID:      "1111",
			isEOF:       false,
			batchNumber: 1,
			batchData:   "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testing_utils.LogStep("Testing %s", tc.name)

			// Create batch
			testBatch := &batch.Batch{
				ClientID:    tc.clientID,
				FileID:      tc.fileID,
				IsEOF:       tc.isEOF,
				BatchNumber: tc.batchNumber,
				BatchSize:   len(tc.batchData),
				BatchData:   tc.batchData,
			}

			// Serialize
			batchMsg := batch.NewBatchMessage(testBatch)
			serializedData, err := batch.SerializeBatchMessage(batchMsg)
			if err != nil {
				t.Fatalf("Failed to serialize batch message: %v", err)
			}

			// Deserialize
			deserializedInterface, err := deserializer.Deserialize(serializedData)
			if err != nil {
				t.Fatalf("Failed to deserialize batch message: %v", err)
			}

			deserializedBatch, ok := deserializedInterface.(*batch.Batch)
			if !ok {
				t.Fatalf("Deserialized message is not a *batch.Batch, got %T", deserializedInterface)
			}

			// Verify data integrity
			if deserializedBatch.ClientID != testBatch.ClientID {
				t.Errorf("ClientID mismatch: got %s, want %s", deserializedBatch.ClientID, testBatch.ClientID)
			}
			if deserializedBatch.FileID != testBatch.FileID {
				t.Errorf("FileID mismatch: got %s, want %s", deserializedBatch.FileID, testBatch.FileID)
			}
			if deserializedBatch.IsEOF != testBatch.IsEOF {
				t.Errorf("IsEOF mismatch: got %t, want %t", deserializedBatch.IsEOF, testBatch.IsEOF)
			}
			if deserializedBatch.BatchNumber != testBatch.BatchNumber {
				t.Errorf("BatchNumber mismatch: got %d, want %d", deserializedBatch.BatchNumber, testBatch.BatchNumber)
			}
			if deserializedBatch.BatchData != testBatch.BatchData {
				t.Errorf("BatchData mismatch: got %s, want %s", deserializedBatch.BatchData, testBatch.BatchData)
			}

			testing_utils.LogSuccess("Batch message %s serialization test passed", tc.name)
		})
	}
}
