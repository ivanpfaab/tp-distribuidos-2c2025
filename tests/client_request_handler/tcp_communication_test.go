package main

import (
	"fmt"
	"testing"

	batch "tp-distribuidos-2c2025/protocol/batch"
	common "tp-distribuidos-2c2025/protocol/common"
	testing_utils "tp-distribuidos-2c2025/shared/testing"
)

// TestTCPCommunication tests batch message serialization and deserialization
func TestTCPCommunication(t *testing.T) {
	testing_utils.InitLogger()
	testing_utils.LogTest("Testing TCP Communication - Batch Message Serialization and Deserialization")

	t.Run("Batch Message Serialization", testBatchMessageSerialization)
	t.Run("Batch Message Deserialization", testBatchMessageDeserialization)
	t.Run("Batch Message Round Trip", testBatchMessageRoundTrip)
	t.Run("Multiple Batch Messages", testMultipleBatchMessages)
}

// testBatchMessageDeserialization tests batch message deserialization
func testBatchMessageDeserialization(t *testing.T) {
	testing_utils.LogStep("Creating test batch for deserialization")

	// Create a test batch
	testBatch := &batch.Batch{
		ClientID:    "1234", // Exactly 4 bytes
		FileID:      "5678", // Exactly 4 bytes
		IsEOF:       false,
		BatchNumber: 1,
		BatchSize:   15,
		BatchData:   "Hello, World!",
	}

	testing_utils.LogStep("Serializing batch message")
	// Create batch message and serialize
	batchMsg := batch.NewBatchMessage(testBatch)
	serializedData, err := batch.SerializeBatchMessage(batchMsg)
	if err != nil {
		t.Fatalf("Failed to serialize batch message: %v", err)
	}

	testing_utils.LogStep("Deserializing batch message")
	// Deserialize the common header
	deserializedInterface, err := common.Deserialize(serializedData)
	if err != nil {
		t.Fatalf("Failed to deserialize common header: %v", err)
	}

	deserializedBatch, ok := deserializedInterface.(*common.Batch)
	if !ok {
		t.Fatalf("Deserialized message is not a *common.Batch, got %T", deserializedInterface)
	}

	testing_utils.LogStep("Validating deserialized batch data")
	// Verify the deserialized batch
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
	if deserializedBatch.BatchSize != testBatch.BatchSize {
		t.Errorf("BatchSize mismatch: got %d, want %d", deserializedBatch.BatchSize, testBatch.BatchSize)
	}
	if deserializedBatch.BatchData != testBatch.BatchData {
		t.Errorf("BatchData mismatch: got %s, want %s", deserializedBatch.BatchData, testBatch.BatchData)
	}

	testing_utils.LogSuccess("Batch message deserialization successful")
}

// testBatchMessageSerialization tests the batch message serialization/deserialization
func testBatchMessageSerialization(t *testing.T) {
	testing_utils.LogStep("Creating test batch for serialization")

	// Create a test batch
	testBatch := &batch.Batch{
		ClientID:    "1234", // Exactly 4 bytes
		FileID:      "5678", // Exactly 4 bytes
		IsEOF:       false,
		BatchNumber: 1,
		BatchSize:   15,
		BatchData:   "Hello, World!",
	}

	testing_utils.LogStep("Serializing batch message")
	// Create batch message and serialize
	batchMsg := batch.NewBatchMessage(testBatch)
	serializedData, err := batch.SerializeBatchMessage(batchMsg)
	if err != nil {
		t.Fatalf("Failed to serialize batch message: %v", err)
	}

	testing_utils.LogStep("Deserializing batch message")
	// Deserialize the common header
	deserializedInterface, err := common.Deserialize(serializedData)
	if err != nil {
		t.Fatalf("Failed to deserialize common header: %v", err)
	}

	deserializedBatch, ok := deserializedInterface.(*common.Batch)
	if !ok {
		t.Fatalf("Deserialized message is not a *common.Batch, got %T", deserializedInterface)
	}

	testing_utils.LogStep("Validating serialized batch data")
	// Verify the deserialized batch
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
	if deserializedBatch.BatchSize != testBatch.BatchSize {
		t.Errorf("BatchSize mismatch: got %d, want %d", deserializedBatch.BatchSize, testBatch.BatchSize)
	}
	if deserializedBatch.BatchData != testBatch.BatchData {
		t.Errorf("BatchData mismatch: got %s, want %s", deserializedBatch.BatchData, testBatch.BatchData)
	}

	testing_utils.LogSuccess("Batch message serialization/deserialization successful")
}

// testBatchMessageRoundTrip tests complete serialization and deserialization round trip
func testBatchMessageRoundTrip(t *testing.T) {
	testing_utils.LogStep("Creating test batch for round trip test")

	// Create a test batch
	testBatch := &batch.Batch{
		ClientID:    "1234", // Exactly 4 bytes
		FileID:      "5678", // Exactly 4 bytes
		IsEOF:       false,
		BatchNumber: 1,
		BatchSize:   22,
		BatchData:   "Test message from test",
	}

	testing_utils.LogStep("Performing serialization round trip")
	// Create batch message and serialize
	batchMsg := batch.NewBatchMessage(testBatch)
	serializedData, err := batch.SerializeBatchMessage(batchMsg)
	if err != nil {
		t.Fatalf("Failed to serialize batch message: %v", err)
	}

	// Deserialize the common header
	deserializedInterface, err := common.Deserialize(serializedData)
	if err != nil {
		t.Fatalf("Failed to deserialize common header: %v", err)
	}

	deserializedBatch, ok := deserializedInterface.(*common.Batch)
	if !ok {
		t.Fatalf("Deserialized message is not a *common.Batch, got %T", deserializedInterface)
	}

	testing_utils.LogStep("Validating round trip data integrity")
	// Verify the round trip was successful
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
	if deserializedBatch.BatchSize != testBatch.BatchSize {
		t.Errorf("BatchSize mismatch: got %d, want %d", deserializedBatch.BatchSize, testBatch.BatchSize)
	}
	if deserializedBatch.BatchData != testBatch.BatchData {
		t.Errorf("BatchData mismatch: got %s, want %s", deserializedBatch.BatchData, testBatch.BatchData)
	}

	testing_utils.LogSuccess("Batch message round trip successful")
}

// testMultipleBatchMessages tests serialization of multiple batch messages
func testMultipleBatchMessages(t *testing.T) {
	testing_utils.LogStep("Preparing multiple batch messages for testing")

	testMessages := []string{
		"First message",
		"Second message",
		"Third message",
	}

	expectedMessages := len(testMessages)
	successfulSerializations := 0

	testing_utils.LogStep("Processing %d batch messages", expectedMessages)
	for i, messageData := range testMessages {
		// Create batch message
		testBatch := &batch.Batch{
			ClientID:    fmt.Sprintf("%04d", i+1), // Exactly 4 bytes
			FileID:      fmt.Sprintf("%04d", i+1), // Exactly 4 bytes
			IsEOF:       i == len(testMessages)-1,
			BatchNumber: i + 1,
			BatchSize:   len(messageData),
			BatchData:   messageData,
		}

		testing_utils.LogStep("Serializing batch message %d: %s", i+1, messageData)
		batchMsg := batch.NewBatchMessage(testBatch)
		serializedData, err := batch.SerializeBatchMessage(batchMsg)
		if err != nil {
			t.Errorf("Failed to serialize batch message %d: %v", i+1, err)
			continue
		}

		// Verify serialization was successful
		if len(serializedData) == 0 {
			t.Errorf("Serialized data is empty for message %d", i+1)
			continue
		}

		testing_utils.LogStep("Deserializing batch message %d", i+1)
		// Test deserialization
		deserializedInterface, err := common.Deserialize(serializedData)
		if err != nil {
			t.Errorf("Failed to deserialize batch message %d: %v", i+1, err)
			continue
		}

		deserializedBatch, ok := deserializedInterface.(*common.Batch)
		if !ok {
			t.Errorf("Deserialized message %d is not a *common.Batch, got %T", i+1, deserializedInterface)
			continue
		}

		testing_utils.LogStep("Validating batch message %d data", i+1)
		// Verify the deserialized batch matches the original
		if deserializedBatch.ClientID != testBatch.ClientID {
			t.Errorf("Message %d ClientID mismatch: got %s, want %s", i+1, deserializedBatch.ClientID, testBatch.ClientID)
			continue
		}
		if deserializedBatch.BatchData != testBatch.BatchData {
			t.Errorf("Message %d BatchData mismatch: got %s, want %s", i+1, deserializedBatch.BatchData, testBatch.BatchData)
			continue
		}

		successfulSerializations++
		testing_utils.LogStep("Successfully processed batch message %d: %s", i+1, messageData)
	}

	if successfulSerializations != expectedMessages {
		t.Errorf("Expected %d successful serializations, got %d", expectedMessages, successfulSerializations)
	}
	testing_utils.LogSuccess("Successfully processed %d batch messages", successfulSerializations)
}
