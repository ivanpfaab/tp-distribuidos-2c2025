package client_request_handler

import (
	"fmt"
	"log"

	"github.com/tp-distribuidos-2c2025/protocol/batch"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	datahandler "github.com/tp-distribuidos-2c2025/server/controller/data-handler"
)

// BatchMessageProcessor processes batch messages
type BatchMessageProcessor struct{}

// NewBatchMessageProcessor creates a new batch message processor
func NewBatchMessageProcessor() *BatchMessageProcessor {
	return &BatchMessageProcessor{}
}

// Process processes a batch message and returns a response
func (p *BatchMessageProcessor) Process(data []byte, dataHandler *datahandler.DataHandler) ([]byte, error) {
	// Deserialize the message
	message, err := deserializer.Deserialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %w", err)
	}

	// Check if it's a Batch message
	batchMsg, ok := message.(*batch.Batch)
	if !ok {
		return nil, fmt.Errorf("expected batch message, got %T", message)
	}

	// Log the received batch
	log.Printf("Batch Processor: Received batch - ClientID: %s, FileID: %s, BatchNumber: %d",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber)

	// Process the batch message directly with the data handler
	if err := dataHandler.ProcessBatchMessage(data); err != nil {
		log.Printf("Batch Processor: Failed to process batch with data handler: %v", err)
		return nil, fmt.Errorf("failed to process batch with data handler: %w", err)
	}

	log.Printf("Batch Processor: Successfully processed batch - ClientID: %s, FileID: %s, BatchNumber: %d",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber)

	// Create acknowledgment response
	response := fmt.Sprintf("ACK: Batch received - ClientID: %s, FileID: %s, BatchNumber: %d\n",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber)

	return []byte(response), nil
}

// ExtractClientID extracts client ID from a message
func (p *BatchMessageProcessor) ExtractClientID(data []byte) (*batch.Batch, bool) {
	message, err := deserializer.Deserialize(data)
	if err != nil {
		return nil, false
	}

	if batchMsg, ok := message.(*batch.Batch); ok {
		return batchMsg, true
	}
	return nil, false
}

