package datahandler

import (
	"fmt"
	"log"
	"net"

	"github.com/tp-distribuidos-2c2025/protocol/batch"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// DataHandler struct
type DataHandler struct {
	// Connection for this specific client
	Conn net.Conn

	// Queue producer for sending chunks to query orchestrator
	queueProducer *workerqueue.QueueMiddleware

	// Configuration
	config *middleware.ConnectionConfig
}

// NewDataHandler creates a new Data Handler instance
func NewDataHandler(config *middleware.ConnectionConfig) *DataHandler {
	return &DataHandler{
		config: config,
	}
}

// NewDataHandlerForConnection creates a new Data Handler instance for a specific connection
func NewDataHandlerForConnection(conn net.Conn, config *middleware.ConnectionConfig) *DataHandler {
	return &DataHandler{
		Conn:   conn,
		config: config,
	}
}

// Initialize sets up the queue producer for sending chunks to query orchestrator
func (dh *DataHandler) Initialize() middleware.MessageMiddlewareError {
	// Initialize queue producer for sending chunks to query orchestrator
	dh.queueProducer = workerqueue.NewMessageMiddlewareQueue(
		"step0-data-queue",
		dh.config,
	)
	if dh.queueProducer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	// Declare the producer queue
	if err := dh.queueProducer.DeclareQueue(true, false, false, false); err != 0 {
		return err
	}

	return 0
}

// ProcessBatchMessage processes a batch message and creates chunks
func (dh *DataHandler) ProcessBatchMessage(data []byte) error {
	// Deserialize the batch message
	message, err := deserializer.Deserialize(data)
	if err != nil {
		log.Printf("Data Handler: Failed to deserialize message: %v", err)
		return fmt.Errorf("failed to deserialize message: %w", err)
	}

	// Check if it's a Batch message
	batchMsg, ok := message.(*batch.Batch)
	if !ok {
		log.Printf("Data Handler: Received non-batch message type: %T", message)
		return fmt.Errorf("expected batch message, got %T", message)
	}

	log.Printf("Data Handler: Processing batch - ClientID: %s, FileID: %s, BatchNumber: %d, Data: %s",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber, batchMsg.BatchData)

	// Create chunk from batch
	chunkObj := chunk.NewChunk(
		batchMsg.ClientID,       // clientID
		1,                       // queryType (hardcoded for now)
		batchMsg.BatchNumber,    // chunkNumber
		batchMsg.IsEOF,          // isLastChunk
		0,                       // step (hardcoded to 0 as requested)
		len(batchMsg.BatchData), // chunkSize
		1,                       // tableID (hardcoded for now)
		batchMsg.BatchData,      // chunkData
	)

	// Create chunk message
	chunkMsg := chunk.NewChunkMessage(chunkObj)

	// Send chunk to query orchestrator
	if err := dh.SendChunk(chunkMsg); err != 0 {
		log.Printf("Data Handler: Failed to send chunk: %v", err)
		return fmt.Errorf("failed to send chunk: %v", err)
	}

	log.Printf("Data Handler: Created and sent chunk - ClientID: %s, ChunkNumber: %d, Step: %d, IsLastChunk: %t",
		chunkObj.ClientID, chunkObj.ChunkNumber, chunkObj.Step, chunkObj.IsLastChunk)

	return nil
}

// Start starts the data handler for a specific connection
func (dh *DataHandler) Start() {
	log.Printf("Data Handler: Starting for connection %s", dh.Conn.RemoteAddr())

	// Initialize the data handler
	if err := dh.Initialize(); err != 0 {
		log.Printf("Data Handler: Failed to initialize: %v", err)
		return
	}
	defer dh.Close()

	log.Printf("Data Handler: Ready to process batches for connection %s", dh.Conn.RemoteAddr())

	// Keep the data handler running for this connection
	// The actual batch processing will be done by calling ProcessBatchMessage directly
	select {}
}

// IsReady checks if the data handler is ready to process batches
func (dh *DataHandler) IsReady() bool {
	return dh.queueProducer != nil
}

// SendChunk sends a chunk message to the query orchestrator
func (dh *DataHandler) SendChunk(chunkMsg *chunk.ChunkMessage) middleware.MessageMiddlewareError {
	// Serialize the chunk message
	messageData, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
		fmt.Printf("Failed to serialize chunk message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send to queue
	return dh.queueProducer.Send(messageData)
}

// Close closes all connections
func (dh *DataHandler) Close() middleware.MessageMiddlewareError {
	if dh.queueProducer != nil {
		return dh.queueProducer.Close()
	}
	return 0
}
