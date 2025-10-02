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

	// Queue producer for sending chunks to data writer
	writerProducer *workerqueue.QueueMiddleware

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

// Initialize sets up the queue producers for sending chunks to query orchestrator and data writer
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

	// Initialize queue producer for sending chunks to data writer
	dh.writerProducer = workerqueue.NewMessageMiddlewareQueue(
		"data-writer-queue",
		dh.config,
	)
	if dh.writerProducer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	// Declare the writer producer queue
	if err := dh.writerProducer.DeclareQueue(true, false, false, false); err != 0 {
		return err
	}

	return 0
}

// isReferenceDataChunk checks if the chunk belongs to reference data files that need to be sent to writer
func isReferenceDataChunk(fileID string) bool {
	// Reference data files that need to be stored in writer for joins:
	// - ST01: stores.csv (for store_id joins in query 3)
	// - MN01: menu_items.csv (for item_id joins in query 2)
	// - US01, US02: users_*.csv (for user_id joins in query 4)
	return fileID == "ST01" || fileID == "MN01" || fileID == "US01" || fileID == "US02"
}

// isTransactionDataChunk checks if the chunk belongs to transaction data files
func isTransactionDataChunk(fileID string) bool {
	// Transaction data files:
	// - TR01, TR02: transactions_*.csv (for store_id and user_id joins)
	// - TI01, TI02: transaction_items_*.csv (for item_id joins)
	return fileID == "TR01" || fileID == "TR02" || fileID == "TI01" || fileID == "TI02"
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
		batchMsg.FileID,         // fileID
		2,                       // queryType (hardcoded for now)
		batchMsg.BatchNumber,    // chunkNumber
		batchMsg.IsEOF,          // isLastChunk
		0,                       // step (hardcoded to 0 as requested)
		len(batchMsg.BatchData), // chunkSize
		1,                       // tableID (hardcoded for now)
		batchMsg.BatchData,      // chunkData
	)

	// Create chunk message
	chunkMsg := chunk.NewChunkMessage(chunkObj)

	// Route chunk based on file type
	if isReferenceDataChunk(chunkObj.FileID) {
		// Send reference data (stores, menu_items, users) to writer for join operations
		if err := dh.SendChunkToWriter(chunkMsg); err != 0 {
			log.Printf("Data Handler: Failed to send reference data chunk to writer: %v", err)
			return fmt.Errorf("failed to send reference data chunk to writer: %v", err)
		}
		log.Printf("Data Handler: Sent reference data chunk to writer - ClientID: %s, FileID: %s, ChunkNumber: %d",
			chunkObj.ClientID, chunkObj.FileID, chunkObj.ChunkNumber)
	} else if isTransactionDataChunk(chunkObj.FileID) {
		// Send transaction data (transactions, transaction_items) to query orchestrator for processing
		if err := dh.SendChunk(chunkMsg); err != 0 {
			log.Printf("Data Handler: Failed to send transaction data chunk to orchestrator: %v", err)
			return fmt.Errorf("failed to send transaction data chunk to orchestrator: %v", err)
		}
		log.Printf("Data Handler: Sent transaction data chunk to orchestrator - ClientID: %s, FileID: %s, ChunkNumber: %d",
			chunkObj.ClientID, chunkObj.FileID, chunkObj.ChunkNumber)
	} else {
		log.Printf("Data Handler: Unknown file type - ClientID: %s, FileID: %s, ChunkNumber: %d",
			chunkObj.ClientID, chunkObj.FileID, chunkObj.ChunkNumber)
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

	log.Printf("Data Handler: Ready to process batches for connection %s", dh.Conn.RemoteAddr())

	// The data handler is now ready and will be used by the client request handler
	// The connection will be managed by the client request handler
	// Don't close here - let the client request handler manage the lifecycle
}

// IsReady checks if the data handler is ready to process batches
func (dh *DataHandler) IsReady() bool {
	return dh.queueProducer != nil && dh.writerProducer != nil
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

// SendChunkToWriter sends a chunk message to the data writer
func (dh *DataHandler) SendChunkToWriter(chunkMsg *chunk.ChunkMessage) middleware.MessageMiddlewareError {
	// Serialize the chunk message
	messageData, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
		fmt.Printf("Failed to serialize chunk message for writer: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send to writer queue
	return dh.writerProducer.Send(messageData)
}

// Close closes all connections
func (dh *DataHandler) Close() middleware.MessageMiddlewareError {
	var err middleware.MessageMiddlewareError = 0

	if dh.queueProducer != nil {
		if closeErr := dh.queueProducer.Close(); closeErr != 0 {
			err = closeErr
		}
	}

	if dh.writerProducer != nil {
		if closeErr := dh.writerProducer.Close(); closeErr != 0 {
			err = closeErr
		}
	}

	return err
}
