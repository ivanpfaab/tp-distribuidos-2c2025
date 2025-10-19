package datahandler

import (
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/batch"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

const (
	// Join data handler queue
	FixedJoinDataQueue = queues.FixedJoinDataQueue
)

// DataHandler struct
type DataHandler struct {
	// Connection for this specific client
	Conn net.Conn

	// Queue producer for sending chunks directly to year-filter (QueryData)
	yearFilterProducer *workerqueue.QueueMiddleware

	// Queue producer for sending chunks directly to join handler (Fixed data used for the joins)
	joinDataHandlerProducer *workerqueue.QueueMiddleware

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

// Initialize sets up the queue producers for sending chunks to data writer and year-filter
func (dh *DataHandler) Initialize() middleware.MessageMiddlewareError {

	// Initialize queue producer for sending chunks directly to year-filter
	dh.yearFilterProducer = workerqueue.NewMessageMiddlewareQueue(
		"year-filter",
		dh.config,
	)
	if dh.yearFilterProducer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	// Declare the year-filter producer queue
	if err := dh.yearFilterProducer.DeclareQueue(false, false, false, false); err != 0 {
		return err
	}

	// Initialize queue producer for sending chunks directly to join data handler
	dh.joinDataHandlerProducer = workerqueue.NewMessageMiddlewareQueue(
		FixedJoinDataQueue,
		dh.config,
	)
	if dh.joinDataHandlerProducer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	// Declare the fixed join data queue
	if err := dh.joinDataHandlerProducer.DeclareQueue(false, false, false, false); err != 0 {
		return err
	}

	return 0
}

// isReferenceDataChunk checks if the chunk belongs to reference data files that need to be sent to writer
func isReferenceDataChunk(fileID string) bool {
	// Reference data files that need to be stored in writer for joins:
	// - ST: stores.csv (for store_id joins in query 3)
	// - MN: menu_items.csv (for item_id joins in query 2)
	// - US, US: users_*.csv (for user_id joins in query 4)
	return strings.HasPrefix(fileID, "ST") || strings.HasPrefix(fileID, "MN") || strings.HasPrefix(fileID, "US")
}

// isTransactionDataChunk checks if the chunk belongs to transaction data files
func isTransactionDataChunk(fileID string) bool {
	// Transaction data files:
	// - TR, TR: transactions_*.csv (for store_id and user_id joins)
	// - TI, TI: transaction_items_*.csv (for item_id joins)
	return strings.HasPrefix(fileID, "TR") || strings.HasPrefix(fileID, "TI")
}

// determineQueryType determines the query type based on the file ID
func determineQueryType(fileID string) uint8 {
	// Query type mapping based on file type:
	// Query 2: transaction_items ↔ menu_items (on item_id)
	// Query 3: transactions ↔ stores (on store_id)
	// Query 4: transactions ↔ users (on user_id)

	switch {
	case strings.HasPrefix(fileID, "TI"):
		// Transaction items files - Query 2 (item_id joins with menu_items)
		return 2
	case strings.HasPrefix(fileID, "TR"):
		// Transaction files - Query 3 (store_id joins with stores)
		// Note: For now, we'll use Query 3 for transactions.
		return 3
	default:
		// Default to Query 3 for other transaction data files
		return 3
	}
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

	log.Printf("Data Handler: Processing batch - ClientID: %s, FileID: %s, BatchNumber: %d",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber)

	// Route chunk based on file type
	if isReferenceDataChunk(batchMsg.FileID) {
		// Send reference data (stores, menu_items, users) to join-data-handler for routing to join workers
		queryType := determineQueryType(batchMsg.FileID)

		chunkObj := chunk.NewChunk(
			batchMsg.ClientID,        // clientID
			batchMsg.FileID,          // fileID
			queryType,                // queryType (determined by file type)
			batchMsg.BatchNumber,     // chunkNumber
			batchMsg.IsEOF,           // isLastChunk
			batchMsg.IsLastFromTable, // isLastFromTable (maintained from batch)
			0,                        // step (hardcoded to 0 as requested)
			batchMsg.BatchSize,       // chunkSize (number of rows from batch)
			1,                        // tableID (hardcoded for now)
			batchMsg.BatchData,       // chunkData
		)

		chunkMsg := chunk.NewChunkMessage(chunkObj)

		if err := dh.SendChunkToJoinDataHandler(chunkMsg); err != 0 {
			log.Printf("Data Handler: Failed to send reference data chunk to join data handler: %v", err)
			return fmt.Errorf("failed to send reference data chunk to join data handler: %v", err)
		}
		log.Printf("Data Handler: Sent reference data chunk to join data handler - ClientID: %s, FileID: %s, ChunkNumber: %d",
			chunkObj.ClientID, chunkObj.FileID, chunkObj.ChunkNumber)

	} else if isTransactionDataChunk(batchMsg.FileID) {
		// Send all transaction data to year-filter (non-reference data)
		if strings.HasPrefix(batchMsg.FileID, "TR") {
			// Transaction files - send all query types to year-filter
			queryTypes := []uint8{1, 3, 4}

			for _, queryType := range queryTypes {
				chunkObj := chunk.NewChunk(
					batchMsg.ClientID,        // clientID
					batchMsg.FileID,          // fileID
					queryType,                // queryType (1, 3, or 4)
					batchMsg.BatchNumber,     // chunkNumber
					batchMsg.IsEOF,           // isLastChunk
					batchMsg.IsLastFromTable, // isLastFromTable (maintained from batch)
					0,                        // step (hardcoded to 0 as requested)
					batchMsg.BatchSize,       // chunkSize (number of rows from batch)
					1,                        // tableID (hardcoded for now)
					batchMsg.BatchData,       // chunkData
				)

				chunkMsg := chunk.NewChunkMessage(chunkObj)

				if err := dh.SendChunkToYearFilter(chunkMsg); err != 0 {
					log.Printf("Data Handler: Failed to send transaction data chunk to year-filter (Query %d): %v", queryType, err)
					return fmt.Errorf("failed to send transaction data chunk to year-filter (Query %d): %v", queryType, err)
				}
				log.Printf("Data Handler: Sent transaction data chunk to year-filter - ClientID: %s, FileID: %s, ChunkNumber: %d, QueryType: %d",
					chunkObj.ClientID, chunkObj.FileID, chunkObj.ChunkNumber, queryType)
			}
		} else if strings.HasPrefix(batchMsg.FileID, "TI") {
			// Transaction items files - send to year-filter
			queryType := uint8(2)

			chunkObj := chunk.NewChunk(
				batchMsg.ClientID,        // clientID
				batchMsg.FileID,          // fileID
				queryType,                // queryType (2)
				batchMsg.BatchNumber,     // chunkNumber
				batchMsg.IsEOF,           // isLastChunk
				batchMsg.IsLastFromTable, // isLastFromTable (maintained from batch)
				0,                        // step (hardcoded to 0 as requested)
				batchMsg.BatchSize,       // chunkSize (number of rows from batch)
				1,                        // tableID (hardcoded for now)
				batchMsg.BatchData,       // chunkData
			)

			chunkMsg := chunk.NewChunkMessage(chunkObj)

			if err := dh.SendChunkToYearFilter(chunkMsg); err != 0 {
				log.Printf("Data Handler: Failed to send transaction items chunk to year-filter: %v", err)
				return fmt.Errorf("failed to send transaction items chunk to year-filter: %v", err)
			}
			log.Printf("Data Handler: Sent transaction items chunk to year-filter - ClientID: %s, FileID: %s, ChunkNumber: %d",
				chunkObj.ClientID, chunkObj.FileID, chunkObj.ChunkNumber)
		}
	} else {
		log.Printf("Data Handler: Unknown file type - ClientID: %s, FileID: %s, BatchNumber: %d",
			batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber)
	}

	log.Printf("Data Handler: Completed processing batch - ClientID: %s, FileID: %s, BatchNumber: %d, IsLastChunk: %t, IsLastFromTable: %t",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber, batchMsg.IsEOF, batchMsg.IsLastFromTable)

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
	return dh.yearFilterProducer != nil && dh.joinDataHandlerProducer != nil
}

// SendChunkToYearFilter sends a chunk message directly to the year-filter worker
func (dh *DataHandler) SendChunkToYearFilter(chunkMsg *chunk.ChunkMessage) middleware.MessageMiddlewareError {
	// Serialize the chunk message
	messageData, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
		fmt.Printf("Failed to serialize chunk message for year-filter: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send to year-filter queue
	return dh.yearFilterProducer.Send(messageData)
}

// SendChunkToJoinDataHandler sends a chunk message to the join data handler worker
func (dh *DataHandler) SendChunkToJoinDataHandler(chunkMsg *chunk.ChunkMessage) middleware.MessageMiddlewareError {
	// Serialize the chunk message
	messageData, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
		fmt.Printf("Failed to serialize chunk message for join data handler: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	fmt.Printf("Data Handler: Sending chunk to join data handler - ClientID: %s, FileID: %s, ChunkNumber: %d /n",
		chunkMsg.Chunk.ClientID, chunkMsg.Chunk.FileID, chunkMsg.Chunk.ChunkNumber)

	// Send to join data handler queue
	return dh.joinDataHandlerProducer.Send(messageData)
}

// Close closes all connections
func (dh *DataHandler) Close() middleware.MessageMiddlewareError {
	var err middleware.MessageMiddlewareError = 0

	if dh.yearFilterProducer != nil {
		if closeErr := dh.yearFilterProducer.Close(); closeErr != 0 {
			err = closeErr
		}
	}

	if dh.joinDataHandlerProducer != nil {
		if closeErr := dh.joinDataHandlerProducer.Close(); closeErr != 0 {
			err = closeErr
		}
	}

	return err
}
