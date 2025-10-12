package controllers

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/batch"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	datahandler "github.com/tp-distribuidos-2c2025/server/controller/data-handler"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// ClientRequestHandler handles incoming client requests
type ClientRequestHandler struct {
	config *middleware.ConnectionConfig
}

// NewClientRequestHandler creates a new instance of ClientRequestHandler
func NewClientRequestHandler(config *middleware.ConnectionConfig) *ClientRequestHandler {
	return &ClientRequestHandler{
		config: config,
	}
}

// HandleConnection handles a TCP connection and creates a data handler for it
func (h *ClientRequestHandler) HandleConnection(conn net.Conn) {
	defer conn.Close()

	log.Printf("Client Request Handler: New connection from %s", conn.RemoteAddr())

	// Create a data handler for this connection
	dataHandler := datahandler.NewDataHandlerForConnection(conn, h.config)

	// Initialize the data handler (don't start it in a goroutine)
	dataHandler.Start()

	// Wait for data handler to be ready
	for !dataHandler.IsReady() {
		time.Sleep(10 * time.Millisecond)
	}

	log.Printf("Client Request Handler: Data handler ready for connection %s", conn.RemoteAddr())

	// Keep the connection alive and process messages
	for {
		// Set a read timeout for each message
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		// Read the complete header (7 bytes: HeaderLength + TotalLength + MsgTypeID)
		// Handle TCP short reads by reading until we get all 7 bytes
		headerBuffer := make([]byte, 7)
		bytesRead := 0
		for bytesRead < 7 {
			n, err := conn.Read(headerBuffer[bytesRead:])
			if err != nil {
				// For any error (connection closed, network issue, etc.), break the loop
				log.Printf("Client Request Handler: Connection %s closed: %v", conn.RemoteAddr(), err)
				break
			}
			bytesRead += n
		}

		// If we didn't read all 7 bytes, break the loop
		if bytesRead < 7 {
			log.Printf("Client Request Handler: Incomplete header read: %d/7 bytes", bytesRead)
			break
		}

		// Parse header components
		headerLength := int(binary.BigEndian.Uint16(headerBuffer[0:2]))
		totalLength := int(binary.BigEndian.Uint32(headerBuffer[2:6]))
		msgTypeID := int(headerBuffer[6])

		// Log header information for debugging
		log.Printf("Client Request Handler: Header info - HeaderLength: %d, TotalLength: %d, MsgTypeID: %d",
			headerLength, totalLength, msgTypeID)

		// Basic validation to prevent panic
		if totalLength < 7 || totalLength > 10*1024*1024 { // Reasonable bounds (10MB max)
			log.Printf("Client Request Handler: Invalid total length %d, closing connection", totalLength)
			continue
		}

		// Calculate remaining data size
		remainingDataSize := totalLength - 7 // totalLength - headerLength(2) - totalLength(4) - msgTypeID(1)

		// Read the remaining message data - handle TCP short reads
		remainingData := make([]byte, remainingDataSize)
		bytesRead = 0
		for bytesRead < remainingDataSize {
			n, err := conn.Read(remainingData[bytesRead:])
			if err != nil {
				// For any error (connection closed, network issue, etc.), break the loop
				log.Printf("Client Request Handler: Failed to read complete message from %s: %v", conn.RemoteAddr(), err)
				break
			}
			bytesRead += n
		}

		// If we didn't read all remaining data, break the loop
		if bytesRead < remainingDataSize {
			log.Printf("Client Request Handler: Incomplete message data read: %d/%d bytes", bytesRead, remainingDataSize)
			continue
		}

		// Combine header and data
		completeMessage := append(headerBuffer, remainingData...)

		// Process the batch message directly
		response, err := h.processBatchMessage(completeMessage, dataHandler)
		if err != nil {
			log.Printf("Client Request Handler: Failed to process message from %s: %v", conn.RemoteAddr(), err)
			response = []byte("ERROR: " + err.Error())
		}

		// Send response back to client
		_, err = conn.Write(response)
		if err != nil {
			// For any error (connection closed, network issue, etc.), break the loop
			log.Printf("Client Request Handler: Failed to send response to %s: %v", conn.RemoteAddr(), err)
			break
		}

		log.Printf("Client Request Handler: Sent response to %s: %s", conn.RemoteAddr(), string(response))
	}

	// Clean up the data handler when connection closes
	log.Printf("Client Request Handler: Cleaning up data handler for connection %s", conn.RemoteAddr())
	dataHandler.Close()
}

// processBatchMessage processes a batch message and returns a response
func (h *ClientRequestHandler) processBatchMessage(data []byte, dataHandler *datahandler.DataHandler) ([]byte, error) {
	// Data handler should already be ready since we wait for it in HandleConnection

	// Deserialize the message
	message, err := deserializer.Deserialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %w", err)
	}

	// Check if it's a Batch message (only type we handle)
	batchMsg, ok := message.(*batch.Batch)
	if !ok {
		return nil, fmt.Errorf("expected batch message, got %T", message)
	}

	// Log the received batch
	log.Printf("Client Request Handler: Received batch - ClientID: %s, FileID: %s, BatchNumber: %d",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber)

	// Process the batch message directly with the data handler
	if err := dataHandler.ProcessBatchMessage(data); err != nil {
		log.Printf("Client Request Handler: Failed to process batch with data handler: %v", err)
		return nil, fmt.Errorf("failed to process batch with data handler: %w", err)
	}

	log.Printf("Client Request Handler: Successfully processed batch - ClientID: %s, FileID: %s, BatchNumber: %d",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber)

	// Create acknowledgment response
	response := fmt.Sprintf("ACK: Batch received - ClientID: %s, FileID: %s, BatchNumber: %d\n",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber)

	return []byte(response), nil
}

// Close performs any necessary cleanup
func (h *ClientRequestHandler) Close() {
	log.Printf("Client Request Handler: Closing handler")
	// Add any cleanup logic here if needed
}
