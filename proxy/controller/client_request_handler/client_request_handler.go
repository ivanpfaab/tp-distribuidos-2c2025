package client_request_handler

import (
	"log"
	"net"
	"sync"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	datahandler "github.com/tp-distribuidos-2c2025/proxy/controller/data-handler"
	"github.com/tp-distribuidos-2c2025/proxy/network"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

// ClientRequestHandler handles incoming client requests
type ClientRequestHandler struct {
	config                *middleware.ConnectionConfig
	clientResultsConsumer *workerqueue.QueueConsumer
	completionExchange    *exchange.ExchangeMiddleware
	connectionManager     *network.ConnectionManager
	messageReader         *network.MessageReader
	processor             *BatchMessageProcessor
	responseFormatter     *ResponseFormatter
	resultsHandler        *ResultsHandler
	completedClients      map[string]bool // clientID -> completed (tracks if completion signal was received)
	completedMutex        sync.RWMutex    // Mutex for thread-safe access
}

// NewClientRequestHandler creates a new instance of ClientRequestHandler
func NewClientRequestHandler(config *middleware.ConnectionConfig) *ClientRequestHandler {
	// Create client results consumer
	clientResultsConsumer := workerqueue.NewQueueConsumer(
		queues.ClientResultsQueue,
		config,
	)

	// Declare Query2 results queue
	clientResultsQueueDeclarer := workerqueue.NewMessageMiddlewareQueue(
		queues.ClientResultsQueue,
		config,
	)
	if err := clientResultsQueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		return nil
	}
	clientResultsQueueDeclarer.Close()

	// Create and declare the client completion cleanup fanout exchange
	completionExchange := exchange.NewMessageMiddlewareExchange(
		queues.ClientCompletionCleanupExchange,
		[]string{}, // No routing keys needed for fanout
		config,
	)
	if completionExchange == nil {
		log.Printf("Client Request Handler: Failed to create completion exchange producer")
		return nil
	}

	// Declare the fanout exchange (not durable, not auto-delete)
	if err := completionExchange.DeclareExchange("fanout", false, false, false, false); err != 0 {
		log.Printf("Client Request Handler: Failed to declare completion exchange: %v", err)
		completionExchange.Close()
		return nil
	}

	connectionManager := network.NewConnectionManager()
	return &ClientRequestHandler{
		config:                config,
		clientResultsConsumer: clientResultsConsumer,
		completionExchange:    completionExchange,
		connectionManager:     connectionManager,
		messageReader:         network.NewMessageReader(),
		processor:             NewBatchMessageProcessor(),
		responseFormatter:     NewResponseFormatter(),
		resultsHandler:        NewResultsHandler(connectionManager, completionExchange),
		completedClients:      make(map[string]bool),
	}
}

// HandleConnection handles a TCP connection and creates a data handler for it
func (h *ClientRequestHandler) HandleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		// Remove connection from connection manager
		h.connectionManager.Remove(conn)
	}()

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

	// Track client ID for this connection
	var currentClientID string

	// Keep the connection alive and process messages
	for {
		// Read complete message using message reader
		completeMessage, err := h.messageReader.ReadCompleteMessage(conn)
		if err != nil {
			log.Printf("Client Request Handler: Failed to read message from %s: %v", conn.RemoteAddr(), err)
			break
		}

		// Process the batch message
		response, err := h.processor.Process(completeMessage, dataHandler)
		if err != nil {
			log.Printf("Client Request Handler: Failed to process message from %s: %v", conn.RemoteAddr(), err)
			response = h.responseFormatter.FormatError(err)
		} else {
			// Store connection for this client if it's a batch message
			if batchMsg, ok := h.processor.ExtractClientID(completeMessage); ok {
				currentClientID = batchMsg.ClientID
				h.connectionManager.Store(batchMsg.ClientID, conn)
			}
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

	// Check if client disconnected prematurely (before completion signal)
	if currentClientID != "" {
		h.handleClientDisconnect(currentClientID)
	}
}

// markClientCompleted marks a client as having received a completion signal
func (h *ClientRequestHandler) markClientCompleted(clientID string) {
	h.completedMutex.Lock()
	defer h.completedMutex.Unlock()
	h.completedClients[clientID] = true
	log.Printf("Client Request Handler: Marked client %s as completed (received completion signal)", clientID)
}

// isClientCompleted checks if a client has received a completion signal
func (h *ClientRequestHandler) isClientCompleted(clientID string) bool {
	h.completedMutex.RLock()
	defer h.completedMutex.RUnlock()
	return h.completedClients[clientID]
}

// handleClientDisconnect handles client disconnection before completion
func (h *ClientRequestHandler) handleClientDisconnect(clientID string) {
	// Check if client already completed successfully
	if h.isClientCompleted(clientID) {
		log.Printf("Client Request Handler: Client %s disconnected after completion (normal)", clientID)
		return
	}

	// Client disconnected prematurely - trigger completion signal
	log.Printf("Client Request Handler: Client %s disconnected before completion, triggering cleanup", clientID)

	// Create completion signal for premature disconnect
	completionSignal := signals.NewClientCompletionSignal(
		clientID,
		"Client disconnected before query completion - cleaning up resources",
	)

	// Send to results handler to process (this will mark as completed and send to exchange)
	h.resultsHandler.HandleCompletionSignal(completionSignal)
}

// StartClientResultsConsumer starts consuming formatted results from results dispatcher
func (h *ClientRequestHandler) StartClientResultsConsumer() {
	if h.clientResultsConsumer == nil {
		log.Printf("Client Request Handler: Client results consumer not initialized")
		return
	}

	log.Printf("Client Request Handler: Starting client results consumer...")

	// Start consuming
	err := h.clientResultsConsumer.StartConsuming(func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			// Deserialize once to check message type
			message, err := deserializer.Deserialize(delivery.Body)
			if err != nil {
				log.Printf("Client Request Handler: Failed to deserialize message in consumer: %v", err)
				delivery.Ack(false)
				continue
			}

			// Track completion signals before processing
			if signal, ok := message.(*signals.ClientCompletionSignal); ok {
				h.markClientCompleted(signal.ClientID)
			}

			// Process message
			h.resultsHandler.ProcessMessage(delivery.Body)

			delivery.Ack(false)
		}
		done <- nil
	})

	if err != 0 {
		log.Printf("Client Request Handler: Error in client results consumer: %v", err)
	}
}

// Close performs any necessary cleanup
func (h *ClientRequestHandler) Close() {
	log.Printf("Client Request Handler: Closing handler")
	if h.clientResultsConsumer != nil {
		h.clientResultsConsumer.Close()
	}
	if h.completionExchange != nil {
		h.completionExchange.Close()
	}
	if h.resultsHandler != nil {
		h.resultsHandler.Close()
	}
}
