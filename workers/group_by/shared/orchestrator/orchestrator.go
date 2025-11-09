package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	"github.com/tp-distribuidos-2c2025/workers/shared"
)

// TerminationSignal represents a signal to terminate processing
type TerminationSignal struct {
	QueryType int
	ClientID  string
	Message   string
}

// GroupByOrchestrator manages the coordination of map-reduce operations
type GroupByOrchestrator struct {
	config              *OrchestratorConfig
	completionTracker   *shared.CompletionTracker
	chunkConsumer       *exchange.ExchangeConsumer
	terminationProducer *exchange.ExchangeMiddleware
	completionProducer  *workerqueue.QueueMiddleware // For sending completion chunks to next step
	fileProcessor       *FileProcessor               // For reading and converting grouped data files
}

// NewGroupByOrchestrator creates a new group by orchestrator
func NewGroupByOrchestrator(queryType int) (*GroupByOrchestrator, error) {
	log.Printf("NewGroupByOrchestrator: Query Type %d", queryType)
	config, err := NewOrchestratorConfig(queryType)
	if err != nil {
		return nil, fmt.Errorf("failed to create orchestrator config: %v", err)
	}

	log.Printf("NewGroupByOrchestrator: Worker ID %d", config.WorkerID)

	orchestrator := &GroupByOrchestrator{
		config:        config,
		fileProcessor: NewFileProcessor(queryType, config.WorkerID),
	}

	// Create completion tracker with callback to send termination signals
	trackerName := fmt.Sprintf("Query%d-Orchestrator-Worker%d", queryType, config.WorkerID)
	orchestrator.completionTracker = shared.NewCompletionTracker(trackerName, orchestrator.onClientCompleted)

	orchestrator.initializeQueues()
	return orchestrator, nil
}

// initializeQueues sets up all necessary queues and exchanges
func (gbo *GroupByOrchestrator) initializeQueues() {
	// Create consumer for chunk notifications from map workers (fanout exchange)
	exchangeName := queues.GetOrchestratorChunksExchangeName(gbo.config.QueryType)
	if exchangeName == "" {
		log.Fatalf("No orchestrator chunks exchange found for query type %d", gbo.config.QueryType)
	}

	gbo.chunkConsumer = exchange.NewExchangeConsumer(exchangeName, []string{}, gbo.config.RabbitMQConfig)
	if gbo.chunkConsumer == nil {
		log.Fatalf("Failed to create chunk notification consumer for exchange: %s", exchangeName)
	}

	// Declare the fanout exchange (workers will also declare it, but we ensure it exists)
	exchangeDeclarer := exchange.NewMessageMiddlewareExchange(exchangeName, []string{}, gbo.config.RabbitMQConfig)
	if exchangeDeclarer == nil {
		gbo.chunkConsumer.Close()
		log.Fatalf("Failed to create exchange declarer for exchange: %s", exchangeName)
	}
	if err := exchangeDeclarer.DeclareExchange("fanout", false, false, false, false); err != 0 {
		gbo.chunkConsumer.Close()
		exchangeDeclarer.Close()
		log.Fatalf("Failed to declare fanout exchange %s: %v", exchangeName, err)
	}
	exchangeDeclarer.Close()

	// Create fanout exchange producer for termination signals to map workers
	terminationExchangeName := getMapWorkerTerminationExchangeName(gbo.config.QueryType)
	gbo.terminationProducer = exchange.NewMessageMiddlewareExchange(terminationExchangeName, []string{}, gbo.config.RabbitMQConfig)
	if gbo.terminationProducer == nil {
		gbo.chunkConsumer.Close()
		log.Fatalf("Failed to create termination exchange producer")
	}

	// Declare the termination exchange
	if err := gbo.terminationProducer.DeclareExchange("fanout", false, false, false, false); err != 0 {
		gbo.chunkConsumer.Close()
		gbo.terminationProducer.Close()
		log.Fatalf("Failed to declare termination exchange: %v", err)
	}

	// Create completion chunk producer for next step
	completionQueueName := getCompletionQueueName(gbo.config.QueryType)
	gbo.completionProducer = workerqueue.NewMessageMiddlewareQueue(completionQueueName, gbo.config.RabbitMQConfig)
	if gbo.completionProducer == nil {
		gbo.chunkConsumer.Close()
		gbo.terminationProducer.Close()
		log.Fatalf("Failed to create completion chunk producer for queue: %s", completionQueueName)
	}

	log.Printf("Orchestrator initialized for Query %d, Worker %d (expected files will be inferred dynamically)",
		gbo.config.QueryType, gbo.config.WorkerID)
	log.Printf("Completion chunks will be sent to queue: %s", completionQueueName)
	log.Printf("Consuming chunk notifications from fanout exchange: %s", exchangeName)
}

// onClientCompleted is called when all files for a client are completed
func (gbo *GroupByOrchestrator) onClientCompleted(clientID string, clientStatus *shared.ClientStatus) {
	log.Printf("Client %s: All %d files completed! Reading grouped data files from worker %d and sending to next step...",
		clientID, clientStatus.TotalExpectedFiles, gbo.config.WorkerID)

	// Get all files for this client from this worker's volume
	files, err := gbo.fileProcessor.GetClientFiles(clientID)
	if err != nil {
		log.Printf("Failed to get files for client %s: %v", clientID, err)
		return
	}

	if len(files) == 0 {
		log.Printf("No files found for client %s in worker %d volume, skipping", clientID, gbo.config.WorkerID)
		gbo.completionTracker.ClearClientState(clientID)
		return
	}

	log.Printf("Client %s: Found %d partition files to process in worker %d volume", clientID, len(files), gbo.config.WorkerID)

	// Aggregate all partition files into a single CSV result
	var allCSVData strings.Builder
	for i, filePath := range files {
		// Read and convert file to CSV
		csvData, err := gbo.fileProcessor.ReadAndConvertFile(filePath)
		if err != nil {
			log.Printf("Failed to read/convert file %s: %v", filePath, err)
			continue
		}

		// Append CSV data (skip header for subsequent files)
		if i == 0 {
			allCSVData.WriteString(csvData)
		} else {
			// Skip header row for subsequent files
			lines := strings.Split(csvData, "\n")
			if len(lines) > 1 {
				allCSVData.WriteString(strings.Join(lines[1:], "\n"))
			}
		}
	}

	// Send single chunk with worker ID as chunk number
	// Chunk number = worker ID (1, 2, 3, ...)
	chunkNumber := gbo.config.WorkerID
	combinedCSV := allCSVData.String()

	// Send chunk to next step (IsLastChunk=true, IsLastFromTable=true as per requirements)
	if err := gbo.sendDataChunk(clientID, chunkNumber, combinedCSV, true); err != nil {
		log.Printf("Failed to send data chunk for client %s: %v", clientID, err)
		return
	}

	// Now that chunk has been sent, clean up files
	log.Printf("Client %s: Chunk %d sent, now cleaning up files...", clientID, chunkNumber)

	// Delete individual files
	for _, filePath := range files {
		if err := gbo.fileProcessor.DeleteFile(filePath); err != nil {
			log.Printf("Failed to delete file %s: %v", filePath, err)
			// Continue anyway - don't block on cleanup failures
		}
	}

	// Delete the entire client directory
	if err := gbo.fileProcessor.DeleteClientDirectory(clientID); err != nil {
		log.Printf("Failed to delete client directory for %s: %v", clientID, err)
		// Continue anyway - don't block on cleanup failures
	}

	// Clear client state from completion tracker
	gbo.completionTracker.ClearClientState(clientID)

	log.Printf("Client %s: Chunk %d sent and files cleaned up", clientID, chunkNumber)
}

// sendTerminationSignals sends termination signals to all map and reduce workers
func (gbo *GroupByOrchestrator) sendTerminationSignals(clientID string) {
	terminationSignal := TerminationSignal{
		QueryType: gbo.config.QueryType,
		ClientID:  clientID,
		Message:   fmt.Sprintf("All data processing completed for client %s", clientID),
	}

	// Serialize termination signal
	signalData, err := json.Marshal(terminationSignal)
	if err != nil {
		log.Printf("Failed to serialize termination signal: %v", err)
		return
	}

	// Send to map workers
	log.Printf("Sending termination signal to map workers for Query %d", gbo.config.QueryType)
	if err := gbo.terminationProducer.Send(signalData, []string{}); err != 0 {
		log.Printf("Failed to send termination signal to map workers: %v", err)
	}

	log.Printf("Termination signals sent successfully for Query %d", gbo.config.QueryType)
}

// sendDataChunk sends a data chunk with CSV data to the next processing step
func (gbo *GroupByOrchestrator) sendDataChunk(clientID string, chunkNumber int, csvData string, isLastChunk bool) error {
	// Create chunk with grouped CSV data
	// Chunk number = worker ID (1, 2, 3, ...)
	// IsLastChunk and IsLastFromTable are always true (each orchestrator sends one chunk)
	dataChunk := chunk.NewChunk(
		clientID,                   // ClientID
		"01",                       // FileID - use "01" so completion tracker can parse it
		byte(gbo.config.QueryType), // QueryType
		chunkNumber,                // ChunkNumber = WorkerID
		true,                       // IsLastChunk - always true (per requirements)
		true,                       // IsLastFromTable - always true (per requirements)
		len(csvData),               // ChunkSize
		0,                          // TableID
		csvData,                    // ChunkData - CSV formatted data
	)

	// Create chunk message
	chunkMessage := chunk.NewChunkMessage(dataChunk)

	// Serialize the chunk
	serializedChunk, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		return fmt.Errorf("failed to serialize data chunk: %v", err)
	}

	// Send to the appropriate queue based on query type
	if sendErr := gbo.completionProducer.Send(serializedChunk); sendErr != 0 {
		return fmt.Errorf("failed to send data chunk: error code %v", sendErr)
	}

	completionQueueName := getCompletionQueueName(gbo.config.QueryType)
	log.Printf("Sent chunk %d for client %s to queue %s (IsLastChunk=%t, %d bytes)",
		chunkNumber, clientID, completionQueueName, isLastChunk, len(csvData))

	return nil
}

// Start starts the orchestrator
func (gbo *GroupByOrchestrator) Start() {
	log.Printf("Starting Group By Orchestrator for Query %d, Worker %d...", gbo.config.QueryType, gbo.config.WorkerID)

	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Printf("Orchestrator Worker %d started consuming chunk notifications from fanout exchange", gbo.config.WorkerID)

		for delivery := range *consumeChannel {
			log.Printf("Orchestrator Worker %d received chunk notification: %d bytes", gbo.config.WorkerID, len(delivery.Body))

			// Deserialize chunk notification using protocol
			message, err := deserializer.Deserialize(delivery.Body)
			if err != nil {
				log.Printf("Failed to deserialize chunk notification: %v", err)
				delivery.Ack(false)
				continue
			}

			notification, ok := message.(*signals.ChunkNotification)
			if !ok {
				log.Printf("Received message is not a ChunkNotification: %T", message)
				delivery.Ack(false)
				continue
			}

			// Process the notification using the completion tracker
			// All orchestrators receive all notifications via fanout, so they all track the same state
			if err := gbo.completionTracker.ProcessChunkNotification(notification); err != nil {
				log.Printf("Failed to process chunk notification: %v", err)
				delivery.Ack(false)
				continue
			}

			// Acknowledge the message
			delivery.Ack(false)
		}
		done <- nil
	}

	exchangeName := queues.GetOrchestratorChunksExchangeName(gbo.config.QueryType)
	if err := gbo.chunkConsumer.StartConsuming(onMessageCallback); err != 0 {
		log.Fatalf("Failed to start consuming chunk notifications: %v", err)
	}

	log.Printf("Successfully started consuming from fanout exchange: %s", exchangeName)
}

// Close closes the orchestrator
func (gbo *GroupByOrchestrator) Close() {
	if gbo.chunkConsumer != nil {
		gbo.chunkConsumer.Close()
	}
	if gbo.terminationProducer != nil {
		gbo.terminationProducer.Close()
	}
	if gbo.completionProducer != nil {
		gbo.completionProducer.Close()
	}
}

// getMapWorkerTerminationExchangeName returns the exchange name for map worker termination
func getMapWorkerTerminationExchangeName(queryType int) string {
	return fmt.Sprintf("query%d-map-termination", queryType)
}

// getCompletionQueueName returns the queue name for completion chunks based on query type
func getCompletionQueueName(queryType int) string {
	switch queryType {
	case 2:
		return queues.Query2GroupByResultsQueue
	case 3:
		return queues.Query3GroupByResultsQueue
	case 4:
		return queues.Query4GroupByResultsQueue
	default:
		log.Fatalf("Unknown query type: %d", queryType)
		return ""
	}
}
