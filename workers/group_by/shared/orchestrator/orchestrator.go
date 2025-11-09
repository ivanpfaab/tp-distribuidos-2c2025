package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
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

// GroupByOrchestrator manages the coordination of map-reduce operations
type GroupByOrchestrator struct {
	config             *OrchestratorConfig
	completionTracker  *shared.CompletionTracker
	chunkConsumer      *exchange.ExchangeConsumer
	completionProducer *workerqueue.QueueMiddleware // For sending completion chunks to next step
	fileProcessor      *FileProcessor               // For reading and converting grouped data files
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

	// Create completion chunk producer for next step
	completionQueueName := getCompletionQueueName(gbo.config.QueryType)
	gbo.completionProducer = workerqueue.NewMessageMiddlewareQueue(completionQueueName, gbo.config.RabbitMQConfig)
	if gbo.completionProducer == nil {
		gbo.chunkConsumer.Close()
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
	combinedCSV, err := gbo.aggregateClientFiles(files)
	if err != nil {
		log.Printf("Failed to aggregate files for client %s: %v", clientID, err)
		return
	}

	// Send aggregated chunk to next step
	chunkNumber := gbo.config.WorkerID
	if err := gbo.sendAggregatedChunk(clientID, chunkNumber, combinedCSV); err != nil {
		log.Printf("Failed to send data chunk for client %s: %v", clientID, err)
		return
	}

	// Clean up files and clear state
	gbo.cleanupClientFiles(clientID, files, chunkNumber)
}

// aggregateClientFiles reads and aggregates all partition files into a single CSV
// This properly aggregates records with the same key across all partition files
func (gbo *GroupByOrchestrator) aggregateClientFiles(files []string) (string, error) {
	if len(files) == 0 {
		return "", fmt.Errorf("no files to aggregate")
	}

	// Determine query type and create appropriate grouper
	var grouper RecordGrouper
	switch gbo.config.QueryType {
	case 2:
		// For Q2, we need to extract year from the first partition file
		// All partition files for a worker should have the same year
		partition, err := gbo.fileProcessor.extractPartitionFromFilename(files[0])
		if err != nil {
			return "", fmt.Errorf("failed to extract partition from filename: %v", err)
		}
		year := gbo.fileProcessor.getYearFromPartition(partition)
		grouper = &Query2Grouper{year: year}
	case 3:
		// For Q3, we need to extract year and semester from the first partition file
		partition, err := gbo.fileProcessor.extractPartitionFromFilename(files[0])
		if err != nil {
			return "", fmt.Errorf("failed to extract partition from filename: %v", err)
		}
		year, semester := gbo.fileProcessor.getYearSemesterFromPartition(partition)
		grouper = &Query3Grouper{year: year, semester: semester}
	case 4:
		grouper = &Query4Grouper{}
	default:
		return "", fmt.Errorf("unsupported query type: %d", gbo.config.QueryType)
	}

	// Initialize aggregated data map
	var aggregatedData map[string]interface{}
	switch grouper.(type) {
	case *Query2Grouper:
		aggregatedData = make(map[string]interface{}, 500)
	case *Query3Grouper:
		aggregatedData = make(map[string]interface{}, 20)
	case *Query4Grouper:
		aggregatedData = make(map[string]interface{}, 10000)
	}

	// Read and aggregate data from all partition files
	for _, filePath := range files {
		if err := gbo.aggregatePartitionFile(filePath, grouper, aggregatedData); err != nil {
			log.Printf("Failed to aggregate partition file %s: %v", filePath, err)
			// Continue with other files
			continue
		}
	}

	// Format output using the grouper
	return grouper.FormatOutput(aggregatedData), nil
}

// aggregatePartitionFile reads a partition file and adds its records to the aggregated data map
func (gbo *GroupByOrchestrator) aggregatePartitionFile(filePath string, grouper RecordGrouper, aggregatedData map[string]interface{}) error {
	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer file.Close()

	// Create buffered CSV reader
	bufferedFile := bufio.NewReaderSize(file, 64*1024) // 64KB buffer
	reader := csv.NewReader(bufferedFile)
	reader.ReuseRecord = true

	// Skip header row
	_, err = reader.Read()
	if err != nil {
		if err == io.EOF {
			return nil // Empty file, nothing to aggregate
		}
		return fmt.Errorf("failed to read header from %s: %v", filePath, err)
	}

	// Read all records from this partition file and aggregate them
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break // Normal end of file
			}
			log.Printf("Error reading from %s: %v", filePath, err)
			break
		}

		if len(record) < grouper.GetMinFieldCount() {
			log.Printf("Skipping malformed record in %s: %v", filePath, record)
			continue
		}

		// Process record to get the aggregation key
		key, shouldContinue, err := grouper.ProcessRecord(record)
		if err != nil {
			log.Printf("Skipping record in %s: %v", filePath, err)
			continue
		}
		if !shouldContinue {
			continue
		}

		// Aggregate based on query type
		switch grouper.(type) {
		case *Query2Grouper:
			// Parse quantity and subtotal from the already-aggregated partition file
			// Record format: month,item_id,quantity,subtotal (from worker)
			quantity, _ := strconv.Atoi(strings.TrimSpace(record[2]))
			subtotal, _ := strconv.ParseFloat(strings.TrimSpace(record[3]), 64)

			// Aggregate across partition files
			if aggregatedData[key] == nil {
				aggregatedData[key] = &Query2AggregatedData{}
			}
			agg := aggregatedData[key].(*Query2AggregatedData)
			agg.TotalQuantity += quantity
			agg.TotalSubtotal += subtotal
			// Count represents number of partition files containing this key
			agg.Count++

		case *Query3Grouper:
			// Parse final_amount from the already-aggregated partition file
			// Record format: store_id,final_amount (from worker)
			finalAmount, _ := strconv.ParseFloat(strings.TrimSpace(record[1]), 64)

			// Aggregate across partition files
			if aggregatedData[key] == nil {
				aggregatedData[key] = &Query3AggregatedData{}
			}
			agg := aggregatedData[key].(*Query3AggregatedData)
			agg.TotalFinalAmount += finalAmount
			// Count represents number of partition files containing this key
			agg.Count++

		case *Query4Grouper:
			// For Q4, partition files contain user_id,store_id pairs
			// We just count occurrences across all partition files
			if count, ok := aggregatedData[key].(int); ok {
				aggregatedData[key] = count + 1
			} else {
				aggregatedData[key] = 1
			}
		}
	}

	return nil
}

// sendAggregatedChunk sends the aggregated CSV data as a chunk to the next processing step
func (gbo *GroupByOrchestrator) sendAggregatedChunk(clientID string, chunkNumber int, csvData string) error {
	// Send chunk to next step (IsLastChunk=true, IsLastFromTable=true as per requirements)
	return gbo.sendDataChunk(clientID, chunkNumber, csvData, true)
}

// cleanupClientFiles deletes all files and clears client state from the completion tracker
func (gbo *GroupByOrchestrator) cleanupClientFiles(clientID string, files []string, chunkNumber int) {
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
	if gbo.completionProducer != nil {
		gbo.completionProducer.Close()
	}
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
