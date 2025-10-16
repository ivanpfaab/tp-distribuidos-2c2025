package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TransactionItem represents a single transaction item record
type TransactionItem struct {
	TransactionID string
	ItemID        string
	Quantity      int
	UnitPrice     float64
	Subtotal      float64
	CreatedAt     time.Time
}

// GroupedResult represents the grouped data by year, semester, item_id
type GroupedResult struct {
	Year          int
	Semester      int
	ItemID        string
	TotalQuantity int
	TotalSubtotal float64
	Count         int
}

// MapWorker processes chunks and groups by year, semester, item_id
type MapWorker struct {
	consumer         *workerqueue.QueueConsumer
	exchangeProducer *exchange.ExchangeMiddleware
	routingKeys      map[string]string // Map queue names to routing keys
	orchestratorComm *OrchestratorCommunicator
	config           *middleware.ConnectionConfig
	completedClients map[string]bool // Track which clients have completed processing
}

// NewMapWorker creates a new map worker instance
func NewMapWorker() *MapWorker {
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	// Create consumer for input chunks
	consumer := workerqueue.NewQueueConsumer(queues.Query2MapQueue, config)
	if consumer == nil {
		log.Fatal("Failed to create consumer")
	}

	// Declare the input queue before consuming
	inputQueueDeclarer := workerqueue.NewMessageMiddlewareQueue(queues.Query2MapQueue, config)
	if inputQueueDeclarer == nil {
		consumer.Close()
		log.Fatal("Failed to create input queue declarer")
	}
	if err := inputQueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		inputQueueDeclarer.Close()
		log.Fatalf("Failed to declare input queue '%s': %v", queues.Query2MapQueue, err)
	}
	inputQueueDeclarer.Close() // Close the declarer as we don't need it anymore

	// Create routing keys map for each semester
	routingKeys := make(map[string]string)
	semesters := GetAllSemesters()

	for _, semester := range semesters {
		routingKey := queues.GetQuery2RoutingKey(semester.Year, semester.Semester)
		if routingKey == "" {
			log.Fatalf("No routing key found for semester %d-%d", semester.Year, semester.Semester)
		}
		routingKeys[GetQueueNameForSemester(semester)] = routingKey
		log.Printf("Mapped queue %s to routing key: %s", GetQueueNameForSemester(semester), routingKey)
	}

	routingKeysArray := make([]string, 0)
	for _, routingKey := range routingKeys {
		routingKeysArray = append(routingKeysArray, routingKey)
	}

	// Create topic exchange producer for all semesters
	exchangeProducer := exchange.NewMessageMiddlewareExchange(queues.Query2MapReduceExchange, routingKeysArray, config)
	if exchangeProducer == nil {
		consumer.Close()
		log.Fatal("Failed to create topic exchange producer")
	}

	// Declare the topic exchange
	if err := exchangeProducer.DeclareExchange("topic", false, false, false, false); err != 0 {
		consumer.Close()
		exchangeProducer.Close()
		log.Fatalf("Failed to declare topic exchange '%s': %v", queues.Query2MapReduceExchange, err)
	}

	// Create orchestrator communicator
	orchestratorComm := NewOrchestratorCommunicator("map-worker-1", config)

	return &MapWorker{
		consumer:         consumer,
		exchangeProducer: exchangeProducer,
		routingKeys:      routingKeys,
		orchestratorComm: orchestratorComm,
		config:           config,
		completedClients: make(map[string]bool),
	}
}

// ProcessChunk processes a single chunk and groups the data
func (mw *MapWorker) ProcessChunk(chunk *chunk.Chunk) error {
	log.Printf("Processing chunk %d from file %s", chunk.ChunkNumber, chunk.FileID)

	// Parse CSV data from chunk
	transactions, err := mw.parseCSVData(chunk.ChunkData)
	if err != nil {
		return fmt.Errorf("failed to parse CSV data: %v", err)
	}

	// Group transactions by item_id (all transactions in chunk are from same semester)
	groupedData, semester := mw.groupTransactions(transactions)

	log.Printf("Grouped data for chunk %d: semester %s with %d items",
		chunk.ChunkNumber, semester.String(), len(groupedData))

	// Send grouped data to appropriate reduce queue for this semester
	err = mw.sendToReduceQueue(chunk, groupedData, semester)
	if err != nil {
		return fmt.Errorf("failed to send to reduce queues: %v", err)
	}

	// Notify orchestrator about chunk processing
	err = mw.orchestratorComm.NotifyChunkProcessed(chunk)
	if err != nil {
		log.Printf("Failed to notify orchestrator about chunk %d: %v", chunk.ChunkNumber, err)
		// Don't return error here as the main processing was successful
	}

	log.Printf("Successfully processed chunk %d, sent to reduce queue for semester %s",
		chunk.ChunkNumber, semester.String())

	return nil
}

// parseCSVData parses CSV data from chunk
func (mw *MapWorker) parseCSVData(csvData string) ([]TransactionItem, error) {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) < 2 {
		return []TransactionItem{}, nil
	}

	// Skip header row
	transactions := make([]TransactionItem, 0, len(records)-1)

	for _, record := range records[1:] {
		if len(record) < 6 {
			continue // Skip malformed records
		}

		quantity, err := strconv.Atoi(record[2])
		if err != nil {
			continue // Skip records with invalid quantity
		}

		unitPrice, err := strconv.ParseFloat(record[3], 64)
		if err != nil {
			continue // Skip records with invalid unit price
		}

		subtotal, err := strconv.ParseFloat(record[4], 64)
		if err != nil {
			continue // Skip records with invalid subtotal
		}

		createdAt, err := time.Parse("2006-01-02 15:04:05", record[5])
		if err != nil {
			// Try alternative date format
			createdAt, err = time.Parse("2006-01-02", record[5])
			if err != nil {
				continue // Skip records with invalid date
			}
		}

		transaction := TransactionItem{
			TransactionID: record[0],
			ItemID:        record[1],
			Quantity:      quantity,
			UnitPrice:     unitPrice,
			Subtotal:      subtotal,
			CreatedAt:     createdAt,
		}

		transactions = append(transactions, transaction)
	}

	fmt.Printf("Parsed %d transactions\n", len(transactions))

	return transactions, nil
}

// groupTransactions groups transactions by item_id (all transactions in chunk are from same semester)
func (mw *MapWorker) groupTransactions(transactions []TransactionItem) (map[string]*GroupedResult, Semester) {
	groupedData := make(map[string]*GroupedResult)
	var semester Semester

	// Process first transaction to determine semester (all transactions share the same semester)
	if len(transactions) > 0 {
		semester = GetSemesterFromDate(transactions[0].CreatedAt)

		// Skip invalid semesters (outside our range)
		if !IsValidSemester(semester) {
			log.Printf("Invalid semester %d-%d, skipping chunk", semester.Year, semester.Semester)
			return groupedData, semester
		}
	}

	for _, transaction := range transactions {
		// Get or create grouped result for this item_id
		itemID := transaction.ItemID
		if groupedData[itemID] == nil {
			groupedData[itemID] = &GroupedResult{
				Year:          semester.Year,
				Semester:      semester.Semester,
				ItemID:        itemID,
				TotalQuantity: 0,
				TotalSubtotal: 0,
				Count:         0,
			}
		}

		// Aggregate data
		groupedData[itemID].TotalQuantity += transaction.Quantity
		groupedData[itemID].TotalSubtotal += transaction.Subtotal
		groupedData[itemID].Count++
	}

	return groupedData, semester
}

// sendToReduceQueue sends grouped data to the appropriate reduce queue for a specific semester
func (mw *MapWorker) sendToReduceQueue(originalChunk *chunk.Chunk, groupedData map[string]*GroupedResult, semester Semester) error {
	queueName := GetQueueNameForSemester(semester)
	routingKey, exists := mw.routingKeys[queueName]
	if !exists {
		return fmt.Errorf("no routing key found for queue: %s", queueName)
	}

	// Convert grouped data to CSV
	csvData := mw.convertToCSV(groupedData)

	// Create new chunk for reduce queue
	reduceChunk := chunk.NewChunk(
		originalChunk.ClientID,
		originalChunk.FileID,
		originalChunk.QueryType,
		originalChunk.ChunkNumber,
		originalChunk.IsLastChunk,
		originalChunk.Step, // Step 1 for reduce workers
		len(csvData),
		originalChunk.TableID, // Table ID 2 for transaction_items
		csvData,
	)

	// Serialize and send chunk
	chunkMsg := chunk.NewChunkMessage(reduceChunk)
	serializedData, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
		return fmt.Errorf("failed to serialize chunk for routing key %s: %v", routingKey, err)
	}

	log.Printf("Sending to routing key %s (queue %s): %d bytes, %d item groups",
		routingKey, queueName, len(serializedData), len(groupedData))
	log.Printf("Serialized data preview: %s", string(serializedData[:min(100, len(serializedData))]))

	sendErr := mw.exchangeProducer.Send(serializedData, []string{routingKey})
	if sendErr != 0 {
		return fmt.Errorf("failed to send to routing key %s: error code %v", routingKey, sendErr)
	}

	log.Printf("Successfully sent %d item groups to reduce queue via routing key: %s", len(groupedData), routingKey)

	return nil
}

// convertToCSV converts grouped data to CSV format
func (mw *MapWorker) convertToCSV(itemGroups map[string]*GroupedResult) string {
	var csvBuilder strings.Builder

	// Write header
	csvBuilder.WriteString("year,semester,item_id,total_quantity,total_subtotal,count\n")

	// Write data rows
	for _, result := range itemGroups {
		csvBuilder.WriteString(fmt.Sprintf("%d,%d,%s,%d,%.2f,%d\n",
			result.Year,
			result.Semester,
			result.ItemID,
			result.TotalQuantity,
			result.TotalSubtotal,
			result.Count,
		))
	}

	return csvBuilder.String()
}

// sendCompletionSignalToReduceWorkers sends a GroupByCompletionSignal to all reduce workers for a specific client
func (mw *MapWorker) sendCompletionSignalToReduceWorkers(clientID string) {
	semesters := GetAllSemesters()

	for _, semester := range semesters {
		queueName := GetQueueNameForSemester(semester)
		routingKey, exists := mw.routingKeys[queueName]
		if !exists {
			log.Printf("No routing key found for queue: %s", queueName)
			continue
		}

		// Create completion signal
		completionSignal := signals.NewGroupByCompletionSignal(
			2,              // Query Type 2
			clientID,       // Client ID
			"map-worker-1", // Map Worker ID. TODO: pass the map worker ID
			fmt.Sprintf("All data processing completed for client %s", clientID),
		)

		// Serialize completion signal
		signalData, err := signals.SerializeGroupByCompletionSignal(completionSignal)
		if err != nil {
			log.Printf("Failed to serialize completion signal for routing key %s: %v", routingKey, err)
			continue
		}

		// Send to reduce queue via topic exchange
		log.Printf("Sending completion signal to routing key %s (queue %s)", routingKey, queueName)
		sendErr := mw.exchangeProducer.Send(signalData, []string{routingKey})
		if sendErr != 0 {
			log.Printf("Failed to send completion signal to routing key %s: error code %v", routingKey, sendErr)
		} else {
			log.Printf("Successfully sent completion signal to routing key: %s", routingKey)
		}
	}
}

// Start starts the map worker
func (mw *MapWorker) Start() {
	log.Println("Starting Map Worker for Query 2...")

	// Start listening for termination signals
	go mw.orchestratorComm.StartTerminationListener(func(signal *TerminationSignal) {
		log.Printf("Map worker received termination signal for client %s: %s", signal.ClientID, signal.Message)
		mw.sendCompletionSignalToReduceWorkers(signal.ClientID)
		mw.completedClients[signal.ClientID] = true // TODO: remove this entry when the client is completed (cleanup)
	})

	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {

			// Deserialize the chunk message
			message, err := deserializer.Deserialize(delivery.Body)
			if err != nil {
				log.Printf("Failed to deserialize chunk: %v", err)
				delivery.Ack(false)
				continue
			}

			// Check if it's a Chunk message
			chunk, ok := message.(*chunk.Chunk)
			if !ok {
				log.Printf("Received non-chunk message: %T", message)
				delivery.Ack(false)
				continue
			}

			// Process only Query Type 2 chunks
			if chunk.QueryType != 2 {
				log.Printf("Received non-Query 2 chunk: QueryType=%d, skipping", chunk.QueryType)
				delivery.Ack(false)
				continue
			}

			// This should never happen, log error
			if mw.completedClients[chunk.ClientID] {
				log.Printf("Error: Client %s has already completed processing, skipping chunk", chunk.ClientID)
				delivery.Ack(false)
				continue
			}

			// Process the chunk
			err = mw.ProcessChunk(chunk)
			if err != nil {
				log.Printf("Failed to process chunk: %v", err)
				delivery.Ack(false)
				continue
			}

			// Acknowledge the message
			delivery.Ack(false)
		}
		done <- nil
	}

	if err := mw.consumer.StartConsuming(onMessageCallback); err != 0 {
		log.Fatalf("Failed to start consuming: %v", err)
	}

	// Keep the main thread alive to prevent the program from exiting
	// The consumer runs in a goroutine, so we need to block here
	select {}
}

// Close closes the map worker
func (mw *MapWorker) Close() {
	if mw.consumer != nil {
		mw.consumer.Close()
	}

	if mw.exchangeProducer != nil {
		mw.exchangeProducer.Close()
	}

	if mw.orchestratorComm != nil {
		mw.orchestratorComm.Close()
	}
}

func main() {
	mapWorker := NewMapWorker()
	defer mapWorker.Close()

	mapWorker.Start()

	// Keep the worker running
	select {}
}
