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

// Transaction represents a single transaction record
type Transaction struct {
	TransactionID   string
	StoreID         string
	PaymentMethodID string
	VoucherID       string
	UserID          string
	OriginalAmount  float64
	DiscountApplied float64
	FinalAmount     float64
	CreatedAt       time.Time
}

// GroupedResult represents the grouped data by year, semester, store_id
type GroupedResult struct {
	Year             int
	Semester         int
	StoreID          string
	TotalFinalAmount float64
	Count            int
}

// MapWorker processes chunks and groups by year, semester, store_id
type MapWorker struct {
	consumer         *workerqueue.QueueConsumer
	exchangeProducer *exchange.ExchangeMiddleware
	routingKeys      map[string]string // Map queue names to routing keys
	orchestratorComm *OrchestratorCommunicator
	config           *middleware.ConnectionConfig
	completedClients map[string]bool // Track which clients have completed processing
}

// NewMapWorker creates a new map worker instance for Query 3
func NewMapWorker() *MapWorker {
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	// Create consumer for input chunks (Query 3 uses storeid-groupby-chunks)
	consumer := workerqueue.NewQueueConsumer(queues.Query3MapQueue, config)
	if consumer == nil {
		log.Fatal("Failed to create consumer")
	}

	// Declare the input queue before consuming
	inputQueueDeclarer := workerqueue.NewMessageMiddlewareQueue(queues.Query3MapQueue, config)
	if inputQueueDeclarer == nil {
		consumer.Close()
		log.Fatal("Failed to create input queue declarer")
	}
	if err := inputQueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		inputQueueDeclarer.Close()
		log.Fatalf("Failed to declare input queue '%s': %v", queues.Query3MapQueue, err)
	}
	inputQueueDeclarer.Close() // Close the declarer as we don't need it anymore

	// Create routing keys mapping
	routingKeys := make(map[string]string)
	semesters := GetAllSemesters()
	for _, semester := range semesters {
		queueName := GetQueueNameForSemester(semester)
		routingKey := queues.GetQuery3RoutingKey(semester.Year, semester.Semester)
		if routingKey == "" {
			consumer.Close()
			log.Fatalf("No routing key found for semester %d-%d", semester.Year, semester.Semester)
		}
		routingKeys[queueName] = routingKey
		log.Printf("Mapped queue %s to routing key: %s", queueName, routingKey)
	}

	routingKeysArray := make([]string, 0)
	for _, routingKey := range routingKeys {
		routingKeysArray = append(routingKeysArray, routingKey)
	}

	// Create exchange producer for topic exchange
	exchangeProducer := exchange.NewMessageMiddlewareExchange(queues.Query3MapReduceExchange, routingKeysArray, config)
	if exchangeProducer == nil {
		consumer.Close()
		log.Fatalf("Failed to create exchange producer for exchange: %s", queues.Query3MapReduceExchange)
	}

	// Declare the topic exchange
	if err := exchangeProducer.DeclareExchange("topic", false, false, false, false); err != 0 {
		consumer.Close()
		exchangeProducer.Close()
		log.Fatalf("Failed to declare topic exchange %s: %v", queues.Query3MapReduceExchange, err)
	}

	// Create orchestrator communicator
	orchestratorComm := NewOrchestratorCommunicator("query3-map-worker", config)
	if orchestratorComm == nil {
		consumer.Close()
		exchangeProducer.Close()
		log.Fatalf("Failed to create orchestrator communicator")
	}

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

	// Notify orchestrator about chunk processing
	if err := mw.orchestratorComm.NotifyChunkProcessed(chunk); err != nil {
		log.Printf("Failed to notify orchestrator about chunk %d: %v", chunk.ChunkNumber, err)
		// Continue processing even if notification fails
	}

	// Parse CSV data from chunk
	transactions, err := mw.parseCSVData(chunk.ChunkData)
	if err != nil {
		return fmt.Errorf("failed to parse CSV data: %v", err)
	}

	// Group transactions by year, semester, store_id (all transactions in chunk are from same semester)
	groupedData, semester := mw.groupTransactions(transactions)

	log.Printf("Grouped data for chunk %d: semester %s (%d stores)",
		chunk.ChunkNumber, semester.String(), len(groupedData))

	// Send grouped data to appropriate reduce queue
	err = mw.sendToReduceQueue(chunk, groupedData, semester)
	if err != nil {
		return fmt.Errorf("failed to send to reduce queue: %v", err)
	}

	log.Printf("Successfully processed chunk %d, sent to reduce queue for semester %s",
		chunk.ChunkNumber, semester.String())

	return nil
}

// parseCSVData parses CSV data from chunk
func (mw *MapWorker) parseCSVData(csvData string) ([]Transaction, error) {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) < 2 {
		return []Transaction{}, nil
	}

	// Skip header row
	transactions := make([]Transaction, 0, len(records)-1)

	for _, record := range records[1:] {
		if len(record) < 9 {
			continue // Skip malformed records
		}

		originalAmount, err := strconv.ParseFloat(record[5], 64)
		if err != nil {
			continue // Skip records with invalid original amount
		}

		discountApplied, err := strconv.ParseFloat(record[6], 64)
		if err != nil {
			continue // Skip records with invalid discount
		}

		finalAmount, err := strconv.ParseFloat(record[7], 64)
		if err != nil {
			continue // Skip records with invalid final amount
		}

		createdAt, err := time.Parse("2006-01-02 15:04:05", record[8])
		if err != nil {
			// Try alternative date format
			createdAt, err = time.Parse("2006-01-02", record[8])
			if err != nil {
				continue // Skip records with invalid date
			}
		}

		transaction := Transaction{
			TransactionID:   record[0],
			StoreID:         record[1],
			PaymentMethodID: record[2],
			VoucherID:       record[3],
			UserID:          record[4],
			OriginalAmount:  originalAmount,
			DiscountApplied: discountApplied,
			FinalAmount:     finalAmount,
			CreatedAt:       createdAt,
		}

		transactions = append(transactions, transaction)
	}

	fmt.Printf("Parsed %d transactions\n", len(transactions))

	return transactions, nil
}

// groupTransactions groups transactions by store_id (all transactions in chunk are from same semester)
func (mw *MapWorker) groupTransactions(transactions []Transaction) (map[string]*GroupedResult, Semester) {
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
		// Get or create grouped result for this store_id
		storeID := transaction.StoreID
		if groupedData[storeID] == nil {
			groupedData[storeID] = &GroupedResult{
				Year:             semester.Year,
				Semester:         semester.Semester,
				StoreID:          storeID,
				TotalFinalAmount: 0,
				Count:            0,
			}
		}

		// Aggregate data
		groupedData[storeID].TotalFinalAmount += transaction.FinalAmount
		groupedData[storeID].Count++
	}

	return groupedData, semester
}

// sendToReduceQueue sends grouped data to appropriate reduce queue via topic exchange
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
		originalChunk.TableID, // Table ID 3 for transactions
		csvData,
	)

	// Serialize and send chunk
	chunkMsg := chunk.NewChunkMessage(reduceChunk)
	serializedData, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
		return fmt.Errorf("failed to serialize chunk for routing key %s: %v", routingKey, err)
	}

	log.Printf("Sending to routing key %s (queue %s): %d bytes, %d store groups",
		routingKey, queueName, len(serializedData), len(groupedData))
	log.Printf("Serialized data preview: %s", string(serializedData[:min(100, len(serializedData))]))

	sendErr := mw.exchangeProducer.Send(serializedData, []string{routingKey})
	if sendErr != 0 {
		return fmt.Errorf("failed to send to routing key %s: error code %v", routingKey, sendErr)
	}

	log.Printf("Successfully sent %d store groups to reduce queue via routing key: %s", len(groupedData), routingKey)

	return nil
}

// convertToCSV converts grouped data to CSV format
func (mw *MapWorker) convertToCSV(storeGroups map[string]*GroupedResult) string {
	var csvBuilder strings.Builder

	// Write header
	csvBuilder.WriteString("year,semester,store_id,total_final_amount,count\n")

	// Write data rows
	for _, result := range storeGroups {
		csvBuilder.WriteString(fmt.Sprintf("%d,%d,%s,%.2f,%d\n",
			result.Year,
			result.Semester,
			result.StoreID,
			result.TotalFinalAmount,
			result.Count,
		))
	}

	return csvBuilder.String()
}

// Start starts the map worker
func (mw *MapWorker) Start() {
	log.Println("Starting Map Worker for Query 3...")

	// Start termination signal listener
	go mw.orchestratorComm.StartTerminationListener(mw.onTerminationSignal)

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

			// Process only Query Type 3 chunks
			if chunk.QueryType != 3 {
				log.Printf("Received non-Query 3 chunk: QueryType=%d, skipping", chunk.QueryType)
				delivery.Ack(false)
				continue
			}

			// Skip chunks for completed clients
			if mw.completedClients[chunk.ClientID] {
				log.Printf("Error: Skipping chunk for completed client: %s", chunk.ClientID)
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

// onTerminationSignal handles termination signals from the orchestrator
func (mw *MapWorker) onTerminationSignal(signal *TerminationSignal) {
	log.Printf("Map worker received termination signal for Query %d, Client %s: %s",
		signal.QueryType, signal.ClientID, signal.Message)

	// Mark client as completed
	mw.completedClients[signal.ClientID] = true

	// Send completion signal to reduce workers
	mw.sendCompletionSignalToReduceWorkers(signal.ClientID)
}

// sendCompletionSignalToReduceWorkers sends completion signal to all reduce workers
func (mw *MapWorker) sendCompletionSignalToReduceWorkers(clientID string) {
	log.Printf("Sending completion signal to reduce workers for client: %s", clientID)

	// Create completion signal
	completionSignal := signals.NewGroupByCompletionSignal(
		3, // Query Type 3
		clientID,
		"query3-map-worker",
		"Query 3 processing completed",
	)

	// Serialize completion signal
	signalData, err := signals.SerializeGroupByCompletionSignal(completionSignal)
	if err != nil {
		log.Printf("Failed to serialize completion signal: %v", err)
		return
	}

	// Send to all reduce workers via their routing keys
	semesters := GetAllSemesters()
	for _, semester := range semesters {
		queueName := GetQueueNameForSemester(semester)
		routingKey, exists := mw.routingKeys[queueName]
		if !exists {
			log.Printf("No routing key found for queue: %s", queueName)
			continue
		}

		log.Printf("Sending completion signal to routing key: %s", routingKey)
		sendErr := mw.exchangeProducer.Send(signalData, []string{routingKey})
		if sendErr != 0 {
			log.Printf("Failed to send completion signal to routing key %s: error code %v", routingKey, sendErr)
		} else {
			log.Printf("Successfully sent completion signal to routing key: %s", routingKey)
		}
	}
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
