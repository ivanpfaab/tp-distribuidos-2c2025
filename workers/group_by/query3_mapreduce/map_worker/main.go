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
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	groupbyshared "github.com/tp-distribuidos-2c2025/workers/group_by/shared"
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
	TransactionID  string
	StoreID        string
	PaymentMethodID string
	VoucherID      string
	UserID         string
	OriginalAmount float64
	DiscountApplied float64
	FinalAmount    float64
	CreatedAt      time.Time
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
	consumer     *workerqueue.QueueConsumer
	producers    map[string]*workerqueue.QueueMiddleware
	config       *middleware.ConnectionConfig
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
	consumer := workerqueue.NewQueueConsumer(groupbyshared.StoreIdGroupByChunkQueue, config)
	if consumer == nil {
		log.Fatal("Failed to create consumer")
	}

	// Declare the input queue before consuming
	inputQueueDeclarer := workerqueue.NewMessageMiddlewareQueue(groupbyshared.StoreIdGroupByChunkQueue, config)
	if inputQueueDeclarer == nil {
		consumer.Close()
		log.Fatal("Failed to create input queue declarer")
	}
	if err := inputQueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		inputQueueDeclarer.Close()
		log.Fatalf("Failed to declare input queue '%s': %v", groupbyshared.StoreIdGroupByChunkQueue, err)
	}
	inputQueueDeclarer.Close() // Close the declarer as we don't need it anymore

	// Create producers for each semester
	producers := make(map[string]*workerqueue.QueueMiddleware)
	semesters := GetAllSemesters()

	for _, semester := range semesters {
		queueName := GetQueueNameForSemester(semester)
		log.Printf("Creating producer for queue: %s", queueName)
		producer := workerqueue.NewMessageMiddlewareQueue(queueName, config)
		if producer == nil {
			log.Fatalf("Failed to create producer for queue: %s", queueName)
		}

		// Declare the queue before using it
		if err := producer.DeclareQueue(false, false, false, false); err != 0 {
			log.Fatalf("Failed to declare queue %s: %v", queueName, err)
		}

		producers[queueName] = producer
	}

	return &MapWorker{
		consumer:  consumer,
		producers: producers,
		config:    config,
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

	// Group transactions by year, semester, store_id
	groupedData := mw.groupTransactions(transactions)

	log.Printf("Grouped data for chunk %d: %d semesters", chunk.ChunkNumber, len(groupedData))
	for semester := range groupedData {
		log.Printf("  Semester: %s (%d stores)", semester.String(), len(groupedData[semester]))
	}

	// Send grouped data to appropriate reduce queues
	err = mw.sendToReduceQueues(chunk, groupedData)
	if err != nil {
		return fmt.Errorf("failed to send to reduce queues: %v", err)
	}

	log.Printf("Successfully processed chunk %d, sent to %d reduce queues",
		chunk.ChunkNumber, len(groupedData))

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

// groupTransactions groups transactions by year, semester, store_id
func (mw *MapWorker) groupTransactions(transactions []Transaction) map[Semester]map[string]*GroupedResult {
	groupedData := make(map[Semester]map[string]*GroupedResult)

	for _, transaction := range transactions {
		// Calculate semester from date
		semester := GetSemesterFromDate(transaction.CreatedAt)

		// Skip invalid semesters (outside our range)
		if !IsValidSemester(semester) {
			continue
		}

		// Initialize semester map if needed
		if groupedData[semester] == nil {
			groupedData[semester] = make(map[string]*GroupedResult)
		}

		// Get or create grouped result for this store_id
		storeID := transaction.StoreID
		if groupedData[semester][storeID] == nil {
			groupedData[semester][storeID] = &GroupedResult{
				Year:             semester.Year,
				Semester:         semester.Semester,
				StoreID:          storeID,
				TotalFinalAmount: 0,
				Count:            0,
			}
		}

		// Aggregate data
		groupedData[semester][storeID].TotalFinalAmount += transaction.FinalAmount
		groupedData[semester][storeID].Count++
	}

	return groupedData
}

// sendToReduceQueues sends grouped data to appropriate reduce queues
func (mw *MapWorker) sendToReduceQueues(originalChunk *chunk.Chunk, groupedData map[Semester]map[string]*GroupedResult) error {
	for semester, storeGroups := range groupedData {
		queueName := GetQueueNameForSemester(semester)
		producer, exists := mw.producers[queueName]
		if !exists {
			return fmt.Errorf("no producer found for queue: %s", queueName)
		}

		// Convert grouped data to CSV
		csvData := mw.convertToCSV(storeGroups)

		// Create new chunk for reduce queue
		reduceChunk := chunk.NewChunk(
			originalChunk.ClientID,
			originalChunk.FileID,
			originalChunk.QueryType,
			originalChunk.ChunkNumber,
			originalChunk.IsLastChunk,
			originalChunk.Step, // Step 1 for reduce workers
			len(csvData),
			originalChunk.TableID, // Table ID 1 for transactions
			csvData,
		)

		// Serialize and send chunk
		chunkMsg := chunk.NewChunkMessage(reduceChunk)
		serializedData, err := chunk.SerializeChunkMessage(chunkMsg)
		if err != nil {
			return fmt.Errorf("failed to serialize chunk for queue %s: %v", queueName, err)
		}

		log.Printf("Sending to queue %s: %d bytes, %d store groups", queueName, len(serializedData), len(storeGroups))
		log.Printf("Serialized data preview: %s", string(serializedData[:min(100, len(serializedData))]))

		sendErr := producer.Send(serializedData)
		if sendErr != 0 {
			return fmt.Errorf("failed to send to queue %s: error code %v", queueName, sendErr)
		}

		log.Printf("Successfully sent %d store groups to reduce queue: %s", len(storeGroups), queueName)
	}

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

	for _, producer := range mw.producers {
		if producer != nil {
			producer.Close()
		}
	}
}

func main() {
	mapWorker := NewMapWorker()
	defer mapWorker.Close()

	mapWorker.Start()

	// Keep the worker running
	select {}
}
