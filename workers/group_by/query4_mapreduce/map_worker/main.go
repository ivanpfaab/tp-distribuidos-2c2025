package main

import (
	"encoding/csv"
	"fmt"
	"log"
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

// GroupedResult represents the grouped data by user_id and store_id
type GroupedResult struct {
	UserID  string
	StoreID string
	Count   int
}

// MapWorker processes chunks and groups by user_id and store_id
type MapWorker struct {
	consumer *workerqueue.QueueConsumer
	producer *workerqueue.QueueMiddleware
	config   *middleware.ConnectionConfig
}

// NewMapWorker creates a new map worker instance for Query 4
func NewMapWorker() *MapWorker {
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	// Create consumer for input chunks (Query 4 uses query4-map-queue)
	consumer := workerqueue.NewQueueConsumer(groupbyshared.Query4MapQueue, config)
	if consumer == nil {
		log.Fatal("Failed to create consumer")
	}

	// Declare the input queue before consuming
	inputQueueDeclarer := workerqueue.NewMessageMiddlewareQueue(groupbyshared.Query4MapQueue, config)
	if inputQueueDeclarer == nil {
		consumer.Close()
		log.Fatal("Failed to create input queue declarer")
	}
	if err := inputQueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		inputQueueDeclarer.Close()
		log.Fatalf("Failed to declare input queue '%s': %v", groupbyshared.Query4MapQueue, err)
	}
	inputQueueDeclarer.Close() // Close the declarer as we don't need it anymore

	// Create producer for the reduce queue
	producer := workerqueue.NewMessageMiddlewareQueue(groupbyshared.Query4ReduceQueue, config)
	if producer == nil {
		consumer.Close()
		log.Fatal("Failed to create producer for reduce-q4-userid queue")
	}

	// Declare the reduce queue before using it
	if err := producer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		producer.Close()
		log.Fatalf("Failed to declare reduce queue: %v", err)
	}

	return &MapWorker{
		consumer: consumer,
		producer: producer,
		config:   config,
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

	// Group transactions by user_id and store_id
	groupedData := mw.groupTransactions(transactions)

	log.Printf("Grouped data for chunk %d: %d user-store combinations", chunk.ChunkNumber, len(groupedData))

	// Send grouped data to reduce queue
	err = mw.sendToReduceQueue(chunk, groupedData)
	if err != nil {
		return fmt.Errorf("failed to send to reduce queue: %v", err)
	}

	log.Printf("Successfully processed chunk %d, sent to reduce queue", chunk.ChunkNumber)

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

		// For Query 4, we only need transaction_id, store_id, user_id, and created_at
		transaction := Transaction{
			TransactionID: record[0],
			StoreID:       record[1],
			UserID:        record[4],
		}

		transactions = append(transactions, transaction)
	}

	fmt.Printf("Parsed %d transactions\n", len(transactions))

	return transactions, nil
}

// groupTransactions groups transactions by user_id and store_id
func (mw *MapWorker) groupTransactions(transactions []Transaction) map[string]*GroupedResult {
	groupedData := make(map[string]*GroupedResult)

	for _, transaction := range transactions {
		// Create composite key: user_id + "|" + store_id
		key := transaction.UserID + "|" + transaction.StoreID

		// Get or create grouped result for this key
		if groupedData[key] == nil {
			groupedData[key] = &GroupedResult{
				UserID:  transaction.UserID,
				StoreID: transaction.StoreID,
				Count:   0,
			}
		}

		// Increment count
		groupedData[key].Count++
	}

	return groupedData
}

// sendToReduceQueue sends grouped data to the reduce queue
func (mw *MapWorker) sendToReduceQueue(originalChunk *chunk.Chunk, groupedData map[string]*GroupedResult) error {
	// Convert grouped data to CSV
	csvData := mw.convertToCSV(groupedData)

	// Create new chunk for reduce queue
	reduceChunk := chunk.NewChunk(
		originalChunk.ClientID,
		originalChunk.FileID,
		originalChunk.QueryType,
		originalChunk.ChunkNumber,
		originalChunk.IsLastChunk,
		originalChunk.Step, // Step 1 for reduce worker
		len(csvData),
		originalChunk.TableID, // Table ID 1 for transactions
		csvData,
	)

	// Serialize and send chunk
	chunkMsg := chunk.NewChunkMessage(reduceChunk)
	serializedData, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
		return fmt.Errorf("failed to serialize chunk: %v", err)
	}

	log.Printf("Sending to reduce queue: %d bytes, %d user-store groups", len(serializedData), len(groupedData))
	log.Printf("Serialized data preview: %s", string(serializedData[:min(100, len(serializedData))]))

	sendErr := mw.producer.Send(serializedData)
	if sendErr != 0 {
		return fmt.Errorf("failed to send to reduce queue: error code %v", sendErr)
	}

	log.Printf("Successfully sent %d user-store groups to reduce queue", len(groupedData))

	return nil
}

// convertToCSV converts grouped data to CSV format
func (mw *MapWorker) convertToCSV(groupedData map[string]*GroupedResult) string {
	var csvBuilder strings.Builder

	// Write header
	csvBuilder.WriteString("user_id,store_id,count\n")

	// Write data rows
	for _, result := range groupedData {
		csvBuilder.WriteString(fmt.Sprintf("%s,%s,%d\n",
			result.UserID,
			result.StoreID,
			result.Count,
		))
	}

	return csvBuilder.String()
}

// Start starts the map worker
func (mw *MapWorker) Start() {
	log.Println("Starting Map Worker for Query 4...")

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

			// Process only Query Type 4 chunks
			if chunk.QueryType != 4 {
				log.Printf("Received non-Query 4 chunk: QueryType=%d, skipping", chunk.QueryType)
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

	if mw.producer != nil {
		mw.producer.Close()
	}
}

func main() {
	mapWorker := NewMapWorker()
	defer mapWorker.Close()

	mapWorker.Start()

	// Keep the worker running
	select {}
}
