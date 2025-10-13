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

// GroupedResult represents the grouped data by user_id and store_id
type GroupedResult struct {
	UserID  string
	StoreID string
	Count   int
}

// ReduceWorker processes chunks and aggregates by user_id and store_id
type ReduceWorker struct {
	consumer    *workerqueue.QueueConsumer
	producer    *workerqueue.QueueMiddleware
	config      *middleware.ConnectionConfig
	groupedData map[string]*GroupedResult // Key: "user_id|store_id", Value: aggregated data
	chunkCount  int                       // Track number of chunks received
	clientID    string                    // Store the client ID from incoming chunks
}

// NewReduceWorker creates a new reduce worker for Query 4
func NewReduceWorker() *ReduceWorker {
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	// Create consumer for the reduce queue
	queueName := groupbyshared.Query4ReduceQueue
	consumer := workerqueue.NewQueueConsumer(queueName, config)
	if consumer == nil {
		log.Fatalf("Failed to create consumer for queue: %s", queueName)
	}

	// Declare the reduce queue before consuming
	queueDeclarer := workerqueue.NewMessageMiddlewareQueue(queueName, config)
	if queueDeclarer == nil {
		consumer.Close()
		log.Fatalf("Failed to create queue declarer for queue: %s", queueName)
	}
	if err := queueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		queueDeclarer.Close()
		log.Fatalf("Failed to declare reduce queue %s: %v", queueName, err)
	}
	queueDeclarer.Close() // Close the declarer as we don't need it anymore

	// Create producer for the final results queue
	producer := workerqueue.NewMessageMiddlewareQueue(groupbyshared.Query4GroupByResultsQueue, config)
	if producer == nil {
		consumer.Close()
		log.Fatalf("Failed to create producer for final results queue")
	}

	// Declare the final results queue
	if err := producer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		producer.Close()
		log.Fatalf("Failed to declare final results queue: %v", err)
	}

	return &ReduceWorker{
		consumer:    consumer,
		producer:    producer,
		config:      config,
		groupedData: make(map[string]*GroupedResult),
		chunkCount:  0,
		clientID:    "", // Will be set when first chunk is processed
	}
}

// ProcessChunk processes a chunk immediately and aggregates data by user_id and store_id
func (rw *ReduceWorker) ProcessChunk(chunk *chunk.Chunk) error {
	log.Printf("Processing chunk %d for Query 4", chunk.ChunkNumber)

	// Store client ID from the first chunk (all chunks should have the same client ID)
	if rw.clientID == "" {
		rw.clientID = chunk.ClientID
		log.Printf("Stored client ID: %s", rw.clientID)
	}

	// Parse CSV data from chunk
	results, err := rw.parseCSVData(chunk.ChunkData)
	if err != nil {
		return fmt.Errorf("failed to parse CSV data: %v", err)
	}

	// Aggregate data by user_id and store_id immediately
	rw.aggregateData(results)

	rw.chunkCount++
	log.Printf("Processed chunk %d, now have %d unique user-store combinations (total chunks: %d)",
		chunk.ChunkNumber, len(rw.groupedData), rw.chunkCount)

	return nil
}

// parseCSVData parses CSV data from chunk
func (rw *ReduceWorker) parseCSVData(csvData string) ([]GroupedResult, error) {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) < 2 {
		return []GroupedResult{}, nil
	}

	// Skip header row
	results := make([]GroupedResult, 0, len(records)-1)

	for _, record := range records[1:] {
		if len(record) < 3 {
			continue // Skip malformed records
		}

		count, err := strconv.Atoi(record[2])
		if err != nil {
			continue // Skip records with invalid count
		}

		result := GroupedResult{
			UserID:  record[0],
			StoreID: record[1],
			Count:   count,
		}

		results = append(results, result)
	}

	return results, nil
}

// aggregateData aggregates data by user_id and store_id
func (rw *ReduceWorker) aggregateData(results []GroupedResult) {
	for _, result := range results {
		// Create composite key: user_id + "|" + store_id
		key := result.UserID + "|" + result.StoreID

		// Get or create grouped result for this key
		if rw.groupedData[key] == nil {
			rw.groupedData[key] = &GroupedResult{
				UserID:  result.UserID,
				StoreID: result.StoreID,
				Count:   0,
			}
		}

		// Aggregate data
		rw.groupedData[key].Count += result.Count
	}
}

// FinalizeResults sends the final aggregated results
func (rw *ReduceWorker) FinalizeResults() error {
	log.Printf("Finalizing results for Query 4 with %d processed chunks and %d unique user-store combinations",
		rw.chunkCount, len(rw.groupedData))

	// Convert to final results
	finalResults := make([]GroupedResult, 0, len(rw.groupedData))
	for _, grouped := range rw.groupedData {
		finalResults = append(finalResults, *grouped)
	}

	// Log detailed final results
	log.Printf("FINAL RESULTS for Query 4:")
	log.Printf("   Total unique user-store combinations: %d", len(finalResults))

	// Show top 10 user-store combinations by count for debugging
	userStores := make([]GroupedResult, len(finalResults))
	copy(userStores, finalResults)

	// Sort by count (descending)
	for i := 0; i < len(userStores)-1; i++ {
		for j := i + 1; j < len(userStores); j++ {
			if userStores[i].Count < userStores[j].Count {
				userStores[i], userStores[j] = userStores[j], userStores[i]
			}
		}
	}

	// Show top 10 combinations
	topCount := 10
	if len(userStores) < topCount {
		topCount = len(userStores)
	}

	log.Printf("   Top %d user-store combinations by count:", topCount)
	for i := 0; i < topCount; i++ {
		combo := userStores[i]
		log.Printf("      %d. UserID: %s | StoreID: %s | Count: %d",
			i+1, combo.UserID, combo.StoreID, combo.Count)
	}

	// Calculate total count
	totalCount := 0
	for _, result := range finalResults {
		totalCount += result.Count
	}

	log.Printf("   Grand total: Transactions=%d", totalCount)

	// Convert to CSV
	csvData := rw.convertToCSV(finalResults)

	// Create chunk for final results
	finalChunk := chunk.NewChunk(
		rw.clientID, // Use the actual client ID from the original request
		"Q4",        // File ID
		4,           // Query Type 4
		1,           // Chunk Number
		true,        // Is Last Chunk
		2,           // Step 2 for final results
		len(csvData),
		1, // Table ID 1 for transactions
		csvData,
	)

	// Serialize and send chunk
	chunkMsg := chunk.NewChunkMessage(finalChunk)
	serializedData, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
		return fmt.Errorf("failed to serialize final chunk: %v", err)
	}

	sendErr := rw.producer.Send(serializedData)
	if sendErr != 0 {
		return fmt.Errorf("failed to send final results: error code %v", sendErr)
	}

	log.Printf("Successfully sent final results for Query 4: %d user-store combinations (ClientID: %s)",
		len(finalResults), rw.clientID)

	// Clear aggregated data to free memory
	rw.clearAggregatedData()

	return nil
}

// clearAggregatedData clears the aggregated data to free memory
func (rw *ReduceWorker) clearAggregatedData() {
	log.Printf("Clearing aggregated data for Query 4 (%d user-store combinations)", len(rw.groupedData))
	rw.groupedData = make(map[string]*GroupedResult)
	rw.chunkCount = 0
	rw.clientID = "" // Reset client ID for next batch
}

// convertToCSV converts final results to CSV format
func (rw *ReduceWorker) convertToCSV(results []GroupedResult) string {
	var csvBuilder strings.Builder

	// Write header
	csvBuilder.WriteString("user_id,store_id,count\n")

	// Write data rows
	for _, result := range results {
		csvBuilder.WriteString(fmt.Sprintf("%s,%s,%d\n",
			result.UserID,
			result.StoreID,
			result.Count,
		))
	}

	return csvBuilder.String()
}

// Start starts the reduce worker
func (rw *ReduceWorker) Start() {
	log.Printf("Starting Reduce Worker for Query 4...")

	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Printf("Reduce worker for Query 4 started consuming messages")
		chunkCount := 0
		for delivery := range *consumeChannel {
			log.Printf("Received message for Query 4 - Message size: %d bytes", len(delivery.Body))
			log.Printf("Message body preview: %s", string(delivery.Body[:min(100, len(delivery.Body))]))
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

			// Process the chunk immediately
			err = rw.ProcessChunk(chunk)
			if err != nil {
				log.Printf("Failed to process chunk: %v", err)
				delivery.Ack(false)
				continue
			}

			chunkCount++

			// If this is the last chunk, finalize results
			if chunk.IsLastChunk {
				log.Printf("Received last chunk for Query 4, finalizing results...")
				err = rw.FinalizeResults()
				if err != nil {
					log.Printf("Failed to finalize results: %v", err)
				}
			}

			// Acknowledge the message
			delivery.Ack(false)
		}
		done <- nil
	}

	queueName := groupbyshared.Query4ReduceQueue
	log.Printf("About to start consuming from queue: %s", queueName)

	// Add a timeout to detect if no messages are received
	go func() {
		time.Sleep(10 * time.Second)
		log.Printf("WARNING: No messages received in 10 seconds for queue: %s", queueName)
	}()

	if err := rw.consumer.StartConsuming(onMessageCallback); err != 0 {
		log.Fatalf("Failed to start consuming: %v", err)
	}
	log.Printf("Successfully started consuming from queue: %s", queueName)
}

// Close closes the reduce worker
func (rw *ReduceWorker) Close() {
	if rw.consumer != nil {
		rw.consumer.Close()
	}

	if rw.producer != nil {
		rw.producer.Close()
	}
}

func main() {
	reduceWorker := NewReduceWorker()
	defer reduceWorker.Close()

	log.Printf("Starting Reduce Worker for Query 4...")
	reduceWorker.Start()

	select {}
}
