package shared

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
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// GroupedResult represents the grouped data by year, month, item_id
type GroupedResult struct {
	Year          int
	Month         int
	ItemID        string
	TotalQuantity int
	TotalSubtotal float64
	Count         int
}

// FinalResult represents the final aggregated result (aggregated by month)
type FinalResult struct {
	Year          int
	Month         int
	ItemID        string
	TotalQuantity int
	TotalSubtotal float64
	Count         int
}

// ReduceWorker processes chunks for a specific semester
type ReduceWorker struct {
	semester    Semester
	consumer    *workerqueue.QueueConsumer
	producer    *workerqueue.QueueMiddleware
	config      *middleware.ConnectionConfig
	groupedData map[string]*GroupedResult // Key: year-month-item_id, Value: aggregated data
	chunkCount  int                       // Track number of chunks received
	clientID    string                    // Store the client ID from incoming chunks
}

// NewReduceWorker creates a new reduce worker for a specific semester
func NewReduceWorker(semester Semester) *ReduceWorker {
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	// Create consumer for the specific semester queue
	queueName := GetQueueNameForSemester(semester)
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

	// Create producer for the top items queue (goes to top classification component)
	producer := workerqueue.NewMessageMiddlewareQueue(queues.Query2TopItemsQueue, config)
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
		semester:    semester,
		consumer:    consumer,
		producer:    producer,
		config:      config,
		groupedData: make(map[string]*GroupedResult),
		chunkCount:  0,
		clientID:    "", // Will be set when first chunk is processed
	}
}

// ProcessChunk processes a chunk immediately and aggregates data by item_id
func (rw *ReduceWorker) ProcessChunk(chunk *chunk.Chunk) error {
	log.Printf("Processing chunk %d for semester %s", chunk.ChunkNumber, rw.semester.String())

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

	// Aggregate data by item_id immediately
	rw.aggregateData(results)

	rw.chunkCount++
	log.Printf("Processed chunk %d, now have %d unique items (total chunks: %d)",
		chunk.ChunkNumber, len(rw.groupedData), rw.chunkCount)

	return nil
}

// parseCSVData parses CSV data from chunk (now with year and month)
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
		if len(record) < 6 {
			continue // Skip malformed records
		}

		year, err := strconv.Atoi(record[0])
		if err != nil {
			continue // Skip records with invalid year
		}

		month, err := strconv.Atoi(record[1])
		if err != nil {
			continue // Skip records with invalid month
		}

		quantity, err := strconv.Atoi(record[3])
		if err != nil {
			continue // Skip records with invalid quantity
		}

		subtotal, err := strconv.ParseFloat(record[4], 64)
		if err != nil {
			continue // Skip records with invalid subtotal
		}

		count, err := strconv.Atoi(record[5])
		if err != nil {
			continue // Skip records with invalid count
		}

		result := GroupedResult{
			Year:          year,
			Month:         month,
			ItemID:        record[2],
			TotalQuantity: quantity,
			TotalSubtotal: subtotal,
			Count:         count,
		}

		results = append(results, result)
	}

	return results, nil
}

// aggregateData aggregates data by year-month-item_id
func (rw *ReduceWorker) aggregateData(results []GroupedResult) {
	for _, result := range results {
		// Create composite key: year-month-itemID (e.g., "2024-03-ITM001")
		compositeKey := fmt.Sprintf("%d-%02d-%s", result.Year, result.Month, result.ItemID)

		// Get or create grouped result for this composite key
		if rw.groupedData[compositeKey] == nil {
			rw.groupedData[compositeKey] = &GroupedResult{
				Year:          result.Year,
				Month:         result.Month,
				ItemID:        result.ItemID,
				TotalQuantity: 0,
				TotalSubtotal: 0,
				Count:         0,
			}
		}

		// Aggregate data
		rw.groupedData[compositeKey].TotalQuantity += result.TotalQuantity
		rw.groupedData[compositeKey].TotalSubtotal += result.TotalSubtotal
		rw.groupedData[compositeKey].Count += result.Count
	}
}

// FinalizeResults sends the final aggregated results
func (rw *ReduceWorker) FinalizeResults() error {
	log.Printf("Finalizing results for semester %s with %d processed chunks and %d unique items",
		rw.semester.String(), rw.chunkCount, len(rw.groupedData))

	// Convert to final results (now with year and month from the data)
	finalResults := make([]FinalResult, 0, len(rw.groupedData))
	for _, grouped := range rw.groupedData {
		finalResult := FinalResult{
			Year:          grouped.Year,
			Month:         grouped.Month,
			ItemID:        grouped.ItemID,
			TotalQuantity: grouped.TotalQuantity,
			TotalSubtotal: grouped.TotalSubtotal,
			Count:         grouped.Count,
		}
		finalResults = append(finalResults, finalResult)
	}

	// Log detailed final results
	log.Printf("FINAL RESULTS for semester %s:", rw.semester.String())
	log.Printf("   Total unique items: %d", len(finalResults))

	// Show top 10 items by total quantity for debugging
	items := make([]FinalResult, len(finalResults))
	copy(items, finalResults)

	// Sort by total quantity (descending)
	for i := 0; i < len(items)-1; i++ {
		for j := i + 1; j < len(items); j++ {
			if items[i].TotalQuantity < items[j].TotalQuantity {
				items[i], items[j] = items[j], items[i]
			}
		}
	}

	// Show top 10 items
	topCount := 10
	if len(items) < topCount {
		topCount = len(items)
	}

	log.Printf("   Top %d items by quantity:", topCount)
	for i := 0; i < topCount; i++ {
		item := items[i]
		log.Printf("      %d. ItemID: %s | Quantity: %d | Subtotal: %.2f | Count: %d",
			i+1, item.ItemID, item.TotalQuantity, item.TotalSubtotal, item.Count)
	}

	// Calculate totals
	totalQuantity := 0
	totalSubtotal := 0.0
	totalCount := 0
	for _, result := range finalResults {
		totalQuantity += result.TotalQuantity
		totalSubtotal += result.TotalSubtotal
		totalCount += result.Count
	}

	log.Printf("   Grand totals: Quantity=%d, Subtotal=%.2f, Transactions=%d",
		totalQuantity, totalSubtotal, totalCount)

	// Convert to CSV
	csvData := rw.convertToCSV(finalResults)

	// Create chunk for final results
	// FileID format: S{semester}{last2digitsOfYear} - e.g., "S125" for S1-2025
	fileID := fmt.Sprintf("S%d%02d", rw.semester.Semester, rw.semester.Year%100)
	finalChunk := chunk.NewChunk(
		rw.clientID, // Use the actual client ID from the original request
		fileID,      // File ID (max 4 bytes)
		2,           // Query Type
		1,           // Chunk Number
		true,        // Is Last Chunk
		2,           // Step 2 for final results
		len(csvData),
		2, // Table ID 2 for transaction_items
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

	log.Printf("Successfully sent final results for semester %s: %d items (ClientID: %s)",
		rw.semester.String(), len(finalResults), rw.clientID)

	// Clear aggregated data to free memory
	rw.clearAggregatedData()

	return nil
}

// clearAggregatedData clears the aggregated data to free memory
func (rw *ReduceWorker) clearAggregatedData() {
	log.Printf("Clearing aggregated data for semester %s (%d items)", rw.semester.String(), len(rw.groupedData))
	rw.groupedData = make(map[string]*GroupedResult)
	rw.chunkCount = 0
	rw.clientID = "" // Reset client ID for next batch
}

// convertToCSV converts final results to CSV format
func (rw *ReduceWorker) convertToCSV(results []FinalResult) string {
	var csvBuilder strings.Builder

	// Write header (changed from semester to month)
	csvBuilder.WriteString("year,month,item_id,total_quantity,total_subtotal,count\n")

	// Write data rows
	for _, result := range results {
		csvBuilder.WriteString(fmt.Sprintf("%d,%d,%s,%d,%.2f,%d\n",
			result.Year,
			result.Month,
			result.ItemID,
			result.TotalQuantity,
			result.TotalSubtotal,
			result.Count,
		))
	}

	return csvBuilder.String()
}

// Start starts the reduce worker
func (rw *ReduceWorker) Start() {
	log.Printf("Starting Reduce Worker for semester %s...", rw.semester.String())

	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Printf("Reduce worker for semester %s started consuming messages", rw.semester.String())
		chunkCount := 0
		for delivery := range *consumeChannel {
			log.Printf("Received message for semester %s - Message size: %d bytes", rw.semester.String(), len(delivery.Body))
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
				log.Printf("Received last chunk for semester %s, finalizing results...", rw.semester.String())
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

	queueName := GetQueueNameForSemester(rw.semester)
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
