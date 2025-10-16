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

// GroupedResult represents the grouped data by store_id (already grouped by semester)
type GroupedResult struct {
	StoreID          string
	TotalFinalAmount float64
	Count            int
}

// FinalResult represents the final aggregated result
type FinalResult struct {
	Year             int
	Semester         int
	StoreID          string
	TotalFinalAmount float64
	Count            int
}

// ClientData represents the data for a specific client
type ClientData struct {
	GroupedData map[string]*GroupedResult // Key: store_id, Value: aggregated data
	ChunkCount  int                       // Track number of chunks received
	IsCompleted bool                      // Whether this client's processing is completed
}

// ReduceWorker processes chunks for a specific semester
type ReduceWorker struct {
	semester   Semester
	consumer   *exchange.ExchangeConsumer
	producer   *workerqueue.QueueMiddleware
	config     *middleware.ConnectionConfig
	clientData map[string]*ClientData // Key: clientID, Value: client-specific data
}

// NewReduceWorker creates a new reduce worker for a specific semester
func NewReduceWorker(semester Semester) *ReduceWorker {
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	// Create consumer for the specific semester queue via topic exchange
	queueName := GetQueueNameForSemester(semester)
	routingKey := queues.GetQuery3RoutingKey(semester.Year, semester.Semester)
	if routingKey == "" {
		log.Fatalf("No routing key found for semester %d-%d", semester.Year, semester.Semester)
	}

	// Create exchange consumer for the specific routing key
	consumer := exchange.NewExchangeConsumer(queues.Query3MapReduceExchange, []string{routingKey}, config)
	if consumer == nil {
		log.Fatalf("Failed to create exchange consumer for routing key: %s", routingKey)
	}

	// Declare the topic exchange
	exchangeDeclarer := exchange.NewMessageMiddlewareExchange(queues.Query3MapReduceExchange, []string{}, config)
	if exchangeDeclarer == nil {
		consumer.Close()
		log.Fatalf("Failed to create exchange declarer for exchange: %s", queues.Query3MapReduceExchange)
	}
	if err := exchangeDeclarer.DeclareExchange("topic", false, false, false, false); err != 0 {
		consumer.Close()
		exchangeDeclarer.Close()
		log.Fatalf("Failed to declare topic exchange %s: %v", queues.Query3MapReduceExchange, err)
	}
	exchangeDeclarer.Close() // Close the declarer as we don't need it anymore

	// Declare the reduce queue for this routing key
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
	producer := workerqueue.NewMessageMiddlewareQueue(queues.StoreIdChunkQueue, config)
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
		semester:   semester,
		consumer:   consumer,
		producer:   producer,
		config:     config,
		clientData: make(map[string]*ClientData),
	}
}

// ProcessChunk processes a chunk immediately and aggregates data by store_id for a specific client
func (rw *ReduceWorker) ProcessChunk(chunk *chunk.Chunk) error {
	log.Printf("Processing chunk %d for semester %s, client %s", chunk.ChunkNumber, rw.semester.String(), chunk.ClientID)

	// Initialize client data if not exists
	if rw.clientData[chunk.ClientID] == nil {
		rw.clientData[chunk.ClientID] = &ClientData{
			GroupedData: make(map[string]*GroupedResult),
			ChunkCount:  0,
			IsCompleted: false,
		}
		log.Printf("Initialized data for client: %s", chunk.ClientID)
	}

	clientData := rw.clientData[chunk.ClientID]

	// Parse CSV data from chunk
	results, err := rw.parseCSVData(chunk.ChunkData)
	if err != nil {
		return fmt.Errorf("failed to parse CSV data: %v", err)
	}

	// Aggregate data by store_id immediately
	rw.aggregateDataForClient(chunk.ClientID, results)

	clientData.ChunkCount++
	log.Printf("Processed chunk %d for client %s, now have %d unique stores (total chunks: %d)",
		chunk.ChunkNumber, chunk.ClientID, len(clientData.GroupedData), clientData.ChunkCount)

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
		if len(record) < 5 {
			continue // Skip malformed records
		}

		totalFinalAmount, err := strconv.ParseFloat(record[3], 64)
		if err != nil {
			continue // Skip records with invalid total final amount
		}

		count, err := strconv.Atoi(record[4])
		if err != nil {
			continue // Skip records with invalid count
		}

		result := GroupedResult{
			StoreID:          record[2],
			TotalFinalAmount: totalFinalAmount,
			Count:            count,
		}

		results = append(results, result)
	}

	return results, nil
}

// aggregateDataForClient aggregates data by store_id for a specific client
func (rw *ReduceWorker) aggregateDataForClient(clientID string, results []GroupedResult) {
	clientData := rw.clientData[clientID]

	for _, result := range results {
		storeID := result.StoreID

		// Get or create grouped result for this store_id
		if clientData.GroupedData[storeID] == nil {
			clientData.GroupedData[storeID] = &GroupedResult{
				StoreID:          storeID,
				TotalFinalAmount: 0,
				Count:            0,
			}
		}

		// Aggregate data
		clientData.GroupedData[storeID].TotalFinalAmount += result.TotalFinalAmount
		clientData.GroupedData[storeID].Count += result.Count
	}
}

// FinalizeResultsForClient sends the final aggregated results for a specific client
func (rw *ReduceWorker) FinalizeResultsForClient(clientID string) error {
	clientData := rw.clientData[clientID]

	// Initialize client data if it doesn't exist (no chunks were processed for this client)
	if clientData == nil {
		clientData = &ClientData{
			GroupedData: make(map[string]*GroupedResult),
			ChunkCount:  0,
			IsCompleted: false,
		}
		rw.clientData[clientID] = clientData
		log.Printf("No data found for client %s, initializing empty results", clientID)
	}

	log.Printf("Finalizing results for client %s, semester %s with %d processed chunks and %d unique stores",
		clientID, rw.semester.String(), clientData.ChunkCount, len(clientData.GroupedData))

	// Convert to final results
	finalResults := make([]FinalResult, 0, len(clientData.GroupedData))
	for _, grouped := range clientData.GroupedData {
		finalResult := FinalResult{
			Year:             rw.semester.Year,
			Semester:         rw.semester.Semester,
			StoreID:          grouped.StoreID,
			TotalFinalAmount: grouped.TotalFinalAmount,
			Count:            grouped.Count,
		}
		finalResults = append(finalResults, finalResult)
	}

	// Log detailed final results
	log.Printf("FINAL RESULTS for client %s, semester %s:", clientID, rw.semester.String())
	log.Printf("   Total unique stores: %d", len(finalResults))

	if len(finalResults) == 0 {
		log.Printf("   No data processed for this client in this semester")
	}

	// Show top 10 stores by total final amount for debugging
	stores := make([]FinalResult, len(finalResults))
	copy(stores, finalResults)

	// Sort by total final amount (descending)
	for i := 0; i < len(stores)-1; i++ {
		for j := i + 1; j < len(stores); j++ {
			if stores[i].TotalFinalAmount < stores[j].TotalFinalAmount {
				stores[i], stores[j] = stores[j], stores[i]
			}
		}
	}

	// Show top 10 stores
	topCount := 10
	if len(stores) < topCount {
		topCount = len(stores)
	}

	log.Printf("   Top %d stores by total final amount:", topCount)
	for i := 0; i < topCount; i++ {
		store := stores[i]
		log.Printf("      %d. StoreID: %s | TotalFinalAmount: %.2f | Count: %d",
			i+1, store.StoreID, store.TotalFinalAmount, store.Count)
	}

	// Calculate totals
	totalFinalAmount := 0.0
	totalCount := 0
	for _, result := range finalResults {
		totalFinalAmount += result.TotalFinalAmount
		totalCount += result.Count
	}

	log.Printf("   Grand totals: TotalFinalAmount=%.2f, Transactions=%d",
		totalFinalAmount, totalCount)

	// Convert to CSV
	csvData := rw.convertToCSV(finalResults)

	// Log the CSV data for debugging
	if len(csvData) > 0 {
		log.Printf("CSV data for client %s, semester %s (first 500 chars):\n%s",
			clientID, rw.semester.String(), csvData[:min(500, len(csvData))])
	} else {
		log.Printf("Empty CSV data for client %s, semester %s", clientID, rw.semester.String())
	}

	// Create chunk for final results
	// FileID format: S{semester}{last2digitsOfYear} - e.g., "S125" for S1-2025
	fileID := fmt.Sprintf("S%d%02d", rw.semester.Semester, rw.semester.Year%100)
	finalChunk := chunk.NewChunk(
		clientID, // Use the specific client ID
		fileID,   // File ID (max 4 bytes)
		3,        // Query Type 3
		1,        // Chunk Number
		true,     // Is Last Chunk
		2,        // Step 2 for final results
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

	log.Printf("Successfully sent final results for client %s, semester %s: %d stores",
		clientID, rw.semester.String(), len(finalResults))

	// Mark client as completed
	clientData.IsCompleted = true

	// Clear aggregated data for this client to free memory
	rw.clearClientData(clientID)

	return nil
}

// clearClientData clears the aggregated data for a specific client to free memory
func (rw *ReduceWorker) clearClientData(clientID string) {
	clientData := rw.clientData[clientID]
	if clientData != nil {
		log.Printf("Clearing aggregated data for client %s, semester %s (%d stores)",
			clientID, rw.semester.String(), len(clientData.GroupedData))
		clientData.GroupedData = make(map[string]*GroupedResult)
		clientData.ChunkCount = 0
	}
}

// convertToCSV converts final results to CSV format
// Always returns valid CSV with at least headers, even for empty results
func (rw *ReduceWorker) convertToCSV(results []FinalResult) string {
	var csvBuilder strings.Builder

	// Write header (always present, even for empty results)
	csvBuilder.WriteString("year,semester,store_id,total_final_amount,count\n")

	// Write data rows
	for _, result := range results {
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

			// Check if it's a Chunk message or GroupByCompletionSignal
			switch msg := message.(type) {
			case *chunk.Chunk:
				// Process the chunk
				err = rw.ProcessChunk(msg)
				if err != nil {
					log.Printf("Failed to process chunk: %v", err)
					delivery.Ack(false)
					continue
				}

				chunkCount++
			case *signals.GroupByCompletionSignal:
				// Handle completion signal for specific client
				log.Printf("Received GroupByCompletionSignal for client %s, semester %s from map worker %s: %s",
					msg.ClientID, rw.semester.String(), msg.MapWorkerID, msg.Message)

				// Finalize results for this specific client
				err = rw.FinalizeResultsForClient(msg.ClientID)
				if err != nil {
					log.Printf("Failed to finalize results for client %s on completion signal: %v", msg.ClientID, err)
				}

				log.Printf("Reduce worker for semester %s completed processing for client %s", rw.semester.String(), msg.ClientID)
				delivery.Ack(false)
				// Continue processing other clients
			default:
				log.Printf("Received unknown message type: %T", message)
				delivery.Ack(false)
				continue
			}

			// Acknowledge the message
			delivery.Ack(false)
		}
		done <- nil
	}

	routingKey := queues.GetQuery3RoutingKey(rw.semester.Year, rw.semester.Semester)
	log.Printf("About to start consuming from exchange: %s with routing key: %s",
		queues.Query3MapReduceExchange, routingKey)

	// Add a timeout to detect if no messages are received
	go func() {
		time.Sleep(10 * time.Second)
		log.Printf("WARNING: No messages received in 10 seconds for exchange: %s with routing key: %s",
			queues.Query3MapReduceExchange, routingKey)
	}()

	if err := rw.consumer.StartConsuming(onMessageCallback); err != 0 {
		log.Fatalf("Failed to start consuming: %v", err)
	}
	log.Printf("Successfully started consuming from exchange: %s with routing key: %s",
		queues.Query3MapReduceExchange, routingKey)
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
