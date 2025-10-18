package shared

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
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
	GroupedData        map[string]*GroupedResult // Key: store_id, Value: aggregated data
	ChunkCount         int                       // Track number of chunks received
	IsCompleted        bool                      // Whether this client's processing is completed
	MapWorkerSignals   map[string]bool           // Track which map workers have sent completion signals
	ExpectedMapWorkers int                       // Number of map workers expected for this query
}

// ReduceWorker processes chunks for a specific semester
type ReduceWorker struct {
	semester           Semester
	consumer           *exchange.ExchangeConsumer
	producer           *workerqueue.QueueMiddleware
	config             *middleware.ConnectionConfig
	clientData         map[string]*ClientData // Key: clientID, Value: client-specific data
	expectedMapWorkers int                    // Number of map workers expected for this query
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

	// Get expected number of map workers for this query
	expectedMapWorkers := getExpectedMapWorkersForQuery(3) // Query 3

	return &ReduceWorker{
		semester:           semester,
		consumer:           consumer,
		producer:           producer,
		config:             config,
		clientData:         make(map[string]*ClientData),
		expectedMapWorkers: expectedMapWorkers,
	}
}

// ProcessChunk processes a chunk immediately and aggregates data by store_id for a specific client
func (rw *ReduceWorker) ProcessChunk(chunk *chunk.Chunk) error {
	log.Printf("[query3-reduce-%s] [CLI%s | Q3] Processing chunk %d for semester %s", rw.semester.String(), chunk.ClientID, chunk.ChunkNumber, rw.semester.String())

	// Initialize client data if not exists
	if rw.clientData[chunk.ClientID] == nil {
		rw.clientData[chunk.ClientID] = &ClientData{
			GroupedData:        make(map[string]*GroupedResult),
			ChunkCount:         0,
			IsCompleted:        false,
			MapWorkerSignals:   make(map[string]bool),
			ExpectedMapWorkers: rw.expectedMapWorkers,
		}
		log.Printf("[query3-reduce-%s] [CLI%s | Q3] Initialized data for client (expecting %d map workers)", rw.semester.String(), chunk.ClientID, rw.expectedMapWorkers)
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
	log.Printf("[query3-reduce-%s] [CLI%s | Q3] Processed chunk %d, now have %d unique stores (total chunks: %d)",
		rw.semester.String(), chunk.ClientID, chunk.ChunkNumber, len(clientData.GroupedData), clientData.ChunkCount)

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

	log.Printf("[query3-reduce-%s] [CLI%s | Q3] Finalizing results with %d processed chunks and %d unique stores",
		rw.semester.String(), clientID, clientData.ChunkCount, len(clientData.GroupedData))

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
	log.Printf("[query3-reduce-%s] [CLI%s | Q3] FINAL RESULTS:", rw.semester.String(), clientID)
	log.Printf("[query3-reduce-%s] [CLI%s | Q3]   Total unique stores: %d", rw.semester.String(), clientID, len(finalResults))

	if len(finalResults) == 0 {
		log.Printf("[query3-reduce-%s] [CLI%s | Q3]   No data processed for this client in this semester", rw.semester.String(), clientID)
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

	log.Printf("[query3-reduce-%s] [CLI%s | Q3]   Top %d stores by total final amount:", rw.semester.String(), clientID, topCount)
	for i := 0; i < topCount; i++ {
		store := stores[i]
		log.Printf("[query3-reduce-%s] [CLI%s | Q3]      %d. StoreID: %s | TotalFinalAmount: %.2f | Count: %d",
			rw.semester.String(), clientID, i+1, store.StoreID, store.TotalFinalAmount, store.Count)
	}

	// Calculate totals
	totalFinalAmount := 0.0
	totalCount := 0
	for _, result := range finalResults {
		totalFinalAmount += result.TotalFinalAmount
		totalCount += result.Count
	}

	log.Printf("[query3-reduce-%s] [CLI%s | Q3]   Grand totals: TotalFinalAmount=%.2f, Transactions=%d",
		rw.semester.String(), clientID, totalFinalAmount, totalCount)

	// Convert to CSV
	csvData := rw.convertToCSV(finalResults)

	// Log the CSV data for debugging
	if len(csvData) > 0 {
		log.Printf("[query3-reduce-%s] [CLI%s | Q3] CSV data (first 500 chars):\n%s",
			rw.semester.String(), clientID, csvData[:min(500, len(csvData))])
	} else {
		log.Printf("[query3-reduce-%s] [CLI%s | Q3] Empty CSV data", rw.semester.String(), clientID)
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

	log.Printf("[query3-reduce-%s] [CLI%s | Q3] Successfully sent final results: %d stores",
		rw.semester.String(), clientID, len(finalResults))

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
		log.Printf("[query3-reduce-%s] [CLI%s | Q3] Clearing aggregated data (%d stores)",
			rw.semester.String(), clientID, len(clientData.GroupedData))
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
	log.Printf("[query3-reduce-%s] [Q3] Starting Reduce Worker for semester %s (expecting %d map workers)...", rw.semester.String(), rw.semester.String(), rw.expectedMapWorkers)

	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Printf("[query3-reduce-%s] [Q3] Started consuming messages", rw.semester.String())
		chunkCount := 0
		for delivery := range *consumeChannel {
			log.Printf("[query3-reduce-%s] [Q3] Received message - Size: %d bytes", rw.semester.String(), len(delivery.Body))
			log.Printf("[query3-reduce-%s] [Q3] Message preview: %s", rw.semester.String(), string(delivery.Body[:min(100, len(delivery.Body))]))
			// Deserialize the chunk message
			message, err := deserializer.Deserialize(delivery.Body)
			if err != nil {
				log.Printf("[query3-reduce-%s] [Q3] Failed to deserialize chunk: %v", rw.semester.String(), err)
				delivery.Ack(false)
				continue
			}

			// Check if it's a Chunk message or GroupByCompletionSignal
			switch msg := message.(type) {
			case *chunk.Chunk:
				// Process the chunk
				chunkCount++
				log.Printf("[query3-reduce-%s] [CLI%s | Q3] Processing chunk %d", rw.semester.String(), msg.ClientID, chunkCount)
				err = rw.ProcessChunk(msg)
				if err != nil {
					log.Printf("[query3-reduce-%s] [CLI%s | Q3] Failed to process chunk: %v", rw.semester.String(), msg.ClientID, err)
					delivery.Ack(false)
					continue
				}
			case *signals.GroupByCompletionSignal:
				// Handle completion signal from specific map worker
				log.Printf("[query3-reduce-%s] [CLI%s | Q3] Received GroupByCompletionSignal from map worker %s: %s",
					rw.semester.String(), msg.ClientID, msg.MapWorkerID, msg.Message)

				// Track this map worker's completion signal
				rw.trackMapWorkerCompletion(msg.ClientID, msg.MapWorkerID)
			default:
				log.Printf("[query3-reduce-%s] [Q3] Received unknown message type: %T", rw.semester.String(), message)
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

// trackMapWorkerCompletion tracks completion signal from a specific map worker
func (rw *ReduceWorker) trackMapWorkerCompletion(clientID, mapWorkerID string) {
	clientData := rw.clientData[clientID]
	if clientData == nil {
		log.Printf("[query3-reduce-%s] [CLI%s | Q3] Received completion signal for unknown client", rw.semester.String(), clientID)
		return
	}

	// Mark this map worker as completed
	clientData.MapWorkerSignals[mapWorkerID] = true

	log.Printf("[query3-reduce-%s] [CLI%s | Q3] Map worker %s completed (%d/%d map workers completed)",
		rw.semester.String(), clientID, mapWorkerID, len(clientData.MapWorkerSignals), clientData.ExpectedMapWorkers)

	// Check if all map workers have completed
	if len(clientData.MapWorkerSignals) >= clientData.ExpectedMapWorkers {
		log.Printf("[query3-reduce-%s] [CLI%s | Q3] ✅ All %d map workers completed, finalizing results...",
			rw.semester.String(), clientID, clientData.ExpectedMapWorkers)

		// Finalize results for this client
		err := rw.FinalizeResultsForClient(clientID)
		if err != nil {
			log.Printf("[query3-reduce-%s] [CLI%s | Q3] ❌ Failed to finalize results: %v", rw.semester.String(), clientID, err)
		}
	} else {
		log.Printf("[query3-reduce-%s] [CLI%s | Q3] ⏳ Waiting for %d more map workers to complete",
			rw.semester.String(), clientID, clientData.ExpectedMapWorkers-len(clientData.MapWorkerSignals))
	}
}

// getExpectedMapWorkersForQuery returns the expected number of map workers for a query
func getExpectedMapWorkersForQuery(queryType int) int {
	// Try to get from environment variable first
	envVar := fmt.Sprintf("QUERY%d_EXPECTED_MAP_WORKERS", queryType)
	if envValue := os.Getenv(envVar); envValue != "" {
		if count, err := strconv.Atoi(envValue); err == nil && count > 0 {
			log.Printf("[Q%d] Using expected map workers from environment: %s=%d", queryType, envVar, count)
			return count
		} else {
			log.Printf("[Q%d] Invalid value for %s: %s, using default", queryType, envVar, envValue)
		}
	}

	// Fallback to reasonable defaults
	switch queryType {
	case 2:
		return 1 // Query 2 has 1 map worker
	case 3:
		return 1 // Query 3 has 1 map worker
	case 4:
		return 1 // Query 4 has 1 map worker
	default:
		return 1 // Default to 1 map worker
	}
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
