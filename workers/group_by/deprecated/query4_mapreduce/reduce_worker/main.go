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

// GroupedResult represents the grouped data by user_id and store_id
type GroupedResult struct {
	UserID  string
	StoreID string
	Count   int
}

// ClientData represents the data for a specific client
type ClientData struct {
	GroupedData map[string]*GroupedResult // Key: "user_id|store_id", Value: aggregated data
	ChunkCount  int                       // Track number of chunks received
	IsCompleted bool                      // Whether this client's processing is completed
}

// ReduceWorker processes chunks and aggregates by user_id and store_id
type ReduceWorker struct {
	consumer   *exchange.ExchangeConsumer
	producer   *workerqueue.QueueMiddleware
	config     *middleware.ConnectionConfig
	clientData map[string]*ClientData // Key: clientID, Value: client-specific data
}

// NewReduceWorker creates a new reduce worker for Query 4
func NewReduceWorker() *ReduceWorker {
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	// Get routing key for Query 4
	routingKey := queues.GetQuery4RoutingKey()
	if routingKey == "" {
		log.Fatalf("No routing key found for Query 4")
	}

	// Create exchange consumer for the specific routing key
	consumer := exchange.NewExchangeConsumer(queues.Query4MapReduceExchange, []string{routingKey}, config)
	if consumer == nil {
		log.Fatalf("Failed to create exchange consumer for routing key: %s", routingKey)
	}

	// Declare the topic exchange
	exchangeDeclarer := exchange.NewMessageMiddlewareExchange(queues.Query4MapReduceExchange, []string{}, config)
	if exchangeDeclarer == nil {
		consumer.Close()
		log.Fatalf("Failed to create exchange declarer for exchange: %s", queues.Query4MapReduceExchange)
	}
	if err := exchangeDeclarer.DeclareExchange("topic", false, false, false, false); err != 0 {
		consumer.Close()
		exchangeDeclarer.Close()
		log.Fatalf("Failed to declare topic exchange %s: %v", queues.Query4MapReduceExchange, err)
	}
	exchangeDeclarer.Close()

	// Create producer for the final results queue
	producer := workerqueue.NewMessageMiddlewareQueue(queues.Query4TopUsersQueue, config)
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
		consumer:   consumer,
		producer:   producer,
		config:     config,
		clientData: make(map[string]*ClientData),
	}
}

// ProcessChunk processes a chunk immediately and aggregates data by user_id and store_id
func (rw *ReduceWorker) ProcessChunk(chunk *chunk.Chunk) error {
	log.Printf("Processing chunk %d for Query 4, Client: %s", chunk.ChunkNumber, chunk.ClientID)

	// Initialize client data if not exists
	if rw.clientData[chunk.ClientID] == nil {
		rw.clientData[chunk.ClientID] = &ClientData{
			GroupedData: make(map[string]*GroupedResult),
			ChunkCount:  0,
			IsCompleted: false,
		}
	}

	clientData := rw.clientData[chunk.ClientID]

	// Parse CSV data from chunk
	results, err := rw.parseCSVData(chunk.ChunkData)
	if err != nil {
		return fmt.Errorf("failed to parse CSV data: %v", err)
	}

	// Aggregate data by user_id and store_id immediately
	rw.aggregateDataForClient(chunk.ClientID, results)

	clientData.ChunkCount++
	log.Printf("Processed chunk %d for client %s, now have %d unique user-store combinations (total chunks: %d)",
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
func (rw *ReduceWorker) aggregateDataForClient(clientID string, results []GroupedResult) {
	clientData := rw.clientData[clientID]

	for _, result := range results {
		// Skip results with empty user_id
		if result.UserID == "" || strings.TrimSpace(result.UserID) == "" {
			continue
		}

		// Create composite key: user_id + "|" + store_id
		key := result.UserID + "|" + result.StoreID

		// Get or create grouped result for this key
		if clientData.GroupedData[key] == nil {
			clientData.GroupedData[key] = &GroupedResult{
				UserID:  result.UserID,
				StoreID: result.StoreID,
				Count:   0,
			}
		}

		// Aggregate data
		clientData.GroupedData[key].Count += result.Count
	}
}

// FinalizeResultsForClient sends the final aggregated results for a specific client
func (rw *ReduceWorker) FinalizeResultsForClient(clientID string) error {
	clientData, exists := rw.clientData[clientID]
	if !exists {
		// Initialize empty client data if no data was processed for this client
		clientData = &ClientData{
			GroupedData: make(map[string]*GroupedResult),
			ChunkCount:  0,
			IsCompleted: true,
		}
		rw.clientData[clientID] = clientData
	}

	log.Printf("Finalizing results for Query 4, Client %s with %d processed chunks and %d unique user-store combinations",
		clientID, clientData.ChunkCount, len(clientData.GroupedData))

	// Convert to final results
	finalResults := make([]GroupedResult, 0, len(clientData.GroupedData))
	for _, grouped := range clientData.GroupedData {
		finalResults = append(finalResults, *grouped)
	}

	// Log detailed final results
	log.Printf("FINAL RESULTS for Query 4, Client %s:", clientID)
	log.Printf("   Total unique user-store combinations: %d", len(finalResults))


	// Convert to CSV
	csvData := rw.convertToCSV(finalResults)

	// Create chunk for final results
	finalChunk := chunk.NewChunk(
		clientID, // Use the specific client ID
		"Q4",     // File ID
		4,        // Query Type 4
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

	log.Printf("Successfully sent final results for Query 4: %d user-store combinations (ClientID: %s)",
		len(finalResults), clientID)

	// Clear client data to free memory
	rw.clearClientData(clientID)

	return nil
}

// clearClientData clears the aggregated data for a specific client to free memory
func (rw *ReduceWorker) clearClientData(clientID string) {
	clientData, exists := rw.clientData[clientID]
	if exists {
		log.Printf("Clearing aggregated data for Query 4, Client %s (%d user-store combinations)",
			clientID, len(clientData.GroupedData))
		delete(rw.clientData, clientID)
	}
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
		for delivery := range *consumeChannel {
			log.Printf("Received message for Query 4 - Message size: %d bytes", len(delivery.Body))
			log.Printf("Message body preview: %s", string(delivery.Body[:min(100, len(delivery.Body))]))

			// Deserialize the message
			message, err := deserializer.Deserialize(delivery.Body)
			if err != nil {
				log.Printf("Failed to deserialize message: %v", err)
				delivery.Ack(false)
				continue
			}

			// Check if it's a GroupByCompletionSignal
			if signal, ok := message.(*signals.GroupByCompletionSignal); ok {
				log.Printf("Received GroupByCompletionSignal for Query 4, Client %s: %s",
					signal.ClientID, signal.Message)

				// Finalize results for this client
				err = rw.FinalizeResultsForClient(signal.ClientID)
				if err != nil {
					log.Printf("Failed to finalize results for client %s: %v", signal.ClientID, err)
				}

				delivery.Ack(false)
				continue
			}

			// Check if it's a Chunk message
			chunk, ok := message.(*chunk.Chunk)
			if !ok {
				log.Printf("Received unknown message type: %T", message)
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

			// Acknowledge the message
			delivery.Ack(false)
		}
		done <- nil
	}

	routingKey := queues.GetQuery4RoutingKey()
	log.Printf("About to start consuming from exchange: %s with routing key: %s",
		queues.Query4MapReduceExchange, routingKey)

	// Add a timeout to detect if no messages are received
	go func() {
		time.Sleep(10 * time.Second)
		log.Printf("WARNING: No messages received in 10 seconds for exchange: %s with routing key: %s",
			queues.Query4MapReduceExchange, routingKey)
	}()

	if err := rw.consumer.StartConsuming(onMessageCallback); err != 0 {
		log.Fatalf("Failed to start consuming: %v", err)
	}
	log.Printf("Successfully started consuming from exchange: %s with routing key: %s",
		queues.Query4MapReduceExchange, routingKey)
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
