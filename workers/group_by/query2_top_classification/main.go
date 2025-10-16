package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"strconv"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

// ItemRecord represents a single item aggregation
type ItemRecord struct {
	ItemID        string
	Year          int
	Month         int
	TotalQuantity int
	TotalSubtotal float64
}

// MonthTopItems stores top items for a specific month
type MonthTopItems struct {
	TopByQuantity *ItemRecord
	TopByRevenue  *ItemRecord
}

// ClientState holds the state for a specific client
type ClientState struct {
	topItemsByMonth map[string]*MonthTopItems // key: "YYYY-MM"
	eosReceived     map[string]bool           // Track EOS from 5 reduce workers (key: FileID)
	eosCount        int
}

// TopItemsWorker processes month-level aggregations and selects top items
type TopItemsWorker struct {
	consumer     *workerqueue.QueueConsumer
	producer     *workerqueue.QueueMiddleware
	config       *middleware.ConnectionConfig
	clientStates map[string]*ClientState // key: ClientID
}

// NewTopItemsWorker creates a new top items worker
func NewTopItemsWorker() *TopItemsWorker {
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	// Create consumer for top items queue
	consumer := workerqueue.NewQueueConsumer(queues.Query2TopItemsQueue, config)
	if consumer == nil {
		log.Fatal("Failed to create consumer")
	}

	// Declare the input queue
	queueDeclarer := workerqueue.NewMessageMiddlewareQueue(queues.Query2TopItemsQueue, config)
	if queueDeclarer == nil {
		consumer.Close()
		log.Fatal("Failed to create queue declarer")
	}
	if err := queueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		queueDeclarer.Close()
		log.Fatalf("Failed to declare input queue '%s': %v", queues.Query2TopItemsQueue, err)
	}
	queueDeclarer.Close()

	// Create producer for output queue (to ItemID join)
	producer := workerqueue.NewMessageMiddlewareQueue(queues.Query2GroupByResultsQueue, config)
	if producer == nil {
		consumer.Close()
		log.Fatal("Failed to create producer")
	}

	// Declare the output queue
	if err := producer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		producer.Close()
		log.Fatalf("Failed to declare output queue '%s': %v", queues.Query2GroupByResultsQueue, err)
	}

	return &TopItemsWorker{
		consumer:     consumer,
		producer:     producer,
		config:       config,
		clientStates: make(map[string]*ClientState),
	}
}

// getOrCreateClientState gets or creates client state
func (tw *TopItemsWorker) getOrCreateClientState(clientID string) *ClientState {
	if tw.clientStates[clientID] == nil {
		tw.clientStates[clientID] = &ClientState{
			topItemsByMonth: make(map[string]*MonthTopItems),
			eosReceived:     make(map[string]bool),
			eosCount:        0,
		}
	}
	return tw.clientStates[clientID]
}

// processMessage processes a single message from reduce workers
func (tw *TopItemsWorker) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		log.Printf("Top Items Worker: Failed to deserialize chunk: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	clientID := chunkMsg.ClientID
	clientState := tw.getOrCreateClientState(clientID)

	// Process the chunk data first (if it has data)
	if chunkMsg.ChunkSize > 0 && len(chunkMsg.ChunkData) > 0 {
		if err := tw.processChunkData(chunkMsg, clientState); err != nil {
			log.Printf("Top Items Worker: Failed to process chunk data: %v", err)
			return middleware.MessageMiddlewareMessageError
		}
	}

	// Check if this is the last chunk (EOS marker)
	if chunkMsg.IsLastChunk {
		// Mark this reduce worker as finished
		if !clientState.eosReceived[chunkMsg.FileID] {
			clientState.eosReceived[chunkMsg.FileID] = true
			clientState.eosCount++
			log.Printf("Top Items Worker: Received EOS from reduce worker %s (Client: %s) - Count: %d/3",
				chunkMsg.FileID, clientID, clientState.eosCount)
		}

		// If we received EOS from all 3 reduce workers, send final results
		if clientState.eosCount >= 3 {
			log.Printf("Top Items Worker: All reduce workers finished for client %s, sending top items...", clientID)
			if err := tw.sendTopItems(clientID, clientState); err != 0 {
				log.Printf("Top Items Worker: Failed to send top items: %v", err)
				return err
			}
			// Clear client state
			delete(tw.clientStates, clientID)
		}
	}

	return 0
}

// processChunkData processes chunk data and updates top items
func (tw *TopItemsWorker) processChunkData(chunkMsg *chunk.Chunk, clientState *ClientState) error {
	// Parse CSV data
	reader := csv.NewReader(strings.NewReader(chunkMsg.ChunkData))
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to parse CSV: %w", err)
	}

	// Skip header row
	for _, record := range records {
		if strings.Contains(record[0], "year") {
			continue
		}

		if len(record) < 6 {
			continue
		}

		year, _ := strconv.Atoi(record[0])
		month, _ := strconv.Atoi(record[1])
		itemID := record[2]
		totalQuantity, _ := strconv.Atoi(record[3])
		totalSubtotal, _ := strconv.ParseFloat(record[4], 64)

		// Create month key
		monthKey := fmt.Sprintf("%04d-%02d", year, month)

		// Initialize month data if needed
		if clientState.topItemsByMonth[monthKey] == nil {
			clientState.topItemsByMonth[monthKey] = &MonthTopItems{}
		}

		monthTop := clientState.topItemsByMonth[monthKey]
		currentItem := &ItemRecord{
			ItemID:        itemID,
			Year:          year,
			Month:         month,
			TotalQuantity: totalQuantity,
			TotalSubtotal: totalSubtotal,
		}

		// Update top by quantity
		if monthTop.TopByQuantity == nil || currentItem.TotalQuantity > monthTop.TopByQuantity.TotalQuantity {
			monthTop.TopByQuantity = currentItem
		}

		// Update top by revenue
		if monthTop.TopByRevenue == nil || currentItem.TotalSubtotal > monthTop.TopByRevenue.TotalSubtotal {
			monthTop.TopByRevenue = currentItem
		}
	}

	log.Printf("Top Items Worker: Processed chunk for client %s - Now tracking %d months",
		chunkMsg.ClientID, len(clientState.topItemsByMonth))

	return nil
}

// sendTopItems sends the final top items to the join worker
func (tw *TopItemsWorker) sendTopItems(clientID string, clientState *ClientState) middleware.MessageMiddlewareError {
	// Convert top items to CSV
	// Schema must match what ItemID join worker expects: year,month,item_id,quantity,subtotal,count
	var csvBuilder strings.Builder
	csvBuilder.WriteString("year,month,item_id,quantity,subtotal,count\n")

	for _, monthTop := range clientState.topItemsByMonth {
		// Add top by quantity
		if monthTop.TopByQuantity != nil {
			csvBuilder.WriteString(fmt.Sprintf("%d,%d,%s,%d,%.2f,%d\n",
				monthTop.TopByQuantity.Year,
				monthTop.TopByQuantity.Month,
				monthTop.TopByQuantity.ItemID,
				monthTop.TopByQuantity.TotalQuantity,
				monthTop.TopByQuantity.TotalSubtotal,
				1, // count = 1 (top item indicator)
			))
		}

		// Add top by revenue (if different from top by quantity)
		if monthTop.TopByRevenue != nil {
			if monthTop.TopByQuantity == nil || monthTop.TopByRevenue.ItemID != monthTop.TopByQuantity.ItemID {
				csvBuilder.WriteString(fmt.Sprintf("%d,%d,%s,%d,%.2f,%d\n",
					monthTop.TopByRevenue.Year,
					monthTop.TopByRevenue.Month,
					monthTop.TopByRevenue.ItemID,
					monthTop.TopByRevenue.TotalQuantity,
					monthTop.TopByRevenue.TotalSubtotal,
					2, // count = 2 (top revenue indicator, different from top quantity)
				))
			}
		}
	}

	csvData := csvBuilder.String()

	// Create chunk for output
	outputChunk := chunk.NewChunk(
		clientID,
		"TOP2", // File ID for top items
		2,      // Query Type 2
		1,      // Chunk Number
		true,   // Is Last Chunk
		3,      // Step 3 (after map and reduce)
		len(csvData),
		2, // Table ID 2
		csvData,
	)

	// Serialize and send
	chunkMsg := chunk.NewChunkMessage(outputChunk)
	serializedData, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
		log.Printf("Top Items Worker: Failed to serialize output chunk: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	sendErr := tw.producer.Send(serializedData)
	if sendErr != 0 {
		log.Printf("Top Items Worker: Failed to send output chunk: %v", sendErr)
		return sendErr
	}

	log.Printf("Top Items Worker: Successfully sent top items for client %s (%d months)", clientID, len(clientState.topItemsByMonth))
	return 0
}

// Start starts the top items worker
func (tw *TopItemsWorker) Start() {
	log.Println("Starting Top Items Worker for Query 2...")

	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			err := tw.processMessage(delivery)
			if err != 0 {
				log.Printf("Failed to process message: %v", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}

	if err := tw.consumer.StartConsuming(onMessageCallback); err != 0 {
		log.Fatalf("Failed to start consuming: %v", err)
	}

	// Keep the worker running
	select {}
}

// Close closes the worker
func (tw *TopItemsWorker) Close() {
	if tw.consumer != nil {
		tw.consumer.Close()
	}
	if tw.producer != nil {
		tw.producer.Close()
	}
}

func main() {
	topItemsWorker := NewTopItemsWorker()
	defer topItemsWorker.Close()

	topItemsWorker.Start()
}
