package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// MenuItem represents a menu item record
type MenuItem struct {
	ItemID        string
	ItemName      string
	Category      string
	Price         string
	IsSeasonal    string
	AvailableFrom string
	AvailableTo   string
}

// ItemIdJoinWorker encapsulates the ItemID join worker state and dependencies
type ItemIdJoinWorker struct {
	dictionaryConsumer *exchange.ExchangeConsumer
	chunkConsumer      *workerqueue.QueueConsumer
	outputProducer     *workerqueue.QueueMiddleware
	config             *middleware.ConnectionConfig

	// Dictionary state
	dictionaryReady bool
	menuItems       map[string]*MenuItem // item_id -> MenuItem
	mutex           sync.RWMutex
}

// NewItemIdJoinWorker creates a new ItemIdJoinWorker instance
func NewItemIdJoinWorker(config *middleware.ConnectionConfig) (*ItemIdJoinWorker, error) {
	// Get worker instance ID from environment (defaults to "1" for single-worker setups)
	instanceID := os.Getenv("WORKER_INSTANCE_ID")
	if instanceID == "" {
		instanceID = "1"
	}
	
	// Create instance-specific routing key for this worker
	instanceRoutingKey := fmt.Sprintf("%s-instance-%s", ItemIdDictionaryRoutingKey, instanceID)
	fmt.Printf("ItemID Join Worker: Initializing with instance ID: %s, routing key: %s\n", instanceID, instanceRoutingKey)
	
	// Create dictionary consumer (exchange for broadcasting to all workers)
	dictionaryConsumer := exchange.NewExchangeConsumer(
		ItemIdDictionaryExchange,
		[]string{instanceRoutingKey},
		config,
	)
	if dictionaryConsumer == nil {
		return nil, fmt.Errorf("failed to create dictionary consumer")
	}

	// Create chunk consumer
	chunkConsumer := workerqueue.NewQueueConsumer(
		ItemIdChunkQueue,
		config,
	)
	if chunkConsumer == nil {
		dictionaryConsumer.Close()
		return nil, fmt.Errorf("failed to create chunk consumer")
	}

	// Create output producer
	outputProducer := workerqueue.NewMessageMiddlewareQueue(
		Query2ResultsQueue,
		config,
	)
	if outputProducer == nil {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		return nil, fmt.Errorf("failed to create output producer")
	}

	// Declare output queue

	if err := outputProducer.DeclareQueue(false, false, false, false); err != 0 {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		outputProducer.Close()
		return nil, fmt.Errorf("failed to declare output queue: %v", err)
	}

	return &ItemIdJoinWorker{
		dictionaryConsumer: dictionaryConsumer,
		chunkConsumer:      chunkConsumer,
		outputProducer:     outputProducer,
		config:             config,
		dictionaryReady:    false,
		menuItems:          make(map[string]*MenuItem),
	}, nil
}

// Start starts the ItemID join worker
func (w *ItemIdJoinWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("ItemID Join Worker: Starting to listen for messages...")

	// Start consuming from dictionary queue
	go func() {
		if err := w.dictionaryConsumer.StartConsuming(w.createDictionaryCallback()); err != 0 {
			fmt.Printf("Failed to start dictionary consumer: %v\n", err)
		}
	}()

	// Start consuming from chunk queue
	go func() {
		if err := w.chunkConsumer.StartConsuming(w.createChunkCallback()); err != 0 {
			fmt.Printf("Failed to start chunk consumer: %v\n", err)
		}
	}()

	return 0
}

// Close closes all connections
func (w *ItemIdJoinWorker) Close() {
	if w.dictionaryConsumer != nil {
		w.dictionaryConsumer.Close()
	}
	if w.chunkConsumer != nil {
		w.chunkConsumer.Close()
	}
	if w.outputProducer != nil {
		w.outputProducer.Close()
	}
}

// createDictionaryCallback creates the dictionary message processing callback
func (w *ItemIdJoinWorker) createDictionaryCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			err := w.processDictionaryMessage(delivery)
			if err != 0 {
				done <- fmt.Errorf("failed to process dictionary message: %v", err)
				return
			}
		}
	}
}

// createChunkCallback creates the chunk message processing callback
func (w *ItemIdJoinWorker) createChunkCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			err := w.processChunkMessage(delivery)
			if err != 0 {
				done <- fmt.Errorf("failed to process chunk message: %v", err)
				return
			}
		}
	}
}

// processDictionaryMessage processes dictionary messages
func (w *ItemIdJoinWorker) processDictionaryMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	fmt.Printf("ItemID Join Worker: Received dictionary message\n")

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("ItemID Join Worker: Failed to deserialize dictionary chunk: %v\n", err)
		delivery.Nack(false, false) // Reject the message
		return middleware.MessageMiddlewareMessageError
	}

	fmt.Printf("ItemID Join Worker: Received dictionary message for FileID: %s, ChunkNumber: %d, IsLastChunk: %t\n",
		chunkMsg.FileID, chunkMsg.ChunkNumber, chunkMsg.IsLastChunk)

	// Parse the menu items data from the chunk
	if err := w.parseMenuItemsData(string(chunkMsg.ChunkData)); err != nil {
		fmt.Printf("ItemID Join Worker: Failed to parse menu items data: %v\n", err)
		delivery.Nack(false, false) // Reject the message
		return middleware.MessageMiddlewareMessageError
	}

	// Check if this is the last chunk for the dictionary
	if chunkMsg.IsLastChunk {
		w.dictionaryReady = true
		fmt.Printf("ItemID Join Worker: Received last chunk for FileID: %s - Dictionary is now ready\n", chunkMsg.FileID)
	}

	delivery.Ack(false) // Acknowledge the dictionary message
	return 0
}

// processChunkMessage processes chunk messages for joining
func (w *ItemIdJoinWorker) processChunkMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	fmt.Printf("ItemID Join Worker: Received chunk message\n")

	// Check if dictionary is ready
	dictionaryReady := w.dictionaryReady

	if !dictionaryReady {
		fmt.Printf("ItemID Join Worker: Dictionary not ready, NACKing chunk for retry\n")
		delivery.Nack(false, true) // Reject and requeue
		return 0
	}

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("ItemID Join Worker: Failed to deserialize chunk: %v\n", err)
		delivery.Nack(false, false) // Reject the message
		return middleware.MessageMiddlewareMessageError
	}

	// Process the chunk
	if err := w.processChunk(chunkMsg); err != 0 {
		fmt.Printf("ItemID Join Worker: Failed to process chunk: %v\n", err)
		delivery.Nack(false, false) // Reject the message
		return middleware.MessageMiddlewareMessageError
	}

	delivery.Ack(false) // Acknowledge the chunk message
	return 0
}

// processChunk processes a single chunk for joining
func (w *ItemIdJoinWorker) processChunk(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	fmt.Printf("ItemID Join Worker: Processing chunk - QueryType: %d, Step: %d, ClientID: %s, ChunkNumber: %d, FileID: %s, ChunkData: %s\n",
		chunkMsg.QueryType, chunkMsg.Step, chunkMsg.ClientID, chunkMsg.ChunkNumber, chunkMsg.FileID, chunkMsg.ChunkData)

	// Perform the join operation
	joinedChunk, err := w.performJoin(chunkMsg)
	if err != nil {
		fmt.Printf("ItemID Join Worker: Failed to perform join: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send joined chunk to output queue
	chunkMessage := chunk.NewChunkMessage(joinedChunk)

	fmt.Printf("chunkMessage: %+v\n", chunkMessage)
	serializedData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		fmt.Printf("ItemID Join Worker: Failed to serialize joined chunk: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	if err := w.outputProducer.Send(serializedData); err != 0 {
		fmt.Printf("ItemID Join Worker: Failed to send joined chunk: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	fmt.Printf("ItemID Join Worker: Successfully processed and sent joined chunk\n")
	return 0
}

// performJoin performs the actual join operation
func (w *ItemIdJoinWorker) performJoin(chunkMsg *chunk.Chunk) (*chunk.Chunk, error) {
	fmt.Printf("ItemID Join Worker: Performing join for QueryType: %d, FileID: %s\n",
		chunkMsg.QueryType, chunkMsg.FileID)

	// Check if this is grouped data from GroupBy (Step 3) or raw data
	if w.isGroupedData(chunkMsg.ChunkData) {
		fmt.Printf("ItemID Join Worker: Received grouped data, joining with menu items\n")
		// Parse the grouped data from GroupBy
		groupedData, err := w.parseGroupedTransactionItemsData(string(chunkMsg.ChunkData))
		if err != nil {
			return nil, fmt.Errorf("failed to parse grouped transaction items data: %w", err)
		}

		// Perform the join with grouped data
		joinedData := w.performGroupedTransactionItemMenuJoin(groupedData)

		// Create new chunk with joined data
		joinedChunk := &chunk.Chunk{
			ClientID:    chunkMsg.ClientID,
			FileID:      chunkMsg.FileID,
			QueryType:   chunkMsg.QueryType,
			ChunkNumber: chunkMsg.ChunkNumber,
			IsLastChunk: chunkMsg.IsLastChunk,
			Step:        chunkMsg.Step,
			ChunkSize:   len(joinedData),
			TableID:     chunkMsg.TableID,
			ChunkData:   joinedData,
		}

		return joinedChunk, nil
	}

	// Parse the transaction items data
	transactionItemsData, err := w.parseTransactionItemsData(string(chunkMsg.ChunkData))
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction items data: %w", err)
	}

	// Perform the join
	joinedData := w.performTransactionItemMenuJoin(transactionItemsData)

	// Create new chunk with joined data
	joinedChunk := &chunk.Chunk{
		ClientID:    chunkMsg.ClientID,
		FileID:      chunkMsg.FileID,
		QueryType:   chunkMsg.QueryType,
		ChunkNumber: chunkMsg.ChunkNumber,
		IsLastChunk: chunkMsg.IsLastChunk,
		Step:        chunkMsg.Step,
		ChunkSize:   len(joinedData),
		TableID:     chunkMsg.TableID,
		ChunkData:   joinedData,
	}

	return joinedChunk, nil
}

// parseMenuItemsData parses menu items CSV data
func (w *ItemIdJoinWorker) parseMenuItemsData(csvData string) error {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to parse CSV: %w", err)
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Skip header row
	for i := 1; i < len(records); i++ {
		record := records[i]
		if len(record) >= 7 {
			menuItem := &MenuItem{
				ItemID:        record[0],
				ItemName:      record[1],
				Category:      record[2],
				Price:         record[3],
				IsSeasonal:    record[4],
				AvailableFrom: record[5],
				AvailableTo:   record[6],
			}
			w.menuItems[menuItem.ItemID] = menuItem
		}
	}

	fmt.Printf("ItemID Join Worker: Parsed %d menu items\n", len(records)-1)
	return nil
}

// parseTransactionItemsData parses transaction items CSV data
func (w *ItemIdJoinWorker) parseTransactionItemsData(csvData string) ([]map[string]string, error) {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	var transactionItems []map[string]string

	// Skip header row
	for i := 1; i < len(records); i++ {
		record := records[i]
		if len(record) >= 6 {
			item := map[string]string{
				"transaction_id": record[0],
				"item_id":        record[1],
				"quantity":       record[2],
				"unit_price":     record[3],
				"subtotal":       record[4],
				"created_at":     record[5],
			}
			transactionItems = append(transactionItems, item)
		}
	}

	return transactionItems, nil
}

// performTransactionItemMenuJoin performs the join between transaction items and menu items
func (w *ItemIdJoinWorker) performTransactionItemMenuJoin(transactionItems []map[string]string) string {
	var result strings.Builder

	// Write header
	result.WriteString("transaction_id,item_id,quantity,unit_price,subtotal,created_at,item_name,category,price,is_seasonal\n")

	w.mutex.RLock()
	defer w.mutex.RUnlock()

	for _, item := range transactionItems {
		itemID := item["item_id"]
		if menuItem, exists := w.menuItems[itemID]; exists {
			// Join successful - write all fields
			result.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
				item["transaction_id"],
				item["item_id"],
				item["quantity"],
				item["unit_price"],
				item["subtotal"],
				item["created_at"],
				menuItem.ItemName,
				menuItem.Category,
				menuItem.Price,
				menuItem.IsSeasonal,
			))
		} else {
			// Join failed - write original data with empty joined fields
			result.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,,,,\n",
				item["transaction_id"],
				item["item_id"],
				item["quantity"],
				item["unit_price"],
				item["subtotal"],
				item["created_at"],
			))
		}
	}

	fmt.Printf("ItemID Join Worker: Joined %d transaction items with menu items\n", len(transactionItems))

	return result.String()
}

// isGroupedData checks if the data is grouped data from GroupBy
func (w *ItemIdJoinWorker) isGroupedData(data string) bool {
	// Check if the data has the grouped schema (year,month,item_id,quantity,subtotal,count)
	lines := strings.Split(data, "\n")
	if len(lines) < 1 {
		return false
	}

	header := lines[0]
	// Check for grouped data headers: year,month,item_id,quantity,subtotal,count
	return strings.Contains(header, "year") && strings.Contains(header, "count") && strings.Contains(header, "item_id")
}

// parseGroupedTransactionItemsData parses grouped transaction items data from GroupBy
func (w *ItemIdJoinWorker) parseGroupedTransactionItemsData(csvData string) ([]map[string]string, error) {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	var groupedItems []map[string]string

	// Skip header row
	for i := 1; i < len(records); i++ {
		record := records[i]
		if len(record) >= 6 {
			item := map[string]string{
				"year":     record[0],
				"month":    record[1],
				"item_id":  record[2],
				"quantity": record[3],
				"subtotal": record[4],
				"count":    record[5],
			}
			groupedItems = append(groupedItems, item)
		}
	}

	return groupedItems, nil
}

// performGroupedTransactionItemMenuJoin performs the join between grouped transaction items and menu items
func (w *ItemIdJoinWorker) performGroupedTransactionItemMenuJoin(groupedItems []map[string]string) string {
	var result strings.Builder

	// Write header for joined grouped data
	result.WriteString("year,month,item_id,quantity,subtotal,count,item_name,category,price,is_seasonal\n")

	w.mutex.RLock()
	defer w.mutex.RUnlock()

	for _, item := range groupedItems {
		itemID := item["item_id"]
		if menuItem, exists := w.menuItems[itemID]; exists {
			// Join successful - write all fields
			result.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
				item["year"],
				item["month"],
				item["item_id"],
				item["quantity"],
				item["subtotal"],
				item["count"],
				menuItem.ItemName,
				menuItem.Category,
				menuItem.Price,
				menuItem.IsSeasonal,
			))
		} else {
			// Join failed - write original data with empty joined fields
			result.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,,,,\n",
				item["year"],
				item["month"],
				item["item_id"],
				item["quantity"],
				item["subtotal"],
				item["count"],
			))
		}
	}

	return result.String()
}
