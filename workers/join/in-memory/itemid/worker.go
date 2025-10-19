package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
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
	cleanupConsumer    *exchange.ExchangeConsumer
	outputProducer     *workerqueue.QueueMiddleware
	completionProducer *workerqueue.QueueMiddleware
	config             *middleware.ConnectionConfig
	workerID           string

	// Dictionary state - now client-aware
	dictionaryReady map[string]bool                 // client_id -> ready status
	menuItems       map[string]map[string]*MenuItem // client_id -> item_id -> MenuItem
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

	// Create cleanup consumer (exchange for cleanup commands)
	cleanupConsumer := exchange.NewExchangeConsumer(
		"itemid-cleanup-exchange",
		[]string{fmt.Sprintf("itemid-cleanup-instance-%s", instanceID)},
		config,
	)
	if cleanupConsumer == nil {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		outputProducer.Close()
		return nil, fmt.Errorf("failed to create cleanup consumer")
	}

	// Create completion producer
	completionProducer := workerqueue.NewMessageMiddlewareQueue(
		"join-completion-queue",
		config,
	)
	if completionProducer == nil {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		outputProducer.Close()
		cleanupConsumer.Close()
		return nil, fmt.Errorf("failed to create completion producer")
	}

	// Declare input queue (ItemIdChunkQueue)
	inputQueueDeclarer := workerqueue.NewMessageMiddlewareQueue(ItemIdChunkQueue, config)
	if inputQueueDeclarer == nil {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		outputProducer.Close()
		cleanupConsumer.Close()
		completionProducer.Close()
		return nil, fmt.Errorf("failed to create input queue declarer")
	}
	if err := inputQueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		outputProducer.Close()
		cleanupConsumer.Close()
		completionProducer.Close()
		inputQueueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare input queue: %v", err)
	}

	// Declare output queue
	if err := outputProducer.DeclareQueue(false, false, false, false); err != 0 {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		outputProducer.Close()
		cleanupConsumer.Close()
		completionProducer.Close()
		return nil, fmt.Errorf("failed to declare output queue: %v", err)
	}

	// Declare completion queue
	if err := completionProducer.DeclareQueue(false, false, false, false); err != 0 {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		outputProducer.Close()
		cleanupConsumer.Close()
		completionProducer.Close()
		return nil, fmt.Errorf("failed to declare completion queue: %v", err)
	}

	return &ItemIdJoinWorker{
		dictionaryConsumer: dictionaryConsumer,
		chunkConsumer:      chunkConsumer,
		cleanupConsumer:    cleanupConsumer,
		outputProducer:     outputProducer,
		completionProducer: completionProducer,
		config:             config,
		workerID:           fmt.Sprintf("itemid-worker-%s", instanceID),
		dictionaryReady:    make(map[string]bool),
		menuItems:          make(map[string]map[string]*MenuItem),
	}, nil
}

// Start starts the ItemID join worker
func (w *ItemIdJoinWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("ItemID Join Worker: Starting to listen for messages...")

	// Start consuming from dictionary queue
	if err := w.dictionaryConsumer.StartConsuming(w.createDictionaryCallback()); err != 0 {
		fmt.Printf("Failed to start dictionary consumer: %v\n", err)
	}

	// Start consuming from chunk queue
	if err := w.chunkConsumer.StartConsuming(w.createChunkCallback()); err != 0 {
		fmt.Printf("Failed to start chunk consumer: %v\n", err)
	}

	// Start consuming from cleanup queue
	if err := w.cleanupConsumer.StartConsuming(w.createCleanupCallback()); err != 0 {
		fmt.Printf("Failed to start cleanup consumer: %v\n", err)
	}

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
	if w.cleanupConsumer != nil {
		w.cleanupConsumer.Close()
	}
	if w.outputProducer != nil {
		w.outputProducer.Close()
	}
	if w.completionProducer != nil {
		w.completionProducer.Close()
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

	fmt.Printf("ItemID Join Worker: Received dictionary message for FileID: %s, ChunkNumber: %d, IsLastChunk: %t, IsLastFromTable: %t\n",
		chunkMsg.FileID, chunkMsg.ChunkNumber, chunkMsg.IsLastChunk, chunkMsg.IsLastFromTable)

	// Parse the menu items data from the chunk
	if err := w.parseMenuItemsData(string(chunkMsg.ChunkData), chunkMsg.ClientID); err != nil {
		fmt.Printf("ItemID Join Worker: Failed to parse menu items data: %v\n", err)
		delivery.Nack(false, false) // Reject the message
		return middleware.MessageMiddlewareMessageError
	}

	// Check if this is the last chunk for the dictionary (it always is?)
	if chunkMsg.IsLastChunk {
		w.dictionaryReady[chunkMsg.ClientID] = true
		fmt.Printf("ItemID Join Worker: Received last chunk for FileID: %s - Dictionary is now ready for client %s\n", chunkMsg.FileID, chunkMsg.ClientID)
	}

	delivery.Ack(false) // Acknowledge the dictionary message
	return 0
}

// processChunkMessage processes chunk messages for joining
func (w *ItemIdJoinWorker) processChunkMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	fmt.Printf("ItemID Join Worker: Received chunk message\n")

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("ItemID Join Worker: Failed to deserialize chunk: %v\n", err)
		delivery.Nack(false, false) // Reject the message
		return middleware.MessageMiddlewareMessageError
	}

	// Check if dictionary is ready for this specific client
	dictionaryReady := w.dictionaryReady[chunkMsg.ClientID]

	if !dictionaryReady {
		fmt.Printf("ItemID Join Worker: Dictionary not ready for client %s, NACKing chunk for retry\n", chunkMsg.ClientID)
		delivery.Nack(false, true) // Reject and requeue
		return 0
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

	// SUCCESS: Data sent successfully - send completion notification to garbage collector
	w.sendCompletionNotification(chunkMsg.ClientID)

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
		joinedData := w.performGroupedTransactionItemMenuJoin(groupedData, chunkMsg.ClientID)

		// Create new chunk with joined data
		joinedChunk := &chunk.Chunk{
			ClientID:        chunkMsg.ClientID,
			FileID:          chunkMsg.FileID,
			QueryType:       chunkMsg.QueryType,
			ChunkNumber:     chunkMsg.ChunkNumber,
			IsLastChunk:     chunkMsg.IsLastChunk,
			IsLastFromTable: chunkMsg.IsLastFromTable,
			Step:            chunkMsg.Step,
			ChunkSize:       len(joinedData),
			TableID:         chunkMsg.TableID,
			ChunkData:       joinedData,
		}

		return joinedChunk, nil
	}

	// Parse the transaction items data
	transactionItemsData, err := w.parseTransactionItemsData(string(chunkMsg.ChunkData))
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction items data: %w", err)
	}

	// Perform the join
	joinedData := w.performTransactionItemMenuJoin(transactionItemsData, chunkMsg.ClientID)

	// Create new chunk with joined data
	joinedChunk := &chunk.Chunk{
		ClientID:        chunkMsg.ClientID,
		FileID:          chunkMsg.FileID,
		QueryType:       chunkMsg.QueryType,
		ChunkNumber:     chunkMsg.ChunkNumber,
		IsLastChunk:     chunkMsg.IsLastChunk,
		IsLastFromTable: chunkMsg.IsLastFromTable,
		Step:            chunkMsg.Step,
		ChunkSize:       len(joinedData),
		TableID:         chunkMsg.TableID,
		ChunkData:       joinedData,
	}

	return joinedChunk, nil
}

// parseMenuItemsData parses menu items CSV data and stores it client-specifically
func (w *ItemIdJoinWorker) parseMenuItemsData(csvData string, clientID string) error {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to parse CSV: %w", err)
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Initialize client-specific dictionary if needed
	if w.menuItems[clientID] == nil {
		w.menuItems[clientID] = make(map[string]*MenuItem)
	}

	for i := 0; i < len(records); i++ {
		record := records[i]
		if strings.Contains(record[0], "item_id") {
			continue
		}
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
			w.menuItems[clientID][menuItem.ItemID] = menuItem
		}
	}

	fmt.Printf("ItemID Join Worker: Parsed %d menu items for client %s\n", len(records)-1, clientID)
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
	for i := 0; i < len(records); i++ {
		record := records[i]
		if strings.Contains(record[0], "transaction_id") {
			continue
		}
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
func (w *ItemIdJoinWorker) performTransactionItemMenuJoin(transactionItems []map[string]string, clientID string) string {
	var result strings.Builder

	// Write header
	result.WriteString("transaction_id,item_id,quantity,unit_price,subtotal,created_at,item_name,category,price,is_seasonal\n")

	w.mutex.RLock()
	defer w.mutex.RUnlock()

	// Get client-specific menu items dictionary
	clientMenuItems := w.menuItems[clientID]
	if clientMenuItems == nil {
		// No menu items for this client, write original data with empty joined fields
		for _, item := range transactionItems {
			result.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,,,,\n",
				item["transaction_id"],
				item["item_id"],
				item["quantity"],
				item["unit_price"],
				item["subtotal"],
				item["created_at"],
			))
		}
		return result.String()
	}

	for _, item := range transactionItems {
		itemID := item["item_id"]
		if menuItem, exists := clientMenuItems[itemID]; exists {
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

	fmt.Printf("ItemID Join Worker: Joined %d transaction items with menu items for client %s\n", len(transactionItems), clientID)

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

	for i := 0; i < len(records); i++ {
		record := records[i]
		if strings.Contains(record[0], "year") {
			continue
		}
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
func (w *ItemIdJoinWorker) performGroupedTransactionItemMenuJoin(groupedItems []map[string]string, clientID string) string {
	var result strings.Builder

	// Write header for joined grouped data
	result.WriteString("year,month,item_id,quantity,subtotal,count,item_name,category,price,is_seasonal\n")

	w.mutex.RLock()
	defer w.mutex.RUnlock()

	// Get client-specific menu items dictionary
	clientMenuItems := w.menuItems[clientID]
	if clientMenuItems == nil {
		// No menu items for this client, write original data with empty joined fields
		for _, item := range groupedItems {
			result.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,,,,\n",
				item["year"],
				item["month"],
				item["item_id"],
				item["quantity"],
				item["subtotal"],
				item["count"],
			))
		}
		return result.String()
	}

	for _, item := range groupedItems {
		itemID := item["item_id"]
		if menuItem, exists := clientMenuItems[itemID]; exists {
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

// sendCompletionNotification sends a completion signal to the garbage collector
func (w *ItemIdJoinWorker) sendCompletionNotification(clientID string) {
	completionSignal := signals.NewJoinCompletionSignal(clientID, "items", w.workerID)

	messageData, err := signals.SerializeJoinCompletionSignal(completionSignal)
	if err != nil {
		fmt.Printf("ItemID Join Worker: Failed to serialize completion signal: %v\n", err)
		return
	}

	if err := w.completionProducer.Send(messageData); err != 0 {
		fmt.Printf("ItemID Join Worker: Failed to send completion signal: %v\n", err)
	} else {
		fmt.Printf("ItemID Join Worker: Sent completion signal for client %s\n", clientID)
	}
}

// createCleanupCallback creates the cleanup message processing callback
func (w *ItemIdJoinWorker) createCleanupCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			err := w.processCleanupMessage(delivery)
			if err != nil {
				done <- fmt.Errorf("failed to process cleanup message: %v", err)
				return
			}
		}
	}
}

// processCleanupMessage processes a cleanup signal
func (w *ItemIdJoinWorker) processCleanupMessage(delivery amqp.Delivery) error {
	// Deserialize the message using the deserializer
	message, err := deserializer.Deserialize(delivery.Body)
	if err != nil {
		fmt.Printf("ItemID Join Worker: Failed to deserialize message: %v\n", err)
		delivery.Nack(false, false) // Reject the message
		return err
	}

	// Check if it's a cleanup signal
	cleanupSignal, ok := message.(*signals.JoinCleanupSignal)
	if !ok {
		fmt.Printf("ItemID Join Worker: Received non-cleanup message type")
		delivery.Nack(false, false) // Reject the message
		return fmt.Errorf("expected JoinCleanupSignal, got %T", message)
	}

	fmt.Printf("ItemID Join Worker: Received cleanup signal for client %s\n", cleanupSignal.ClientID)

	// Perform cleanup
	w.cleanupClientItems(cleanupSignal.ClientID)

	delivery.Ack(false) // Acknowledge the message
	return nil
}

// cleanupClientItems deletes all item dictionary data for a specific client
func (w *ItemIdJoinWorker) cleanupClientItems(clientID string) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Delete client-specific item dictionary
	delete(w.menuItems, clientID)
	delete(w.dictionaryReady, clientID)

	fmt.Printf("ItemID Join Worker: Cleaned up items for client %s\n", clientID)
}
