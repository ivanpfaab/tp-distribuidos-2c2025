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

// Store represents a store record
type Store struct {
	StoreID    string
	StoreName  string
	Street     string
	PostalCode string
	City       string
	State      string
	Latitude   string
	Longitude  string
}

// StoreIdJoinWorker encapsulates the StoreID join worker state and dependencies
type StoreIdJoinWorker struct {
	dictionaryConsumer *exchange.ExchangeConsumer
	chunkConsumer      *workerqueue.QueueConsumer
	outputProducer     *workerqueue.QueueMiddleware
	config             *middleware.ConnectionConfig

	// Dictionary state - now client-aware
	dictionaryReady map[string]bool              // client_id -> ready status
	stores          map[string]map[string]*Store // client_id -> store_id -> Store
	mutex           sync.RWMutex
}

// NewStoreIdJoinWorker creates a new StoreIdJoinWorker instance
func NewStoreIdJoinWorker(config *middleware.ConnectionConfig) (*StoreIdJoinWorker, error) {
	// Get worker instance ID from environment (defaults to "1" for single-worker setups)
	instanceID := os.Getenv("WORKER_INSTANCE_ID")
	if instanceID == "" {
		instanceID = "1"
	}

	// Create instance-specific routing key for this worker
	instanceRoutingKey := fmt.Sprintf("%s-instance-%s", StoreIdDictionaryRoutingKey, instanceID)
	fmt.Printf("StoreID Join Worker: Initializing with instance ID: %s, routing key: %s\n", instanceID, instanceRoutingKey)

	// Create dictionary consumer (exchange for broadcasting to all workers)
	dictionaryConsumer := exchange.NewExchangeConsumer(
		StoreIdDictionaryExchange,
		[]string{instanceRoutingKey},
		config,
	)
	if dictionaryConsumer == nil {
		return nil, fmt.Errorf("failed to create dictionary consumer")
	}

	// Create chunk consumer
	chunkConsumer := workerqueue.NewQueueConsumer(
		StoreIdChunkQueue,
		config,
	)
	if chunkConsumer == nil {
		dictionaryConsumer.Close()
		return nil, fmt.Errorf("failed to create chunk consumer")
	}

	// Create output producer
	outputProducer := workerqueue.NewMessageMiddlewareQueue(
		Query3ResultsQueue,
		config,
	)
	if outputProducer == nil {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		return nil, fmt.Errorf("failed to create output producer")
	}

	// Declare input queue (StoreIdChunkQueue)
	inputQueueDeclarer := workerqueue.NewMessageMiddlewareQueue(StoreIdChunkQueue, config)
	if inputQueueDeclarer == nil {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		outputProducer.Close()
		return nil, fmt.Errorf("failed to create input queue declarer")
	}
	if err := inputQueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		outputProducer.Close()
		inputQueueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare input queue: %v", err)
	}

	// Declare output queue
	if err := outputProducer.DeclareQueue(false, false, false, false); err != 0 {
		dictionaryConsumer.Close()
		chunkConsumer.Close()
		outputProducer.Close()
		return nil, fmt.Errorf("failed to declare output queue: %v", err)
	}

	return &StoreIdJoinWorker{
		dictionaryConsumer: dictionaryConsumer,
		chunkConsumer:      chunkConsumer,
		outputProducer:     outputProducer,
		config:             config,
		dictionaryReady:    make(map[string]bool),
		stores:             make(map[string]map[string]*Store),
	}, nil
}

// Start starts the StoreID join worker
func (w *StoreIdJoinWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("StoreID Join Worker: Starting to listen for messages...")

	if err := w.dictionaryConsumer.StartConsuming(w.createDictionaryCallback()); err != 0 {
		fmt.Printf("Failed to start dictionary consumer: %v\n", err)
	}

	if err := w.chunkConsumer.StartConsuming(w.createChunkCallback()); err != 0 {
		fmt.Printf("Failed to start chunk consumer: %v\n", err)
	}

	return 0
}

// Close closes all connections
func (w *StoreIdJoinWorker) Close() {
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
func (w *StoreIdJoinWorker) createDictionaryCallback() func(middleware.ConsumeChannel, chan error) {
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
func (w *StoreIdJoinWorker) createChunkCallback() func(middleware.ConsumeChannel, chan error) {
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
func (w *StoreIdJoinWorker) processDictionaryMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	fmt.Printf("StoreID Join Worker: Received dictionary message\n")

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("StoreID Join Worker: Failed to deserialize dictionary chunk: %v\n", err)
		delivery.Nack(false, false) // Reject the message
		return middleware.MessageMiddlewareMessageError
	}

	fmt.Printf("StoreID Join Worker: Received dictionary message for FileID: %s, ChunkNumber: %d, IsLastChunk: %t\n",
		chunkMsg.FileID, chunkMsg.ChunkNumber, chunkMsg.IsLastChunk)

	// Parse the stores data from the chunk
	if err := w.parseStoresData(string(chunkMsg.ChunkData), chunkMsg.ClientID); err != nil {
		fmt.Printf("StoreID Join Worker: Failed to parse stores data: %v\n", err)
		delivery.Nack(false, false) // Reject the message
		return middleware.MessageMiddlewareMessageError
	}

	// Check if this is the last chunk for the dictionary
	if chunkMsg.IsLastChunk {
		w.dictionaryReady[chunkMsg.ClientID] = true
		fmt.Printf("StoreID Join Worker: Received last chunk for FileID: %s - Dictionary is now ready for client %s\n", chunkMsg.FileID, chunkMsg.ClientID)
	}

	delivery.Ack(false) // Acknowledge the dictionary message
	return 0
}

// processChunkMessage processes chunk messages for joining
func (w *StoreIdJoinWorker) processChunkMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	fmt.Printf("StoreID Join Worker: Received chunk message\n")

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("StoreID Join Worker: Failed to deserialize chunk: %v\n", err)
		delivery.Nack(false, false) // Reject the message
		return middleware.MessageMiddlewareMessageError
	}

	// Check if dictionary is ready for this specific client
	dictionaryReady := w.dictionaryReady[chunkMsg.ClientID]

	if !dictionaryReady {
		fmt.Printf("StoreID Join Worker: Dictionary not ready for client %s, NACKing chunk for retry\n", chunkMsg.ClientID)
		delivery.Nack(false, true) // Reject and requeue
		return 0
	}

	// Process the chunk
	if err := w.processChunk(chunkMsg); err != 0 {
		fmt.Printf("StoreID Join Worker: Failed to process chunk: %v\n", err)
		delivery.Nack(false, false) // Reject the message
		return middleware.MessageMiddlewareMessageError
	}

	delivery.Ack(false) // Acknowledge the chunk message
	return 0
}

// processChunk processes a single chunk for joining
func (w *StoreIdJoinWorker) processChunk(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	fmt.Printf("StoreID Join Worker: Processing chunk - QueryType: %d, Step: %d, ClientID: %s, ChunkNumber: %d, FileID: %s\n",
		chunkMsg.QueryType, chunkMsg.Step, chunkMsg.ClientID, chunkMsg.ChunkNumber, chunkMsg.FileID)

	// Perform the join operation
	joinedChunk, err := w.performJoin(chunkMsg)
	if err != nil {
		fmt.Printf("StoreID Join Worker: Failed to perform join: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send joined chunk to output queue
	chunkMessage := chunk.NewChunkMessage(joinedChunk)
	serializedData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		fmt.Printf("StoreID Join Worker: Failed to serialize joined chunk: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	if err := w.outputProducer.Send(serializedData); err != 0 {
		fmt.Printf("StoreID Join Worker: Failed to send joined chunk: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// SUCCESS: Data sent successfully - cleanup client's dictionary immediately
	w.cleanupClientStores(chunkMsg.ClientID)

	fmt.Printf("StoreID Join Worker: Successfully processed and sent joined chunk\n")
	return 0
}

// performJoin performs the actual join operation
func (w *StoreIdJoinWorker) performJoin(chunkMsg *chunk.Chunk) (*chunk.Chunk, error) {
	fmt.Printf("StoreID Join Worker: Performing join for QueryType: %d, FileID: %s\n",
		chunkMsg.QueryType, chunkMsg.FileID)

	// Check if this is grouped data from GroupBy (Step 3) or raw data
	if w.isGroupedData(chunkMsg.ChunkData) {
		fmt.Printf("StoreID Join Worker: Received grouped data, joining with stores\n")
		// Parse the grouped data from GroupBy
		groupedData, err := w.parseGroupedTransactionData(string(chunkMsg.ChunkData))
		if err != nil {
			return nil, fmt.Errorf("failed to parse grouped transaction data: %w", err)
		}

		// Perform the join with grouped data
		joinedData := w.performGroupedTransactionStoreJoin(groupedData, chunkMsg.ClientID)

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

	// Parse the transaction data
	transactionData, err := w.parseTransactionData(string(chunkMsg.ChunkData))
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction data: %w", err)
	}

	// Perform the join
	joinedData := w.performTransactionStoreJoin(transactionData, chunkMsg.ClientID)

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

// parseStoresData parses stores CSV data and stores it client-specifically
func (w *StoreIdJoinWorker) parseStoresData(csvData string, clientID string) error {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to parse CSV: %w", err)
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Initialize client-specific dictionary if needed
	if w.stores[clientID] == nil {
		w.stores[clientID] = make(map[string]*Store)
	}

	// Skip header row
	for i := 0; i < len(records); i++ {
		record := records[i]
		if strings.Contains(record[0], "store_id") {
			continue
		}
		if len(record) >= 8 {
			store := &Store{
				StoreID:    record[0],
				StoreName:  record[1],
				Street:     record[2],
				PostalCode: record[3],
				City:       record[4],
				State:      record[5],
				Latitude:   record[6],
				Longitude:  record[7],
			}
			w.stores[clientID][store.StoreID] = store
		}
	}

	fmt.Printf("StoreID Join Worker: Parsed %d stores for client %s\n", len(records)-1, clientID)
	return nil
}

// parseTransactionData parses transactions CSV data
func (w *StoreIdJoinWorker) parseTransactionData(csvData string) ([]map[string]string, error) {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	var transactions []map[string]string

	// Skip header row
	for i := 1; i < len(records); i++ {
		record := records[i]
		if len(record) >= 9 {
			transaction := map[string]string{
				"transaction_id":    record[0],
				"store_id":          record[1],
				"payment_method_id": record[2],
				"voucher_id":        record[3],
				"user_id":           record[4],
				"original_amount":   record[5],
				"discount_applied":  record[6],
				"final_amount":      record[7],
				"created_at":        record[8],
			}
			transactions = append(transactions, transaction)
		}
	}

	return transactions, nil
}

// performTransactionStoreJoin performs the join between transactions and stores
func (w *StoreIdJoinWorker) performTransactionStoreJoin(transactions []map[string]string, clientID string) string {
	var result strings.Builder

	// Write header
	result.WriteString("transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at,store_name,street,postal_code,city,state,latitude,longitude\n")

	w.mutex.RLock()
	defer w.mutex.RUnlock()

	// Get client-specific stores dictionary
	clientStores := w.stores[clientID]
	fmt.Printf("StoreID Join Worker: Looking up stores for client %s, found %d stores\n", clientID, len(clientStores))
	if clientStores == nil {
		fmt.Printf("StoreID Join Worker: No stores dictionary found for client %s\n", clientID)
		// No stores for this client, write original data with empty joined fields
		for _, transaction := range transactions {
			result.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,,,,,,,\n",
				transaction["transaction_id"],
				transaction["store_id"],
				transaction["payment_method_id"],
				transaction["voucher_id"],
				transaction["user_id"],
				transaction["original_amount"],
				transaction["discount_applied"],
				transaction["final_amount"],
				transaction["created_at"],
			))
		}
		return result.String()
	}

	for _, transaction := range transactions {
		storeID := transaction["store_id"]
		if store, exists := clientStores[storeID]; exists {
			// Join successful - write all fields
			result.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
				transaction["transaction_id"],
				transaction["store_id"],
				transaction["payment_method_id"],
				transaction["voucher_id"],
				transaction["user_id"],
				transaction["original_amount"],
				transaction["discount_applied"],
				transaction["final_amount"],
				transaction["created_at"],
				store.StoreName,
				store.Street,
				store.PostalCode,
				store.City,
				store.State,
				store.Latitude,
				store.Longitude,
			))
		} else {
			// Join failed - write original data with empty joined fields
			result.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,,,,,,,\n",
				transaction["transaction_id"],
				transaction["store_id"],
				transaction["payment_method_id"],
				transaction["voucher_id"],
				transaction["user_id"],
				transaction["original_amount"],
				transaction["discount_applied"],
				transaction["final_amount"],
				transaction["created_at"],
			))
		}
	}

	return result.String()
}

// isGroupedData checks if the data is grouped data from GroupBy
func (w *StoreIdJoinWorker) isGroupedData(data string) bool {
	// Check if the data has the grouped schema (year,semester,store_id,total_final_amount,count)
	lines := strings.Split(data, "\n")
	if len(lines) < 1 {
		return false
	}

	header := lines[0]
	// Check for grouped data headers: year,semester,store_id,total_final_amount,count
	return strings.Contains(header, "year") && strings.Contains(header, "count") && strings.Contains(header, "store_id")
}

// parseGroupedTransactionData parses grouped transaction data from GroupBy (Query Type 3)
func (w *StoreIdJoinWorker) parseGroupedTransactionData(csvData string) ([]map[string]string, error) {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	var groupedTransactions []map[string]string

	// Skip header row
	for i := 1; i < len(records); i++ {
		record := records[i]
		if len(record) >= 5 {
			transaction := map[string]string{
				"year":               record[0],
				"semester":           record[1],
				"store_id":           record[2],
				"total_final_amount": record[3],
				"count":              record[4],
			}
			groupedTransactions = append(groupedTransactions, transaction)
		}
	}

	return groupedTransactions, nil
}

// performGroupedTransactionStoreJoin performs the join between grouped transactions and stores
func (w *StoreIdJoinWorker) performGroupedTransactionStoreJoin(groupedTransactions []map[string]string, clientID string) string {
	var result strings.Builder

	// Write header for joined grouped data
	result.WriteString("year,semester,store_id,total_final_amount,count,store_name,street,postal_code,city,state,latitude,longitude\n")

	w.mutex.RLock()
	defer w.mutex.RUnlock()

	// Get client-specific stores dictionary
	clientStores := w.stores[clientID]
	fmt.Printf("StoreID Join Worker: Looking up stores for client %s, found %d stores\n", clientID, len(clientStores))
	if clientStores == nil {
		fmt.Printf("StoreID Join Worker: No stores dictionary found for client %s\n", clientID)
		// No stores for this client, write original data with empty joined fields
		for _, transaction := range groupedTransactions {
			result.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,,,,,,,\n",
				transaction["year"],
				transaction["semester"],
				transaction["store_id"],
				transaction["total_final_amount"],
				transaction["count"],
			))
		}
		return result.String()
	}

	for _, transaction := range groupedTransactions {
		storeID := transaction["store_id"]
		if store, exists := clientStores[storeID]; exists {
			// Join successful - write all fields
			result.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
				transaction["year"],
				transaction["semester"],
				transaction["store_id"],
				transaction["total_final_amount"],
				transaction["count"],
				store.StoreName,
				store.Street,
				store.PostalCode,
				store.City,
				store.State,
				store.Latitude,
				store.Longitude,
			))
		} else {
			// Join failed - write original data with empty joined fields
			result.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,,,,,,,\n",
				transaction["year"],
				transaction["semester"],
				transaction["store_id"],
				transaction["total_final_amount"],
				transaction["count"],
			))
		}
	}

	return result.String()
}

// cleanupClientStores deletes all store dictionary data for a specific client
func (w *StoreIdJoinWorker) cleanupClientStores(clientID string) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Delete client-specific store dictionary
	delete(w.stores, clientID)
	delete(w.dictionaryReady, clientID)

	fmt.Printf("StoreID Join Worker: Cleaned up stores for client %s\n", clientID)
}
