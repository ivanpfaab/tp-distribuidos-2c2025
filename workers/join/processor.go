package main

import (
	"encoding/csv"
	"fmt"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
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

// User represents a user record
type User struct {
	UserID       string
	Gender       string
	Birthdate    string
	RegisteredAt string
}

// ReferenceData stores reference data for joins
type ReferenceData struct {
	menuItems map[string]*MenuItem // item_id -> MenuItem
	stores    map[string]*Store    // store_id -> Store
	users     map[string]*User     // user_id -> User
	mutex     sync.RWMutex
}

// ChunkQueue stores pending chunks waiting for reference data
type ChunkQueue struct {
	chunks []*chunk.Chunk
	mutex  sync.RWMutex
}

// Global reference data storage
var referenceData = &ReferenceData{
	menuItems: make(map[string]*MenuItem),
	stores:    make(map[string]*Store),
	users:     make(map[string]*User),
}

// Global chunk queue for pending chunks
var chunkQueue = &ChunkQueue{
	chunks: make([]*chunk.Chunk, 0),
}

// Helper functions for chunk queue management
func (cq *ChunkQueue) AddChunk(chunk *chunk.Chunk) {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()
	cq.chunks = append(cq.chunks, chunk)
	fmt.Printf("Join Worker: Queued chunk for ClientID: %s, FileID: %s, ChunkNumber: %d (queue size: %d)\n",
		chunk.ClientID, chunk.FileID, chunk.ChunkNumber, len(cq.chunks))
}

func (cq *ChunkQueue) GetAndClearChunks() []*chunk.Chunk {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()
	chunks := make([]*chunk.Chunk, len(cq.chunks))
	copy(chunks, cq.chunks)
	cq.chunks = cq.chunks[:0] // Clear the queue
	fmt.Printf("Join Worker: Processing %d queued chunks\n", len(chunks))
	return chunks
}

// Helper function to check if reference data is ready for a query type
func isReferenceDataReady(queryType uint8) bool {
	referenceData.mutex.RLock()
	defer referenceData.mutex.RUnlock()

	switch queryType {
	case 2:
		// Query 2: Need menu_items for item_id joins
		return len(referenceData.menuItems) > 0
	case 3:
		// Query 3: Need stores for store_id joins
		return len(referenceData.stores) > 0
	case 4:
		// Query 4: Need users for user_id joins
		return len(referenceData.users) > 0
	default:
		return false
	}
}

// processMessage processes a single message
func (jw *JoinWorker) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	fmt.Printf("Join Worker: Received message: %s\n", string(delivery.Body))

	// Check if this is a reference data message from the writer
	messageBody := string(delivery.Body)
	if strings.HasPrefix(messageBody, "REFERENCE_DATA_CSV:") {
		// Handle reference data CSV message
		if err := jw.handleReferenceDataCSVMessage(messageBody); err != nil {
			fmt.Printf("Join Worker: Failed to handle reference data CSV message: %v\n", err)
			return middleware.MessageMiddlewareMessageError
		}

		// After receiving reference data, process any queued chunks
		jw.processQueuedChunks()
		return 0 // Don't send reply for reference data messages
	}

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("Join Worker: Failed to deserialize chunk message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Check if reference data is ready for this query type
	if !isReferenceDataReady(chunkMsg.QueryType) {
		fmt.Printf("Join Worker: Reference data not ready for QueryType: %d, queuing chunk\n", chunkMsg.QueryType)
		chunkQueue.AddChunk(chunkMsg)
		return 0 // Don't send reply for queued chunks
	}

	// Reference data is ready, process the chunk immediately
	return jw.processChunk(chunkMsg)
}

// processChunk processes a single chunk (extracted from processMessage for reuse)
func (jw *JoinWorker) processChunk(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	fmt.Printf("Join Worker: Processing chunk - QueryType: %d, Step: %d, ClientID: %s, ChunkNumber: %d, FileID: %s\n",
		chunkMsg.QueryType, chunkMsg.Step, chunkMsg.ClientID, chunkMsg.ChunkNumber, chunkMsg.FileID)

	// Perform the join operation
	joinedChunk, err := jw.performJoin(chunkMsg)
	if err != nil {
		fmt.Printf("Join Worker: Failed to perform join: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send reply back to orchestrator
	return jw.sendReply(joinedChunk)
}

// processQueuedChunks processes all chunks in the queue
func (jw *JoinWorker) processQueuedChunks() {
	queuedChunks := chunkQueue.GetAndClearChunks()

	for _, chunkMsg := range queuedChunks {
		fmt.Printf("Join Worker: Processing queued chunk - QueryType: %d, ClientID: %s, ChunkNumber: %d, FileID: %s\n",
			chunkMsg.QueryType, chunkMsg.ClientID, chunkMsg.ChunkNumber, chunkMsg.FileID)

		// Check if reference data is ready for this query type
		if !isReferenceDataReady(chunkMsg.QueryType) {
			fmt.Printf("Join Worker: Reference data still not ready for QueryType: %d, re-queuing chunk\n", chunkMsg.QueryType)
			chunkQueue.AddChunk(chunkMsg)
			continue
		}

		// Perform the join operation
		joinedChunk, err := jw.performJoin(chunkMsg)
		if err != nil {
			fmt.Printf("Join Worker: Failed to perform join on queued chunk: %v\n", err)
			continue
		}

		// Send reply back to orchestrator
		if err := jw.sendReply(joinedChunk); err != 0 {
			fmt.Printf("Join Worker: Failed to send reply for queued chunk: %v\n", err)
		}
	}
}

// sendReply sends a processed chunk as a reply back to the orchestrator
func (jw *JoinWorker) sendReply(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Create a chunk message for serialization
	chunkMessage := chunk.NewChunkMessage(chunkMsg)

	// Serialize the chunk message for reply
	replyData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		fmt.Printf("Join Worker: Failed to serialize reply message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send the reply to the orchestrator reply queue
	if err := jw.replyProducer.Send(replyData); err != 0 {
		fmt.Printf("Join Worker: Failed to send reply to orchestrator: %v\n", err)
		return err
	}

	fmt.Printf("Join Worker: Reply sent successfully for ClientID: %s, ChunkNumber: %d\n",
		chunkMsg.ClientID, chunkMsg.ChunkNumber)
	return 0
}

// handleReferenceDataCSVMessage handles reference data CSV messages from the writer
func (jw *JoinWorker) handleReferenceDataCSVMessage(message string) error {
	fmt.Printf("Join Worker: Received reference data CSV message\n")

	// Parse the message format: "REFERENCE_DATA_CSV:fileID:csvData"
	// Find the first two colons to split properly
	firstColon := strings.Index(message, ":")
	if firstColon == -1 {
		return fmt.Errorf("invalid reference data CSV message format - no first colon")
	}

	secondColon := strings.Index(message[firstColon+1:], ":")
	if secondColon == -1 {
		return fmt.Errorf("invalid reference data CSV message format - no second colon")
	}

	fileID := message[firstColon+1 : firstColon+1+secondColon]
	csvData := message[firstColon+1+secondColon+1:]

	fmt.Printf("Join Worker: Reference data CSV for FileID: %s (size: %d bytes)\n", fileID, len(csvData))

	// Parse and load the reference data based on fileID
	switch fileID {
	case "MN01":
		fmt.Printf("Join Worker: Parsing menu items CSV data\n")
		if err := jw.parseMenuItemsData(csvData); err != nil {
			return fmt.Errorf("failed to parse menu items CSV data: %w", err)
		}
		fmt.Printf("Join Worker: Menu items CSV data parsed successfully\n")
	case "ST01":
		fmt.Printf("Join Worker: Parsing stores CSV data\n")
		if err := jw.parseStoresData(csvData); err != nil {
			return fmt.Errorf("failed to parse stores CSV data: %w", err)
		}
		fmt.Printf("Join Worker: Stores CSV data parsed successfully\n")
	case "US01":
		fmt.Printf("Join Worker: Parsing users CSV data\n")
		if err := jw.parseUsersData(csvData); err != nil {
			return fmt.Errorf("failed to parse users CSV data: %w", err)
		}
		fmt.Printf("Join Worker: Users CSV data parsed successfully\n")
	default:
		fmt.Printf("Join Worker: Unknown reference data fileID: %s\n", fileID)
		return fmt.Errorf("unknown reference data fileID: %s", fileID)
	}

	return nil
}

// performJoin performs the actual join operation based on query type and file
func (jw *JoinWorker) performJoin(chunkMsg *chunk.Chunk) (*chunk.Chunk, error) {
	fmt.Printf("Join Worker: Performing join for QueryType: %d, FileID: %s\n",
		chunkMsg.QueryType, chunkMsg.FileID)

	switch chunkMsg.QueryType {
	case 2:
		// Query 2: transaction_items ↔ menu_items (on item_id)
		return jw.joinTransactionItemsWithMenuItems(chunkMsg)
	case 3:
		// Query 3: transactions ↔ stores (on store_id)
		return jw.joinTransactionsWithStores(chunkMsg)
	case 4:
		// Query 4: transactions ↔ users (on user_id)
		return jw.joinTransactionsWithUsers(chunkMsg)
	default:
		return nil, fmt.Errorf("unknown query type: %d", chunkMsg.QueryType)
	}
}

// joinTransactionItemsWithMenuItems joins transaction_items with menu_items on item_id
func (jw *JoinWorker) joinTransactionItemsWithMenuItems(chunkMsg *chunk.Chunk) (*chunk.Chunk, error) {
	fmt.Printf("Join Worker: Joining transaction_items (FileID: %s) with menu_items\n", chunkMsg.FileID)

	// Parse the transaction items data
	transactionItemsData, err := jw.parseTransactionItemsData(string(chunkMsg.ChunkData))
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction items data: %w", err)
	}

	// Perform the join
	joinedData := jw.performTransactionItemMenuJoin(transactionItemsData)

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

// joinTransactionsWithStores joins transactions with stores on store_id
func (jw *JoinWorker) joinTransactionsWithStores(chunkMsg *chunk.Chunk) (*chunk.Chunk, error) {
	fmt.Printf("Join Worker: Joining transactions (FileID: %s) with stores\n", chunkMsg.FileID)

	// Parse the transaction data
	transactionData, err := jw.parseTransactionData(string(chunkMsg.ChunkData))
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction data: %w", err)
	}

	// Perform the join
	joinedData := jw.performTransactionStoreJoin(transactionData)

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

// joinTransactionsWithUsers joins transactions with users on user_id
func (jw *JoinWorker) joinTransactionsWithUsers(chunkMsg *chunk.Chunk) (*chunk.Chunk, error) {
	fmt.Printf("Join Worker: Joining transactions (FileID: %s) with users\n", chunkMsg.FileID)

	// Parse the transaction data
	transactionData, err := jw.parseTransactionData(string(chunkMsg.ChunkData))
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction data: %w", err)
	}

	// Perform the join
	joinedData := jw.performTransactionUserJoin(transactionData)

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
func (jw *JoinWorker) parseMenuItemsData(csvData string) error {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to parse CSV: %w", err)
	}

	referenceData.mutex.Lock()
	defer referenceData.mutex.Unlock()

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
			referenceData.menuItems[menuItem.ItemID] = menuItem
		}
	}

	fmt.Printf("Join Worker: Parsed %d menu items\n", len(records)-1)
	return nil
}

// parseStoresData parses stores CSV data
func (jw *JoinWorker) parseStoresData(csvData string) error {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to parse CSV: %w", err)
	}

	referenceData.mutex.Lock()
	defer referenceData.mutex.Unlock()

	// Skip header row
	for i := 1; i < len(records); i++ {
		record := records[i]
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
			referenceData.stores[store.StoreID] = store
		}
	}

	fmt.Printf("Join Worker: Parsed %d stores\n", len(records)-1)
	return nil
}

// parseUsersData parses users CSV data
func (jw *JoinWorker) parseUsersData(csvData string) error {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to parse CSV: %w", err)
	}

	referenceData.mutex.Lock()
	defer referenceData.mutex.Unlock()

	// Skip header row
	for i := 1; i < len(records); i++ {
		record := records[i]
		if len(record) >= 4 {
			user := &User{
				UserID:       record[0],
				Gender:       record[1],
				Birthdate:    record[2],
				RegisteredAt: record[3],
			}
			referenceData.users[user.UserID] = user
		}
	}

	fmt.Printf("Join Worker: Parsed %d users\n", len(records)-1)
	return nil
}

// parseTransactionItemsData parses transaction items CSV data
func (jw *JoinWorker) parseTransactionItemsData(csvData string) ([]map[string]string, error) {
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

// parseTransactionData parses transactions CSV data
func (jw *JoinWorker) parseTransactionData(csvData string) ([]map[string]string, error) {
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

// performTransactionItemMenuJoin performs the join between transaction items and menu items
func (jw *JoinWorker) performTransactionItemMenuJoin(transactionItems []map[string]string) string {
	var result strings.Builder

	// Write header
	result.WriteString("transaction_id,item_id,quantity,unit_price,subtotal,created_at,item_name,category,price,is_seasonal\n")

	referenceData.mutex.RLock()
	defer referenceData.mutex.RUnlock()

	for _, item := range transactionItems {
		itemID := item["item_id"]
		if menuItem, exists := referenceData.menuItems[itemID]; exists {
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

	return result.String()
}

// performTransactionStoreJoin performs the join between transactions and stores
func (jw *JoinWorker) performTransactionStoreJoin(transactions []map[string]string) string {
	var result strings.Builder

	// Write header
	result.WriteString("transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at,store_name,street,postal_code,city,state,latitude,longitude\n")

	referenceData.mutex.RLock()
	defer referenceData.mutex.RUnlock()

	for _, transaction := range transactions {
		storeID := transaction["store_id"]
		if store, exists := referenceData.stores[storeID]; exists {
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

// performTransactionUserJoin performs the join between transactions and users
func (jw *JoinWorker) performTransactionUserJoin(transactions []map[string]string) string {
	var result strings.Builder

	// Write header
	result.WriteString("transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at,gender,birthdate,registered_at\n")

	referenceData.mutex.RLock()
	defer referenceData.mutex.RUnlock()

	for _, transaction := range transactions {
		userID := transaction["user_id"]
		if user, exists := referenceData.users[userID]; exists {
			// Join successful - write all fields
			result.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
				transaction["transaction_id"],
				transaction["store_id"],
				transaction["payment_method_id"],
				transaction["voucher_id"],
				transaction["user_id"],
				transaction["original_amount"],
				transaction["discount_applied"],
				transaction["final_amount"],
				transaction["created_at"],
				user.Gender,
				user.Birthdate,
				user.RegisteredAt,
			))
		} else {
			// Join failed - write original data with empty joined fields
			result.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,,,\n",
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
