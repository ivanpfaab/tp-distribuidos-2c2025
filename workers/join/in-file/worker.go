package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// JoinByUserIdWorker handles joining transaction data with user data from CSV files
type JoinByUserIdWorker struct {
	consumer *workerqueue.QueueConsumer
	producer *workerqueue.QueueMiddleware
	config   *middleware.ConnectionConfig
}

// NewJoinByUserIdWorker creates a new JoinByUserIdWorker instance
func NewJoinByUserIdWorker(config *middleware.ConnectionConfig) (*JoinByUserIdWorker, error) {
	// Create consumer for user ID chunks
	consumer := workerqueue.NewQueueConsumer(
		UserIdChunkQueue,
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create user ID chunk consumer")
	}

	// Declare the user ID chunk queue using QueueMiddleware
	userChunkDeclarer := workerqueue.NewMessageMiddlewareQueue(
		UserIdChunkQueue,
		config,
	)
	if userChunkDeclarer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create user chunk queue declarer")
	}
	if err := userChunkDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		userChunkDeclarer.Close()
		return nil, fmt.Errorf("failed to declare user ID chunk queue: %v", err)
	}

	// Create producer for query 4 results
	producer := workerqueue.NewMessageMiddlewareQueue(
		Query4ResultsQueue,
		config,
	)
	if producer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create query 4 results producer")
	}

	// Declare producer queue
	if err := producer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		producer.Close()
		return nil, fmt.Errorf("failed to declare query 4 results queue: %v", err)
	}

	return &JoinByUserIdWorker{
		consumer: consumer,
		producer: producer,
		config:   config,
	}, nil
}

// Start starts the join by user ID worker
func (jw *JoinByUserIdWorker) Start() middleware.MessageMiddlewareError {
	fmt.Println("Join by User ID Worker: Starting to listen for transaction chunks...")
	return jw.consumer.StartConsuming(jw.createCallback())
}

// Close closes all connections
func (jw *JoinByUserIdWorker) Close() {
	if jw.consumer != nil {
		jw.consumer.Close()
	}
	if jw.producer != nil {
		jw.producer.Close()
	}
}

// createCallback creates the message processing callback
func (jw *JoinByUserIdWorker) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			if err := jw.processMessage(delivery); err != 0 {
				fmt.Printf("Join by User ID Worker: Failed to process message: %v\n", err)
				delivery.Nack(false, true) // Reject and requeue
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	}
}

// processMessage processes a single transaction chunk
func (jw *JoinByUserIdWorker) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	fmt.Printf("Join by User ID Worker: Received transaction chunk\n")

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("Join by User ID Worker: Failed to deserialize chunk message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Perform join operation
	joinedChunk, err := jw.performJoin(chunkMsg)
	if err != nil {
		fmt.Printf("Join by User ID Worker: Failed to perform join: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send result to query 4 results queue
	if err := jw.sendResult(joinedChunk); err != 0 {
		fmt.Printf("Join by User ID Worker: Failed to send result: %v\n", err)
		return err
	}

	fmt.Printf("Join by User ID Worker: Successfully processed chunk %d\n", chunkMsg.ChunkNumber)
	return 0
}

// performJoin performs the join between transactions and users
func (jw *JoinByUserIdWorker) performJoin(chunkMsg *chunk.Chunk) (*chunk.Chunk, error) {
	fmt.Printf("Join by User ID Worker: Performing join for QueryType: %d, FileID: %s\n",
		chunkMsg.QueryType, chunkMsg.FileID)

	// Parse transaction data
	transactions, err := jw.parseTransactionData(string(chunkMsg.ChunkData))
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction data: %w", err)
	}

	// Perform the join with user data from CSV files
	joinedData := jw.performTransactionUserJoin(transactions)

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

// parseTransactionData parses transaction CSV data
func (jw *JoinByUserIdWorker) parseTransactionData(csvData string) ([]map[string]string, error) {
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

// performTransactionUserJoin performs the join between transactions and users from CSV files
func (jw *JoinByUserIdWorker) performTransactionUserJoin(transactions []map[string]string) string {
	var result strings.Builder

	// Write header
	result.WriteString("transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at,gender,birthdate,registered_at\n")

	// Cache users to avoid repeated lookups
	userCache := make(map[string]*UserData)

	// Process each transaction
	for _, transaction := range transactions {
		userID := transaction["user_id"]

		// Skip transactions with empty user IDs
		if userID == "" {
			fmt.Printf("Join by User ID Worker: Skipping transaction with empty user ID\n")
			continue
		}

		// Check cache first
		user, cached := userCache[userID]
		if !cached {
			// Look up user data from CSV file
			var err error
			user, err = jw.lookupUserFromFile(userID)
			if err != nil {
				fmt.Printf("Join by User ID Worker: Failed to lookup user %s: %v\n", userID, err)
				continue // Skip this transaction
			}
			// Cache the result (even if nil)
			userCache[userID] = user
		}

		if user != nil {
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
		}
		// INNER JOIN: Skip records where there's no match (don't write anything)
	}

	return result.String()
}

// UserData represents a user record from CSV file
type UserData struct {
	UserID       string
	Gender       string
	Birthdate    string
	RegisteredAt string
}

// lookupUserFromFile looks up user data from the appropriate partition file
func (jw *JoinByUserIdWorker) lookupUserFromFile(userID string) (*UserData, error) {
	// Normalize user ID (remove decimal point if present, e.g., "13060.0" -> "13060")
	normalizedUserID := strings.TrimSuffix(userID, ".0")

	// Determine which partition this user belongs to
	partition, err := getUserPartition(normalizedUserID)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition for user %s: %w", normalizedUserID, err)
	}

	// Open the partition file
	filename := fmt.Sprintf("users-partition-%03d.csv", partition)
	filePath := filepath.Join(SharedDataDir, filename)

	file, err := os.Open(filePath)
	if err != nil {
		// If file doesn't exist, user is not in dataset
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to open partition file %s: %w", filePath, err)
	}
	defer file.Close()

	// Read and search for user
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV from file %s: %w", filePath, err)
	}

	// Search for the specific user ID (skip header)
	for i := 1; i < len(records); i++ {
		record := records[i]
		if len(record) >= 4 && record[0] == normalizedUserID {
			return &UserData{
				UserID:       record[0],
				Gender:       record[1],
				Birthdate:    record[2],
				RegisteredAt: record[3],
			}, nil
		}
	}

	// User not found in partition
	return nil, nil
}

// sendResult sends the processed chunk to the results queue
func (jw *JoinByUserIdWorker) sendResult(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Create a chunk message for serialization
	chunkMessage := chunk.NewChunkMessage(chunkMsg)

	// Serialize the chunk message
	resultData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		fmt.Printf("Join by User ID Worker: Failed to serialize result message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send the result to the query 4 results queue
	if err := jw.producer.Send(resultData); err != 0 {
		fmt.Printf("Join by User ID Worker: Failed to send result to queue: %v\n", err)
		return err
	}

	fmt.Printf("Join by User ID Worker: Result sent successfully for ClientID: %s, ChunkNumber: %d\n",
		chunkMsg.ClientID, chunkMsg.ChunkNumber)
	return 0
}
