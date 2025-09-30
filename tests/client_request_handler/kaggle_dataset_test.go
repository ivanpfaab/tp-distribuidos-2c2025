package main

import (
	"encoding/csv"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	batch "github.com/tp-distribuidos-2c2025/protocol/batch"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
)

// CoffeeShopTransaction represents a single transaction record
type CoffeeShopTransaction struct {
	TransactionID string
	Timestamp     string
	CustomerID    string
	ProductID     string
	ProductName   string
	Category      string
	Quantity      int
	UnitPrice     float64
	TotalAmount   float64
	PaymentMethod string
	StoreID       string
	StoreName     string
	City          string
	Country       string
}

// TestKaggleDatasetProcessing tests processing the coffee shop transaction dataset
func TestKaggleDatasetProcessing(t *testing.T) {
	testing_utils.InitLogger()
	testing_utils.LogTest("Testing Kaggle Coffee Shop Dataset Processing")

	t.Run("Generate and Process Dataset", testGenerateAndProcessDataset)
	t.Run("Process Real Dataset File", testProcessRealDatasetFile)
}

// testGenerateAndProcessDataset generates a simulated dataset and processes it
func testGenerateAndProcessDataset(t *testing.T) {
	testing_utils.LogStep("Generating simulated coffee shop transaction dataset")

	// Configuration
	batchSize := 1000
	chunkSize := 1000
	totalRecords := 50000 // Simulate 50k records
	fileID := "C202"      // Max 4 bytes

	// Generate simulated dataset
	transactions := generateSimulatedTransactions(totalRecords)
	testing_utils.LogStep("Generated %d simulated transactions", len(transactions))

	// Process the dataset
	processDataset(t, transactions, fileID, batchSize, chunkSize)
}

// TestConnection holds the connection and queue for processing multiple files
type TestConnection struct {
	conn           net.Conn
	consumer       *workerqueue.QueueConsumer
	receivedChunks chan *chunk.Chunk
	chunkCount     int
	expectedChunks int
}

// testProcessRealDatasetFile processes all real dataset files if available
func testProcessRealDatasetFile(t *testing.T) {
	testing_utils.LogStep("Attempting to process all real dataset files")

	// Define all possible dataset directory paths
	datasetPaths := []string{
		"/app/data/", // Docker container mounted volume path
		"../data/",   // Local testing path
	}

	var basePath string
	for _, path := range datasetPaths {
		if _, err := os.Stat(path); err == nil {
			basePath = path
			break
		}
	}

	if basePath == "" {
		t.Skipf("No dataset directory found. Please place the dataset in one of: %v", datasetPaths)
		return
	}

	testing_utils.LogStep("Found dataset directory: %s", basePath)

	// Find all CSV files in the dataset directory
	csvFiles, err := findCSVFiles(basePath)
	if err != nil {
		t.Fatalf("Failed to find CSV files: %v", err)
	}

	if len(csvFiles) == 0 {
		t.Skip("No CSV files found in dataset directory.")
		return
	}

	testing_utils.LogStep("Found %d CSV files: %v", len(csvFiles), csvFiles)

	// Setup single connection and queue for all files
	testConn, err := setupTestConnection(t)
	if err != nil {
		t.Fatalf("Failed to setup test connection: %v", err)
	}
	defer testConn.cleanup()

	// Process each CSV file through the same connection
	totalRecords := 0
	for i, csvFile := range csvFiles {
		fileID := fmt.Sprintf("F%03d", i+1) // F001, F002, etc.
		testing_utils.LogStep("Processing file %d/%d: %s (FileID: %s)", i+1, len(csvFiles), csvFile, fileID)

		// Load and process this specific file
		transactions, err := loadDatasetFromFile(csvFile)
		if err != nil {
			testing_utils.LogStep("Warning: Failed to load %s: %v", csvFile, err)
			continue
		}

		if len(transactions) == 0 {
			testing_utils.LogStep("Warning: No data in %s", csvFile)
			continue
		}

		testing_utils.LogStep("Loaded %d records from %s", len(transactions), csvFile)
		totalRecords += len(transactions)

		// Process this file's data through the existing connection
		processDatasetWithConnection(t, testConn, transactions, fileID, 1000, 1000)
	}

	testing_utils.LogSuccess("Processed all dataset files - Total records: %d", totalRecords)
}

// setupTestConnection creates a single connection and queue for processing all files
func setupTestConnection(t *testing.T) (*TestConnection, error) {
	testing_utils.LogStep("Setting up test connection and queue")

	// Create RabbitMQ consumer for monitoring chunks
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	consumer := workerqueue.NewQueueConsumer("query-orchestrator-queue", config)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create RabbitMQ consumer")
	}

	// Channel to collect received chunks
	receivedChunks := make(chan *chunk.Chunk, 1000)

	// Start consuming chunks in a goroutine
	go func() {
		onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
			for delivery := range *consumeChannel {
				// Deserialize the chunk message
				message, err := deserializer.Deserialize(delivery.Body)
				if err != nil {
					testing_utils.LogStep("Failed to deserialize chunk: %v", err)
					delivery.Ack(false)
					continue
				}

				// Check if it's a Chunk message
				chunkMsg, ok := message.(*chunk.Chunk)
				if !ok {
					testing_utils.LogStep("Received non-chunk message: %T", message)
					delivery.Ack(false)
					continue
				}

				// Process all chunks
				receivedChunks <- chunkMsg
				testing_utils.LogStep("Received chunk: ClientID=%s, ChunkNumber=%d, Step=%d, IsLastChunk=%t",
					chunkMsg.ClientID, chunkMsg.ChunkNumber, chunkMsg.Step, chunkMsg.IsLastChunk)

				// Acknowledge the message
				delivery.Ack(false)
			}
			done <- nil
		}

		if err := consumer.StartConsuming(onMessageCallback); err != 0 {
			testing_utils.LogStep("Failed to start consuming: %v", err)
		}
	}()

	// Wait for consumer to start
	testing_utils.LogStep("Waiting for consumer to start...")
	time.Sleep(2 * time.Second)

	// Connect to server
	testing_utils.LogStep("Connecting to server")
	conn, err := net.Dial("tcp", "server:8080")
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("test server not available: %v", err)
	}

	// Set read/write timeouts
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	conn.SetWriteDeadline(time.Now().Add(30 * time.Second))

	return &TestConnection{
		conn:           conn,
		consumer:       consumer,
		receivedChunks: receivedChunks,
		chunkCount:     0,
		expectedChunks: 0,
	}, nil
}

// cleanup closes the connection and consumer
func (tc *TestConnection) cleanup() {
	if tc.conn != nil {
		tc.conn.Close()
	}
	if tc.consumer != nil {
		tc.consumer.Close()
	}
	close(tc.receivedChunks)
}

// processDatasetWithConnection processes a dataset using an existing connection and queue
func processDatasetWithConnection(t *testing.T, testConn *TestConnection, transactions []CoffeeShopTransaction, fileID string, batchSize, chunkSize int) {
	testing_utils.LogStep("Processing dataset with existing connection - %d transactions, batch size: %d, chunk size: %d",
		len(transactions), batchSize, chunkSize)

	// Start timing
	startTime := time.Now()
	testing_utils.LogStep("Starting dataset processing at %s", startTime.Format("2006-01-02 15:04:05"))

	// Process transactions in batches
	totalBatches := (len(transactions) + batchSize - 1) / batchSize
	testConn.expectedChunks += totalBatches

	testing_utils.LogStep("Processing %d transactions in %d batches", len(transactions), totalBatches)

	for batchNum := 0; batchNum < totalBatches; batchNum++ {
		startIdx := batchNum * batchSize
		endIdx := startIdx + batchSize
		if endIdx > len(transactions) {
			endIdx = len(transactions)
		}

		batchTransactions := transactions[startIdx:endIdx]
		isLastBatch := batchNum == totalBatches-1

		// Convert batch to CSV string
		batchData := convertTransactionsToCSV(batchTransactions)

		testing_utils.LogStep("Sending batch %d/%d (%d transactions) - Size: %d bytes",
			batchNum+1, totalBatches, len(batchTransactions), len(batchData))

		// Create batch message
		testBatch := &batch.Batch{
			ClientID:    "KAG", // Kaggle dataset client (max 4 bytes)
			FileID:      fileID,
			IsEOF:       isLastBatch,
			BatchNumber: batchNum + 1,
			BatchSize:   len(batchData),
			BatchData:   batchData,
		}

		// Serialize and send
		batchMsg := batch.NewBatchMessage(testBatch)
		serializedData, err := batch.SerializeBatchMessage(batchMsg)
		if err != nil {
			t.Fatalf("Failed to serialize batch %d: %v", batchNum+1, err)
		}

		// Send batch
		_, err = testConn.conn.Write(serializedData)
		if err != nil {
			t.Fatalf("Failed to send batch %d: %v", batchNum+1, err)
		}

		// Read acknowledgment
		response := make([]byte, 1024)
		// Update read deadline for each batch
		testConn.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		testConn.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		n, err := testConn.conn.Read(response)
		if err != nil {
			testing_utils.LogStep("Failed to read response for batch %d: %v", batchNum+1, err)
			// Try to reconnect if connection was lost
			testing_utils.LogStep("Attempting to reconnect...")
			testConn.conn.Close()
			conn, err := net.Dial("tcp", "server:8080")
			if err != nil {
				testing_utils.LogStep("Failed to reconnect: %v", err)
				continue
			}
			testConn.conn = conn
			testConn.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			testConn.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
			testing_utils.LogStep("Reconnected successfully")
			continue
		}

		responseStr := string(response[:n])
		testing_utils.LogStep("Batch %d/%d processed: %s", batchNum+1, totalBatches, responseStr)

		// Progress update every 10 batches
		if (batchNum+1)%10 == 0 || isLastBatch {
			elapsed := time.Since(startTime)
			rate := float64(batchNum+1) / elapsed.Seconds()
			testing_utils.LogStep("Progress: %d/%d batches (%.1f%%) - Rate: %.2f batches/sec - Elapsed: %v",
				batchNum+1, totalBatches, float64(batchNum+1)/float64(totalBatches)*100, rate, elapsed)
		}

		// Small delay between batches
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for all chunks to be processed
	testing_utils.LogStep("Waiting for all chunks to be processed for file %s", fileID)

	// Wait for chunks with timeout
	timeout := time.After(60 * time.Second)
	chunksReceived := 0
	expectedChunksForFile := totalBatches

	for chunksReceived < expectedChunksForFile {
		select {
		case chunk, ok := <-testConn.receivedChunks:
			if !ok {
				testing_utils.LogStep("Chunk channel closed")
				break
			}
			
			chunksReceived++
			testConn.chunkCount++
			testing_utils.LogStep("Received chunk %d/%d for file %s: ClientID=%s, ChunkNumber=%d, Step=%d, IsLastChunk=%t",
				chunksReceived, expectedChunksForFile, fileID, chunk.ClientID, chunk.ChunkNumber, chunk.Step, chunk.IsLastChunk)

			// Verify chunk properties
			if chunk.Step != 0 {
				t.Errorf("Expected chunk step to be 0, got %d", chunk.Step)
			}
			if chunk.QueryType != 1 {
				t.Errorf("Expected query type to be 1, got %d", chunk.QueryType)
			}
			if chunk.TableID != 1 {
				t.Errorf("Expected table ID to be 1, got %d", chunk.TableID)
			}

		case <-timeout:
			testing_utils.LogStep("Timeout waiting for chunks for file %s. Received %d/%d", fileID, chunksReceived, expectedChunksForFile)
			break
		}
	}

	if chunksReceived >= expectedChunksForFile {
		testing_utils.LogSuccess("All expected chunks received for file %s!", fileID)
	}

	// Calculate final statistics
	totalTime := time.Since(startTime)
	processingRate := float64(len(transactions)) / totalTime.Seconds()

	testing_utils.LogSuccess("File %s processing completed - Records: %d, Time: %v, Rate: %.2f records/sec",
		fileID, len(transactions), totalTime, processingRate)
}

// generateSimulatedTransactions creates simulated coffee shop transaction data
func generateSimulatedTransactions(count int) []CoffeeShopTransaction {
	transactions := make([]CoffeeShopTransaction, count)

	products := []string{"Espresso", "Latte", "Cappuccino", "Americano", "Mocha", "Frappuccino", "Tea", "Pastry", "Sandwich", "Salad"}
	categories := []string{"Beverage", "Food", "Dessert"}
	paymentMethods := []string{"Credit Card", "Cash", "Mobile Payment", "Gift Card"}
	stores := []string{"Downtown Store", "Mall Location", "Airport Store", "University Store", "Suburban Store"}
	cities := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"}
	countries := []string{"USA", "Canada", "Mexico"}

	for i := 0; i < count; i++ {
		product := products[i%len(products)]
		category := categories[i%len(categories)]
		quantity := (i % 5) + 1
		unitPrice := 2.50 + float64(i%20)*0.25
		totalAmount := unitPrice * float64(quantity)

		transactions[i] = CoffeeShopTransaction{
			TransactionID: fmt.Sprintf("TXN_%06d", i+1),
			Timestamp:     time.Now().Add(-time.Duration(i) * time.Minute).Format("2006-01-02 15:04:05"),
			CustomerID:    fmt.Sprintf("CUST_%05d", (i%1000)+1),
			ProductID:     fmt.Sprintf("PROD_%03d", (i%len(products))+1),
			ProductName:   product,
			Category:      category,
			Quantity:      quantity,
			UnitPrice:     unitPrice,
			TotalAmount:   totalAmount,
			PaymentMethod: paymentMethods[i%len(paymentMethods)],
			StoreID:       fmt.Sprintf("STORE_%02d", (i%len(stores))+1),
			StoreName:     stores[i%len(stores)],
			City:          cities[i%len(cities)],
			Country:       countries[i%len(countries)],
		}
	}

	return transactions
}

// findCSVFiles finds all CSV files in a directory
func findCSVFiles(basePath string) ([]string, error) {
	var csvFiles []string

	err := filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasSuffix(strings.ToLower(path), ".csv") {
			csvFiles = append(csvFiles, path)
		}

		return nil
	})

	return csvFiles, err
}

// loadDatasetFromFile loads a real dataset from a CSV file
// This function is flexible and can handle different CSV schemas
func loadDatasetFromFile(filePath string) ([]CoffeeShopTransaction, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) < 2 {
		return nil, fmt.Errorf("dataset file is empty or has no data rows")
	}

	// Get the header to understand the schema
	header := records[0]
	testing_utils.LogStep("CSV Schema for %s: %v", filePath, header)

	// Skip header row
	transactions := make([]CoffeeShopTransaction, 0, len(records)-1)

	for i, record := range records[1:] { // Skip header
		// Create a generic transaction record that works with any CSV schema
		// We'll use the row number and file info to create meaningful IDs
		transaction := CoffeeShopTransaction{
			TransactionID: fmt.Sprintf("TXN_%s_%06d", filepath.Base(filePath), i+1),
			Timestamp:     "2023-07-01 12:00:00",                // Default timestamp
			CustomerID:    fmt.Sprintf("CUST_%06d", (i%1000)+1), // Cycle through customers
			ProductID:     fmt.Sprintf("PROD_%03d", (i%100)+1),  // Cycle through products
			ProductName:   fmt.Sprintf("Item_%d", i+1),
			Category:      "Data",
			Quantity:      1,
			UnitPrice:     10.0,
			TotalAmount:   10.0,
			PaymentMethod: "Credit Card",
			StoreID:       "ST01",
			StoreName:     "Store",
			City:          "City",
			Country:       "Country",
		}

		// Try to map actual CSV data to our transaction structure
		// This is a generic mapping that works with any CSV schema
		for j, value := range record {
			if j >= len(header) {
				break
			}

			columnName := strings.ToLower(header[j])

			// Map common column names to our transaction fields
			switch {
			case strings.Contains(columnName, "id") && strings.Contains(columnName, "transaction"):
				transaction.TransactionID = value
			case strings.Contains(columnName, "timestamp") || strings.Contains(columnName, "date") || strings.Contains(columnName, "time"):
				transaction.Timestamp = value
			case strings.Contains(columnName, "customer"):
				transaction.CustomerID = value
			case strings.Contains(columnName, "product") && strings.Contains(columnName, "id"):
				transaction.ProductID = value
			case strings.Contains(columnName, "product") && strings.Contains(columnName, "name"):
				transaction.ProductName = value
			case strings.Contains(columnName, "category"):
				transaction.Category = value
			case strings.Contains(columnName, "quantity"):
				if qty, err := strconv.Atoi(value); err == nil {
					transaction.Quantity = qty
				}
			case strings.Contains(columnName, "price") && !strings.Contains(columnName, "total"):
				if price, err := strconv.ParseFloat(value, 64); err == nil {
					transaction.UnitPrice = price
				}
			case strings.Contains(columnName, "total") || strings.Contains(columnName, "amount"):
				if amount, err := strconv.ParseFloat(value, 64); err == nil {
					transaction.TotalAmount = amount
				}
			case strings.Contains(columnName, "payment"):
				transaction.PaymentMethod = value
			case strings.Contains(columnName, "store") && strings.Contains(columnName, "id"):
				transaction.StoreID = value
			case strings.Contains(columnName, "store") && strings.Contains(columnName, "name"):
				transaction.StoreName = value
			case strings.Contains(columnName, "city"):
				transaction.City = value
			case strings.Contains(columnName, "country"):
				transaction.Country = value
			}
		}

		// Ensure we have valid data
		if transaction.TransactionID == "" {
			transaction.TransactionID = fmt.Sprintf("TXN_%s_%06d", filepath.Base(filePath), i+1)
		}
		if transaction.CustomerID == "" {
			transaction.CustomerID = fmt.Sprintf("CUST_%06d", (i%1000)+1)
		}
		if transaction.ProductID == "" {
			transaction.ProductID = fmt.Sprintf("PROD_%03d", (i%100)+1)
		}

		transactions = append(transactions, transaction)
	}

	testing_utils.LogStep("Successfully loaded %d records from %s", len(transactions), filePath)
	return transactions, nil
}

// processDataset processes the dataset through the system
func processDataset(t *testing.T, transactions []CoffeeShopTransaction, fileID string, batchSize, chunkSize int) {
	testing_utils.LogStep("Starting dataset processing - %d transactions, batch size: %d, chunk size: %d",
		len(transactions), batchSize, chunkSize)

	// Create RabbitMQ consumer for monitoring chunks
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	consumer := workerqueue.NewQueueConsumer("query-orchestrator-queue", config)
	if consumer == nil {
		t.Skipf("Skipping test - failed to create RabbitMQ consumer")
		return
	}
	defer consumer.Close()

	// Channel to collect received chunks
	receivedChunks := make(chan *chunk.Chunk, 1000)
	chunkCount := 0
	expectedChunks := 0

	// Start consuming chunks in a goroutine
	go func() {
		onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
			for delivery := range *consumeChannel {
				// Deserialize the chunk message
				message, err := deserializer.Deserialize(delivery.Body)
				if err != nil {
					testing_utils.LogStep("Failed to deserialize chunk: %v", err)
					delivery.Ack(false)
					continue
				}

				// Check if it's a Chunk message
				chunkMsg, ok := message.(*chunk.Chunk)
				if !ok {
					testing_utils.LogStep("Received non-chunk message: %T", message)
					delivery.Ack(false)
					continue
				}

				// Process all chunks (remove file ID filtering for now)
				receivedChunks <- chunkMsg
				chunkCount++
				testing_utils.LogStep("Received chunk %d: ClientID=%s, ChunkNumber=%d, Step=%d, IsLastChunk=%t",
					chunkCount, chunkMsg.ClientID, chunkMsg.ChunkNumber, chunkMsg.Step, chunkMsg.IsLastChunk)

				// Acknowledge the message
				delivery.Ack(false)
			}
			done <- nil
		}

		if err := consumer.StartConsuming(onMessageCallback); err != 0 {
			testing_utils.LogStep("Failed to start consuming: %v", err)
		}
	}()

	// Wait for consumer to start
	testing_utils.LogStep("Waiting for consumer to start...")
	time.Sleep(2 * time.Second)
	testing_utils.LogStep("Consumer should be ready now")

	// Connect to server
	testing_utils.LogStep("Connecting to server")
	conn, err := net.Dial("tcp", "server:8080")
	if err != nil {
		t.Skipf("Skipping test - test server not available: %v", err)
		return
	}

	// Set read/write timeouts
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	conn.SetWriteDeadline(time.Now().Add(30 * time.Second))

	// Start timing
	startTime := time.Now()
	testing_utils.LogStep("Starting dataset processing at %s", startTime.Format("2006-01-02 15:04:05"))

	// Process transactions in batches
	totalBatches := (len(transactions) + batchSize - 1) / batchSize
	expectedChunks = totalBatches

	testing_utils.LogStep("Processing %d transactions in %d batches", len(transactions), totalBatches)

	for batchNum := 0; batchNum < totalBatches; batchNum++ {
		startIdx := batchNum * batchSize
		endIdx := startIdx + batchSize
		if endIdx > len(transactions) {
			endIdx = len(transactions)
		}

		batchTransactions := transactions[startIdx:endIdx]
		isLastBatch := batchNum == totalBatches-1

		// Convert batch to CSV string
		batchData := convertTransactionsToCSV(batchTransactions)

		testing_utils.LogStep("Sending batch %d/%d (%d transactions) - Size: %d bytes",
			batchNum+1, totalBatches, len(batchTransactions), len(batchData))

		// Create batch message
		testBatch := &batch.Batch{
			ClientID:    "KAG", // Kaggle dataset client (max 4 bytes)
			FileID:      fileID,
			IsEOF:       isLastBatch,
			BatchNumber: batchNum + 1,
			BatchSize:   len(batchData),
			BatchData:   batchData,
		}

		// Serialize and send
		batchMsg := batch.NewBatchMessage(testBatch)
		serializedData, err := batch.SerializeBatchMessage(batchMsg)
		if err != nil {
			t.Fatalf("Failed to serialize batch %d: %v", batchNum+1, err)
		}

		// Send batch
		_, err = conn.Write(serializedData)
		if err != nil {
			t.Fatalf("Failed to send batch %d: %v", batchNum+1, err)
		}

		// Read acknowledgment
		response := make([]byte, 1024)
		// Update read deadline for each batch
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		n, err := conn.Read(response)
		if err != nil {
			testing_utils.LogStep("Failed to read response for batch %d: %v", batchNum+1, err)
			// Try to reconnect if connection was lost
			testing_utils.LogStep("Attempting to reconnect...")
			conn.Close()
			conn, err = net.Dial("tcp", "server:8080")
			if err != nil {
				testing_utils.LogStep("Failed to reconnect: %v", err)
				continue
			}
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
			testing_utils.LogStep("Reconnected successfully")
			continue
		}

		responseStr := string(response[:n])
		testing_utils.LogStep("Batch %d/%d processed: %s", batchNum+1, totalBatches, responseStr)

		// Progress update every 10 batches
		if (batchNum+1)%10 == 0 || isLastBatch {
			elapsed := time.Since(startTime)
			rate := float64(batchNum+1) / elapsed.Seconds()
			testing_utils.LogStep("Progress: %d/%d batches (%.1f%%) - Rate: %.2f batches/sec - Elapsed: %v",
				batchNum+1, totalBatches, float64(batchNum+1)/float64(totalBatches)*100, rate, elapsed)
		}

		// Small delay between batches
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for all chunks to be processed
	testing_utils.LogStep("Waiting for all chunks to be processed")

	for {
		for chunk := range receivedChunks {
			chunkCount++
			testing_utils.LogStep("Received chunk %d/%d: ClientID=%s, ChunkNumber=%d, Step=%d, IsLastChunk=%t",
				chunkCount, expectedChunks, chunk.ClientID, chunk.ChunkNumber, chunk.Step, chunk.IsLastChunk)

			// Verify chunk properties
			if chunk.Step != 0 {
				t.Errorf("Expected chunk step to be 0, got %d", chunk.Step)
			}
			if chunk.QueryType != 1 {
				t.Errorf("Expected query type to be 1, got %d", chunk.QueryType)
			}
			if chunk.TableID != 1 {
				t.Errorf("Expected table ID to be 1, got %d", chunk.TableID)
			}

			if chunkCount >= expectedChunks {
				testing_utils.LogSuccess("All expected chunks received!")
				goto final_stats
			}
		}
	}

final_stats:

	// Calculate final statistics
	totalTime := time.Since(startTime)
	processingRate := float64(len(transactions)) / totalTime.Seconds()
	batchRate := float64(totalBatches) / totalTime.Seconds()

	testing_utils.LogSuccess("Dataset processing completed!")
	testing_utils.LogStep("Final Statistics:")
	testing_utils.LogStep("  Total Records: %d", len(transactions))
	testing_utils.LogStep("  Total Batches: %d", totalBatches)
	testing_utils.LogStep("  Total Chunks: %d", chunkCount)
	testing_utils.LogStep("  Total Time: %v", totalTime)
	testing_utils.LogStep("  Processing Rate: %.2f records/sec", processingRate)
	testing_utils.LogStep("  Batch Rate: %.2f batches/sec", batchRate)
	testing_utils.LogStep("  Average Batch Size: %.1f records", float64(len(transactions))/float64(totalBatches))

	conn.Close()
}

// convertTransactionsToCSV converts a slice of transactions to CSV format
func convertTransactionsToCSV(transactions []CoffeeShopTransaction) string {
	var csv strings.Builder

	// Write header
	csv.WriteString("TransactionID,Timestamp,CustomerID,ProductID,ProductName,Category,Quantity,UnitPrice,TotalAmount,PaymentMethod,StoreID,StoreName,City,Country\n")

	// Write data rows
	for _, txn := range transactions {
		csv.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,%d,%.2f,%.2f,%s,%s,%s,%s,%s\n",
			txn.TransactionID, txn.Timestamp, txn.CustomerID, txn.ProductID, txn.ProductName,
			txn.Category, txn.Quantity, txn.UnitPrice, txn.TotalAmount, txn.PaymentMethod,
			txn.StoreID, txn.StoreName, txn.City, txn.Country))
	}

	return csv.String()
}

// TestDatasetPerformance tests the performance of dataset processing
func TestDatasetPerformance(t *testing.T) {
	testing_utils.InitLogger()
	testing_utils.LogTest("Testing dataset processing performance")

	// Test different batch sizes
	batchSizes := []int{100, 500, 1000, 2000}
	recordCounts := []int{1000, 5000, 10000}

	for _, recordCount := range recordCounts {
		for _, batchSize := range batchSizes {
			t.Run(fmt.Sprintf("Records_%d_BatchSize_%d", recordCount, batchSize), func(t *testing.T) {
				transactions := generateSimulatedTransactions(recordCount)
				processDataset(t, transactions, fmt.Sprintf("P%d", recordCount), batchSize, 1000)
			})
		}
	}
}
