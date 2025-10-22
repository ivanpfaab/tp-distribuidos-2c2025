package groupby_blackbox_test

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

const (
	TestClientID = "TEST"
	TestFileID   = "TI01"
)

// TestGroupByQuery2 tests the Query 2 group by system end-to-end
func TestGroupByQuery2(t *testing.T) {
	// Wait for services to be ready
	t.Log("Waiting for services to be ready...")
	time.Sleep(10 * time.Second)

	// Load test data
	testDataPath := "/app/testdata/transaction_items.csv"
	t.Logf("Loading test data from: %s", testDataPath)

	chunks, err := loadTestDataAsChunks(testDataPath, t)
	if err != nil {
		t.Fatalf("Failed to load test data: %v", err)
	}

	t.Logf("Loaded %d chunks from test data", len(chunks))

	// Connect to RabbitMQ and send chunks
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	// Create queue producer to send chunks to the partitioner queue
	producer := workerqueue.NewMessageMiddlewareQueue(queues.Query2GroupByQueue, config)
	if producer == nil {
		t.Fatal("Failed to create queue producer")
	}
	defer producer.Close()

	// Declare the queue to ensure it exists
	if err := producer.DeclareQueue(false, false, false, false); err != 0 {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	t.Logf("Sending %d chunks to queue: %s", len(chunks), queues.Query2GroupByQueue)

	// Send all chunks to the queue
	for i, chunkData := range chunks {
		chunkMsg := chunk.NewChunkMessage(chunkData)
		serialized, err := chunk.SerializeChunkMessage(chunkMsg)
		if err != nil {
			t.Fatalf("Failed to serialize chunk %d: %v", i, err)
		}

		if err := producer.Send(serialized); err != 0 {
			t.Fatalf("Failed to send chunk %d: %v", i, err)
		}

		t.Logf("Sent chunk %d (size: %d bytes, records: ~%d)", i+1, len(serialized), chunkData.ChunkSize)
	}

	t.Log("All chunks sent successfully!")

	// Wait for processing to complete
	t.Log("Waiting for processing to complete...")
	time.Sleep(30 * time.Second)

	// TODO: Add verification logic here
	// - Check that workers received and processed the data
	// - Verify orchestrator has aggregated results
	// - Compare expected vs actual results

	t.Log("Test completed! (Manual verification required)")
}

// loadTestDataAsChunks reads a CSV file and converts it into chunks
func loadTestDataAsChunks(filePath string, t *testing.T) ([]*chunk.Chunk, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open test data file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(records) < 2 {
		return nil, fmt.Errorf("CSV file has insufficient data")
	}

	// First row is header
	header := records[0]
	dataRecords := records[1:]

	t.Logf("CSV has %d data records with schema: %v", len(dataRecords), header)

	// Split data into chunks (e.g., 10 records per chunk)
	chunkSize := 10
	var chunks []*chunk.Chunk

	for i := 0; i < len(dataRecords); i += chunkSize {
		end := i + chunkSize
		if end > len(dataRecords) {
			end = len(dataRecords)
		}

		chunkRecords := dataRecords[i:end]
		chunkNumber := (i / chunkSize) + 1
		isLastChunk := end >= len(dataRecords)

		// Build CSV data for this chunk (with header)
		var csvBuilder strings.Builder
		csvBuilder.WriteString(strings.Join(header, ","))
		csvBuilder.WriteString("\n")

		for _, record := range chunkRecords {
			csvBuilder.WriteString(strings.Join(record, ","))
			csvBuilder.WriteString("\n")
		}

		chunkData := csvBuilder.String()

		// Create chunk
		chunkObj := chunk.NewChunk(
			TestClientID,
			TestFileID,
			2, // QueryType 2
			chunkNumber,
			isLastChunk,
			isLastChunk, // isLastFromTable - for test purposes, same as isLastChunk
			1,           // step
			len(chunkData),
			1, // tableID
			chunkData,
		)

		chunks = append(chunks, chunkObj)
	}

	return chunks, nil
}
