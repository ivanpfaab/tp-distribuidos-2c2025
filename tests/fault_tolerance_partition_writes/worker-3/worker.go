package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

type Worker struct {
	config         *Config
	consumer       *workerqueue.QueueConsumer
	partitionsDir  string
}

func NewWorker(config *Config) (*Worker, error) {
	// Wait for RabbitMQ to be ready
	log.Println("Worker 3: Waiting for RabbitMQ to be ready...")
	if err := middleware.WaitForConnection(config.RabbitMQ, 30, 1*time.Second); err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	log.Println("Worker 3: RabbitMQ is ready")

	// Create consumer
	consumer := workerqueue.NewQueueConsumer(config.InputQueue, config.RabbitMQ)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create consumer")
	}

	// Declare input queue
	inputQueueDeclarer := workerqueue.NewMessageMiddlewareQueue(config.InputQueue, config.RabbitMQ)
	if inputQueueDeclarer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create input queue declarer")
	}
	if err := inputQueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		inputQueueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare input queue: %d", err)
	}
	inputQueueDeclarer.Close()

	partitionsDir := "/app/worker-data/partitions"

	return &Worker{
		config:        config,
		consumer:      consumer,
		partitionsDir: partitionsDir,
	}, nil
}

func (w *Worker) Start() middleware.MessageMiddlewareError {
	log.Println("Worker 3: Starting")

	return w.consumer.StartConsuming(func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			err := w.processMessage(delivery)
			if err != 0 {
				log.Printf("Worker 3: Error processing message, requeuing")
				delivery.Nack(false, true)
				continue
			}
			delivery.Ack(false)
		}
		done <- nil
	})
}

func (w *Worker) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	// Deserialize chunk
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		log.Printf("Worker 3: Failed to deserialize chunk: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Parse CSV data
	records, err := parseCSV(chunkMsg.ChunkData)
	if err != nil {
		log.Printf("Worker 3: Failed to parse CSV: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Verify each record exists in correct partition file
	allFound := true
	for _, record := range records {
		if len(record) < 2 {
			continue
		}

		userIDStr := record[0]
		userID, err := strconv.Atoi(userIDStr)
		if err != nil {
			log.Printf("Worker 3: Invalid user_id: %s, skipping", userIDStr)
			continue
		}

		partitionNum := userID % w.config.NumPartitions

		// Verify line exists in partition file
		if !w.verifyLineInPartition(chunkMsg.ClientID, partitionNum, record) {
			allFound = false
			log.Printf("Worker 3: Line not found in partition %d: %v", partitionNum, record)
			break
		}
	}

	// Print result
	if allFound {
		fmt.Printf("SUCCESS [%s]\n", chunkMsg.ID)
		log.Printf("Worker 3: SUCCESS [%s]", chunkMsg.ID)
	} else {
		fmt.Printf("FAILURE [%s]\n", chunkMsg.ID)
		log.Printf("Worker 3: FAILURE [%s]", chunkMsg.ID)
	}

	return 0
}

// parseCSV parses CSV text into records
func parseCSV(csvData string) ([][]string, error) {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	// Filter out header and empty records
	result := make([][]string, 0)
	for _, record := range records {
		if len(record) == 0 {
			continue
		}
		// Skip header
		if strings.ToLower(record[0]) == "user_id" {
			continue
		}
		if len(record) >= 2 {
			result = append(result, record)
		}
	}

	return result, nil
}

// verifyLineInPartition checks if a CSV record exists in the partition file
func (w *Worker) verifyLineInPartition(clientID string, partitionNum int, record []string) bool {
	// Build file path
	filename := fmt.Sprintf("%s-users-partition-%03d.csv", clientID, partitionNum)
	filePath := filepath.Join(w.partitionsDir, filename)

	// Open partition file
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		log.Printf("Worker 3: Failed to open partition file %s: %v", filePath, err)
		return false
	}
	defer file.Close()

	// Read all records from file
	reader := csv.NewReader(file)
	fileRecords, err := reader.ReadAll()
	if err != nil {
		log.Printf("Worker 3: Failed to read partition file %s: %v", filePath, err)
		return false
	}

	// Search for the record (skip header)
	for i, fileRecord := range fileRecords {
		if i == 0 {
			// Skip header
			continue
		}
		if recordsEqual(record, fileRecord) {
			return true
		}
	}

	return false
}

// recordsEqual compares two CSV records for equality
func recordsEqual(r1, r2 []string) bool {
	if len(r1) != len(r2) {
		return false
	}
	for i := range r1 {
		if r1[i] != r2[i] {
			return false
		}
	}
	return true
}

func (w *Worker) Close() {
	if w.consumer != nil {
		w.consumer.Close()
	}
}

