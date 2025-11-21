package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	partitionmanager "github.com/tp-distribuidos-2c2025/shared/partition_manager"
	statemanager "github.com/tp-distribuidos-2c2025/shared/state_manager"
)

type Worker struct {
	config              *Config
	consumer            *workerqueue.QueueConsumer
	producer            *workerqueue.QueueMiddleware
	processedChunks     *statemanager.StateManager
	partitionChunkState *statemanager.StateManager
	partitionManager    *partitionmanager.PartitionManager
}

func NewWorker(config *Config) (*Worker, error) {
	// Wait for RabbitMQ to be ready
	log.Println("Worker 2: Waiting for RabbitMQ to be ready...")
	if err := middleware.WaitForConnection(config.RabbitMQ, 30, 1*time.Second); err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	log.Println("Worker 2: RabbitMQ is ready")

	// Create consumer for input queue
	consumer := workerqueue.NewQueueConsumer(config.InputQueue, config.RabbitMQ)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create consumer")
	}

	// Create producer for output queue
	producer := workerqueue.NewMessageMiddlewareQueue(config.OutputQueue, config.RabbitMQ)
	if producer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create producer")
	}

	// Declare input queue
	inputQueueDeclarer := workerqueue.NewMessageMiddlewareQueue(config.InputQueue, config.RabbitMQ)
	if inputQueueDeclarer == nil {
		consumer.Close()
		producer.Close()
		return nil, fmt.Errorf("failed to create input queue declarer")
	}
	if err := inputQueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		producer.Close()
		inputQueueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare input queue: %d", err)
	}
	inputQueueDeclarer.Close()

	// Declare output queue
	if err := producer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		producer.Close()
		return nil, fmt.Errorf("failed to declare output queue: %d", err)
	}

	// Ensure state directory exists
	if err := os.MkdirAll("/app/worker-data/state", 0755); err != nil {
		consumer.Close()
		producer.Close()
		return nil, fmt.Errorf("failed to create state directory: %w", err)
	}

	// Initialize StateManager for processed chunks
	processedChunks := statemanager.NewStateManager("/app/worker-data/state/processed-chunks.txt")
	if err := processedChunks.LoadProcessedIDs(); err != nil {
		log.Printf("Worker 2: Warning - failed to load processed chunks: %v (starting with empty state)", err)
	} else {
		count := processedChunks.GetProcessedCount()
		log.Printf("Worker 2: Loaded %d processed chunks", count)
	}

	// Initialize StateManager for partition-chunk pairs
	partitionChunkState := statemanager.NewStateManager("/app/worker-data/state/written-partition-chunks.txt")
	if err := partitionChunkState.LoadProcessedIDs(); err != nil {
		log.Printf("Worker 2: Warning - failed to load partition-chunk state: %v (starting with empty state)", err)
	} else {
		count := partitionChunkState.GetProcessedCount()
		log.Printf("Worker 2: Loaded %d partition-chunk pairs", count)
	}

	// Initialize PartitionManager
	partitionManager, err := partitionmanager.NewPartitionManager(
		"/app/worker-data/partitions",
		config.NumPartitions,
	)
	if err != nil {
		consumer.Close()
		producer.Close()
		return nil, fmt.Errorf("failed to create partition manager: %w", err)
	}
	log.Printf("Worker 2: PartitionManager initialized with %d partitions", config.NumPartitions)

	return &Worker{
		config:              config,
		consumer:            consumer,
		producer:            producer,
		processedChunks:     processedChunks,
		partitionChunkState: partitionChunkState,
		partitionManager:    partitionManager,
	}, nil
}

func (w *Worker) Start() middleware.MessageMiddlewareError {
	log.Println("Worker 2: Starting")

	return w.consumer.StartConsuming(func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			err := w.processMessage(delivery)
			if err != 0 {
				log.Printf("Worker 2: Error processing message, requeuing")
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
		log.Printf("Worker 2: Failed to deserialize chunk: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Check if chunk was fully processed
	if w.processedChunks.IsProcessed(chunkMsg.ID) {
		log.Printf("Worker 2: Chunk %s already processed, skipping", chunkMsg.ID)
		return 0
	}

	log.Printf("Worker 2: Processing chunk %s", chunkMsg.ID)

	// Parse CSV data
	records, err := parseCSV(chunkMsg.ChunkData)
	if err != nil {
		log.Printf("Worker 2: Failed to parse CSV: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Partition data
	partitions := partitionData(records, w.config.NumPartitions)

	// Write each partition
	for _, partition := range partitions {
		partitionChunkKey := fmt.Sprintf("%d:%s", partition.Number, chunkMsg.ID)

		// Check if this partition-chunk pair was already written
		if w.partitionChunkState.IsProcessed(partitionChunkKey) {
			log.Printf("Worker 2: Partition %d from chunk %s already written, skipping", partition.Number, chunkMsg.ID)
			continue
		}

		// Write partition
		opts := partitionmanager.WriteOptions{
			FilePrefix: "users-partition",
			Header:     []string{"user_id", "name"},
			ClientID:   chunkMsg.ClientID,
			DebugMode:  w.config.DebugMode,
		}

		if err := w.partitionManager.WritePartition(partition, chunkMsg.ID, opts); err != nil {
			log.Printf("Worker 2: Failed to write partition %d: %v", partition.Number, err)
			return middleware.MessageMiddlewareMessageError
		}

		// Mark partition-chunk pair as written
		if err := w.partitionChunkState.MarkProcessed(partitionChunkKey); err != nil {
			log.Printf("Worker 2: Failed to mark partition-chunk pair: %v", err)
			return middleware.MessageMiddlewareMessageError
		}

		log.Printf("Worker 2: Wrote partition %d from chunk %s", partition.Number, chunkMsg.ID)
	}

	// Forward chunk to Worker 3
	chunkMessage := chunk.NewChunkMessage(chunkMsg)
	serialized, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		log.Printf("Worker 2: Failed to serialize chunk: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	if err := w.producer.Send(serialized); err != 0 {
		log.Printf("Worker 2: Failed to send chunk: %v", err)
		return err
	}

	// Mark chunk as fully processed
	if err := w.processedChunks.MarkProcessed(chunkMsg.ID); err != nil {
		log.Printf("Worker 2: Failed to mark chunk as processed: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	log.Printf("Worker 2: Completed processing chunk %s", chunkMsg.ID)
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

// partitionData partitions records by user_id % numPartitions
func partitionData(records [][]string, numPartitions int) []partitionmanager.PartitionData {
	partitions := make(map[int][]string)

	for _, record := range records {
		if len(record) < 2 {
			continue
		}

		userIDStr := record[0]
		userID, err := strconv.Atoi(userIDStr)
		if err != nil {
			log.Printf("Worker 2: Invalid user_id: %s, skipping", userIDStr)
			continue
		}

		partitionNum := userID % numPartitions

		// Convert record to CSV line
		csvLine := strings.Join(record, ",") + "\n"
		partitions[partitionNum] = append(partitions[partitionNum], csvLine)
	}

	// Convert to PartitionData slice
	result := make([]partitionmanager.PartitionData, 0, len(partitions))
	for num, lines := range partitions {
		result = append(result, partitionmanager.PartitionData{
			Number: num,
			Lines:  lines,
		})
	}

	return result
}

func (w *Worker) Close() {
	if w.processedChunks != nil {
		w.processedChunks.Close()
	}
	if w.partitionChunkState != nil {
		w.partitionChunkState.Close()
	}
	if w.consumer != nil {
		w.consumer.Close()
	}
	if w.producer != nil {
		w.producer.Close()
	}
}
