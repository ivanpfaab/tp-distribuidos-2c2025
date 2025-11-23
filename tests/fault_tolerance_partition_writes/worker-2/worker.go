package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	partitionmanager "github.com/tp-distribuidos-2c2025/shared/partition_manager"
)

type Worker struct {
	config              *Config
	consumer            *workerqueue.QueueConsumer
	producer            *workerqueue.QueueMiddleware
	processedChunks     *messagemanager.MessageManager
	partitionChunkState *messagemanager.MessageManager
	partitionManager    *partitionmanager.PartitionManager
	recoveryState       map[string]bool // "chunkID" -> needs duplicate check for all partitions
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

	// Initialize MessageManager for processed chunks
	processedChunks := messagemanager.NewMessageManager("/app/worker-data/state/processed-chunks.txt")
	if err := processedChunks.LoadProcessedIDs(); err != nil {
		log.Printf("Worker 2: Warning - failed to load processed chunks: %v (starting with empty state)", err)
	} else {
		count := processedChunks.GetProcessedCount()
		log.Printf("Worker 2: Loaded %d processed chunks", count)
	}

	// Initialize MessageManager for partition-chunk pairs
	partitionChunkState := messagemanager.NewMessageManager("/app/worker-data/state/written-partition-chunks.txt")
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

	worker := &Worker{
		config:              config,
		consumer:            consumer,
		producer:            producer,
		processedChunks:     processedChunks,
		partitionChunkState: partitionChunkState,
		partitionManager:    partitionManager,
		recoveryState:       make(map[string]bool),
	}

	// On startup, check and fix any incomplete partition writes
	if err := worker.recoverIncompleteWrites(); err != nil {
		consumer.Close()
		producer.Close()
		return nil, fmt.Errorf("failed to recover incomplete writes: %w", err)
	}

	return worker, nil
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

		opts := partitionmanager.WriteOptions{
			FilePrefix: "users-partition",
			Header:     []string{"user_id", "name"},
			ClientID:   chunkMsg.ClientID,
			DebugMode:  w.config.DebugMode,
		}

		// Check if this chunk needs duplicate checking (was partially written)
		// This check must come BEFORE the state file check, because recoveryState indicates
		// a potentially partial write that needs verification even if marked in state file
		if w.recoveryState[chunkMsg.ID] {
			// This chunk was partially written - need to check for duplicates in ALL partitions
			filePath := w.partitionManager.GetPartitionFilePath(opts, partition.Number)
			lastLines, err := w.partitionManager.GetLastLines(filePath, 2)
			if err != nil {
				log.Printf("Worker 2: Failed to get last lines for partition %d: %v", partition.Number, err)
				return middleware.MessageMiddlewareMessageError
			}

			if err := w.partitionManager.WriteOnlyMissingLines(filePath, lastLines, partition.Lines, opts); err != nil {
				log.Printf("Worker 2: Failed to write missing lines to partition %d: %v", partition.Number, err)
				return middleware.MessageMiddlewareMessageError
			}

			log.Printf("Worker 2: Wrote missing lines to partition %d from chunk %s (recovery)", partition.Number, chunkMsg.ID)
		} else {
			// Check if this partition-chunk pair was already written (normal duplicate check)
			if w.partitionChunkState.IsProcessed(partitionChunkKey) {
				log.Printf("Worker 2: Partition %d from chunk %s already written, skipping", partition.Number, chunkMsg.ID)
				continue
			}

			// Normal write (WritePartition handles incomplete writes automatically)
			if err := w.partitionManager.WritePartition(partition, opts); err != nil {
				log.Printf("Worker 2: Failed to write partition %d: %v", partition.Number, err)
				return middleware.MessageMiddlewareMessageError
			}

			log.Printf("Worker 2: Wrote partition %d from chunk %s", partition.Number, chunkMsg.ID)
		}

		// Mark partition-chunk pair as written
		if err := w.partitionChunkState.MarkProcessed(partitionChunkKey); err != nil {
			log.Printf("Worker 2: Failed to mark partition-chunk pair: %v", err)
			return middleware.MessageMiddlewareMessageError
		}
	}

	// If this chunk was in recovery state, remove it now (all partitions processed)
	if w.recoveryState[chunkMsg.ID] {
		delete(w.recoveryState, chunkMsg.ID)
		log.Printf("Worker 2: Completed recovery for chunk %s", chunkMsg.ID)
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

// recoverIncompleteWrites checks all partition files for incomplete writes and fixes them on startup
// It also populates recoveryState with partition-chunk pairs that were partially written
func (w *Worker) recoverIncompleteWrites() error {
	log.Println("Worker 2: Checking for incomplete partition writes...")

	// First, fix incomplete last lines in all partition files
	fixedCount, err := w.partitionManager.RecoverIncompleteWrites()
	if err != nil {
		return fmt.Errorf("failed to recover incomplete writes: %w", err)
	}

	if fixedCount > 0 {
		log.Printf("Worker 2: Fixed %d incomplete last lines", fixedCount)
	}

	// Now, identify partition-chunk pairs that were partially written
	// We need to find the LAST chunk that wrote to each partition (only that one might be partial)
	// Read written-partition-chunks.txt to find which partitions exist
	file, err := os.Open("/app/worker-data/state/written-partition-chunks.txt")
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("Worker 2: No partition state file found, no recovery needed")
			return nil
		}
		return fmt.Errorf("failed to open state file: %w", err)
	}
	defer file.Close()

	// First pass: collect all partition-chunk pairs and find the last chunk per (clientID, partition)
	// Map: "clientID:partitionNum" -> (chunkID, chunkNumber)
	lastChunkPerPartition := make(map[string]struct {
		chunkID     string
		chunkNumber int
	})

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Format: "partitionNum:chunkID" where chunkID is "CLIENTID%08d"
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		partitionNum := parts[0]
		chunkID := parts[1]

		// Extract client ID and chunk number from chunkID
		if len(chunkID) < 8 {
			continue
		}
		clientID := chunkID[:4]
		chunkNumberStr := chunkID[len(chunkID)-8:]
		chunkNumber, err := strconv.Atoi(chunkNumberStr)
		if err != nil {
			continue
		}

		// Track the last (highest) chunk number for each (clientID, partition) pair
		key := fmt.Sprintf("%s:%s", clientID, partitionNum)
		if last, exists := lastChunkPerPartition[key]; !exists || chunkNumber > last.chunkNumber {
			lastChunkPerPartition[key] = struct {
				chunkID     string
				chunkNumber int
			}{chunkID: chunkID, chunkNumber: chunkNumber}
		}
	}

	// Second pass: collect all unique chunks that are "last chunk" for any partition
	// and check if any of their partitions have data (indicating potential partial write)
	chunksToCheck := make(map[string]bool) // chunkID -> needs checking

	for key, lastChunk := range lastChunkPerPartition {
		// Key format: "clientID:partitionNum"
		keyParts := strings.SplitN(key, ":", 2)
		if len(keyParts) != 2 {
			continue
		}
		clientID := keyParts[0]
		partitionNum := keyParts[1]

		// Build partition file path
		opts := partitionmanager.WriteOptions{
			FilePrefix: "users-partition",
			Header:     []string{"user_id", "name"},
			ClientID:   clientID,
			DebugMode:  false,
		}

		// Parse partition number
		partitionNumInt, err := strconv.Atoi(partitionNum)
		if err != nil {
			continue
		}

		filePath := w.partitionManager.GetPartitionFilePath(opts, partitionNumInt)

		// Check if partition file exists and has data
		stat, err := os.Stat(filePath)
		if err != nil {
			continue // File doesn't exist, not a partial write
		}

		if stat.Size() > 0 {
			// File exists and has data - the last chunk that wrote to this partition might be partial
			// Mark the entire chunk for duplicate checking (all its partitions)
			chunksToCheck[lastChunk.chunkID] = true
		}
	}

	// Mark all chunks that need duplicate checking
	for chunkID := range chunksToCheck {
		w.recoveryState[chunkID] = true
		log.Printf("Worker 2: Found potentially partial write - chunk %s (will check all partitions)", chunkID)
	}

	if len(chunksToCheck) > 0 {
		log.Printf("Worker 2: Identified %d chunks that need duplicate checking", len(chunksToCheck))
	} else {
		log.Println("Worker 2: No partially written chunks found")
	}

	return nil
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

