package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/signals"
	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
	"github.com/tp-distribuidos-2c2025/workers/shared"
)

// InFileJoinOrchestrator manages the completion tracking and signaling for in-file joins
type InFileJoinOrchestrator struct {
	// Consumer for chunk notifications from user partition writers
	consumer *workerqueue.QueueConsumer

	// Exchange producer for signaling user join workers
	completionProducer *exchange.ExchangeMiddleware

	// Configuration
	config *middleware.ConnectionConfig

	// Completion tracker
	completionTracker *shared.CompletionTracker

	// Fault tolerance components
	messageManager *messagemanager.MessageManager
	metadataDir    string
}

// NewInFileJoinOrchestrator creates a new in-file join orchestrator
func NewInFileJoinOrchestrator(config *middleware.ConnectionConfig) (*InFileJoinOrchestrator, error) {
	// Create consumer for chunk notifications from user partition writers
	consumer := workerqueue.NewQueueConsumer(
		queues.UserPartitionCompletionQueue, // Queue for receiving completion notifications
		config,
	)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create consumer")
	}

	// Declare the completion queue
	queueDeclarer := workerqueue.NewMessageMiddlewareQueue(
		queues.UserPartitionCompletionQueue,
		config,
	)
	if queueDeclarer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create queue declarer for user partition completion queue")
	}
	if err := queueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		consumer.Close()
		queueDeclarer.Close()
		return nil, fmt.Errorf("failed to declare user partition completion queue: %v", err)
	}
	queueDeclarer.Close()

	// Create exchange producer for signaling user join workers
	completionProducer := exchange.NewMessageMiddlewareExchange(
		queues.UserIdCompletionExchange,
		[]string{queues.UserIdCompletionRoutingKey}, // Use fanout exchange to broadcast to all user join workers
		config,
	)
	if completionProducer == nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to create completion exchange producer")
	}

	// Declare the exchange
	if err := completionProducer.DeclareExchange("fanout", false, false, false, false); err != 0 {
		consumer.Close()
		completionProducer.Close()
		return nil, fmt.Errorf("failed to declare completion exchange: %v", err)
	}

	// Initialize fault tolerance components
	metadataDir := "/app/orchestrator-data/metadata"
	processedNotificationsPath := "/app/orchestrator-data/processed-notifications.txt"

	// Ensure metadata directory exists
	if err := os.MkdirAll(metadataDir, 0755); err != nil {
		consumer.Close()
		completionProducer.Close()
		return nil, fmt.Errorf("failed to create metadata directory: %w", err)
	}

	// Initialize MessageManager for duplicate detection
	messageManager := messagemanager.NewMessageManager(processedNotificationsPath)
	if err := messageManager.LoadProcessedIDs(); err != nil {
		log.Printf("Warning: failed to load processed notifications: %v (starting with empty state)", err)
	} else {
		count := messageManager.GetProcessedCount()
		log.Printf("Loaded %d processed notification IDs", count)
	}

	// Create completion tracker with callback
	completionTracker := shared.NewCompletionTracker(
		"InFileJoinOrchestrator",
		func(clientID string, clientStatus *shared.ClientStatus) {
			// Add a small delay to ensure all files are fully written to disk
			log.Printf("All files completed for client %s, waiting 200ms to ensure file sync...", clientID)
			time.Sleep(200 * time.Millisecond)

			// Send completion signal to all user join workers
			if err := sendCompletionSignal(completionProducer, clientID); err != nil {
				log.Printf("Failed to send completion signal for client %s: %v", clientID, err)
			} else {
				log.Printf("Sent completion signal for client %s to all user join workers", clientID)
			}

			// Delete CSV metadata file for completed client
			csvPath := filepath.Join(metadataDir, clientID+".csv")
			if err := os.Remove(csvPath); err != nil && !os.IsNotExist(err) {
				log.Printf("Warning: failed to delete metadata file for client %s: %v", clientID, err)
			} else {
				log.Printf("Deleted metadata file for completed client %s", clientID)
			}
		},
	)

	orchestrator := &InFileJoinOrchestrator{
		consumer:           consumer,
		completionProducer: completionProducer,
		config:             config,
		completionTracker:  completionTracker,
		messageManager:     messageManager,
		metadataDir:        metadataDir,
	}

	// Rebuild state from CSV metadata on startup
	if err := orchestrator.rebuildStateFromCSV(); err != nil {
		log.Printf("Warning: failed to rebuild state from CSV: %v", err)
	}

	return orchestrator, nil
}

// Start starts the orchestrator
func (o *InFileJoinOrchestrator) Start() {
	log.Println("In-File Join Orchestrator: Starting...")

	// Start consuming chunk notifications
	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			// Deserialize the chunk notification
			message, err := signals.DeserializeChunkNotification(delivery.Body)
			if err != nil {
				log.Printf("Failed to deserialize chunk notification: %v", err)
				delivery.Ack(false)
				continue
			}

			// Check for duplicate notification
			if o.messageManager.IsProcessed(message.ID) {
				log.Printf("Notification %s already processed, skipping", message.ID)
				delivery.Ack(false)
				continue
			}

			// Process the chunk notification
			if err := o.completionTracker.ProcessChunkNotification(message); err != nil {
				log.Printf("Failed to process chunk notification: %v", err)
				delivery.Ack(false)
				continue
			}

			// Append notification to CSV for state rebuild
			if err := o.appendNotificationToCSV(message); err != nil {
				log.Printf("Warning: failed to append notification to CSV: %v", err)
			}

			// Mark as processed in MessageManager
			if err := o.messageManager.MarkProcessed(message.ID); err != nil {
				log.Printf("Warning: failed to mark notification as processed: %v", err)
			}

			// Acknowledge the message
			delivery.Ack(false)
		}
		done <- nil
	}

	if err := o.consumer.StartConsuming(onMessageCallback); err != 0 {
		log.Fatalf("Failed to start consuming: %v", err)
	}
}

// sendCompletionSignal sends a completion signal to all user join workers
func sendCompletionSignal(producer *exchange.ExchangeMiddleware, clientID string) error {
	// Create completion signal
	signal := signals.NewJoinCompletionSignal(
		clientID,
		"user-files",
		"in-file-join-orchestrator",
	)

	// Serialize the signal
	messageData, err := signals.SerializeJoinCompletionSignal(signal)
	if err != nil {
		return fmt.Errorf("failed to serialize completion signal: %w", err)
	}

	// Send to exchange (fanout will broadcast to all user join workers)
	if err := producer.Send(messageData, []string{queues.UserIdCompletionRoutingKey}); err != 0 {
		return fmt.Errorf("failed to send completion signal: %v", err)
	}
	return nil
}

// Close closes all connections
func (o *InFileJoinOrchestrator) Close() {
	if o.consumer != nil {
		o.consumer.Close()
	}
	if o.completionProducer != nil {
		o.completionProducer.Close()
	}
	if o.messageManager != nil {
		o.messageManager.Close()
	}
}

// rebuildStateFromCSV rebuilds completion tracker state from CSV metadata files
func (o *InFileJoinOrchestrator) rebuildStateFromCSV() error {
	log.Println("Rebuilding state from CSV metadata...")

	// Read all CSV files in metadata directory
	files, err := os.ReadDir(o.metadataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Directory doesn't exist, nothing to rebuild
		}
		return fmt.Errorf("failed to read metadata directory: %w", err)
	}

	rebuiltCount := 0
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".csv") {
			continue
		}

		// Extract clientID from filename: {clientID}.csv
		clientID := strings.TrimSuffix(file.Name(), ".csv")
		filePath := filepath.Join(o.metadataDir, file.Name())

		// Read notifications from CSV
		notifications, err := o.readNotificationsFromCSV(filePath)
		if err != nil {
			log.Printf("Warning: failed to read notifications from %s: %v", filePath, err)
			continue
		}

		if len(notifications) == 0 {
			continue
		}

		// Rebuild state from notifications
		clientStatus := o.buildStatusFromNotifications(clientID, notifications)
		if clientStatus == nil {
			continue
		}

		// Only keep non-completed clients
		if !clientStatus.IsCompleted {
			// Rebuild state by processing notifications directly into completion tracker
			// We process them in order to rebuild the state correctly
			// Note: These notifications are already in MessageManager, so they won't be processed again
			// during normal operation (duplicate check will skip them)
			for _, notification := range notifications {
				// Process each notification to rebuild state
				// This will update the internal state without triggering callbacks if already completed
				if err := o.completionTracker.ProcessChunkNotification(notification); err != nil {
					log.Printf("Warning: failed to process notification during rebuild: %v", err)
				}
			}
			rebuiltCount++
			log.Printf("Rebuilt state for client %s (%d notifications)", clientID, len(notifications))
		} else {
			// Client already completed, delete CSV file
			if err := os.Remove(filePath); err != nil {
				log.Printf("Warning: failed to delete metadata file for completed client %s: %v", clientID, err)
			} else {
				log.Printf("Deleted metadata file for already-completed client %s", clientID)
			}
		}
	}

	log.Printf("State rebuild complete: %d clients rebuilt", rebuiltCount)
	return nil
}

// readNotificationsFromCSV reads notifications from a CSV file, handling incomplete last row
func (o *InFileJoinOrchestrator) readNotificationsFromCSV(filePath string) ([]*signals.ChunkNotification, error) {
	// Check and fix incomplete last row
	hasIncomplete, err := o.hasIncompleteLastRow(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to check incomplete row: %w", err)
	}
	if hasIncomplete {
		if err := o.removeIncompleteLastRow(filePath); err != nil {
			return nil, fmt.Errorf("failed to remove incomplete row: %w", err)
		}
		log.Printf("Fixed incomplete last row in %s", filePath)
	}

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return []*signals.ChunkNotification{}, nil
		}
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	notifications := []*signals.ChunkNotification{}

	// Read header
	header, err := reader.Read()
	if err == io.EOF {
		return notifications, nil
	}
	if err != nil {
		return nil, err
	}

	// Validate header - support both old format (without table_id) and new format (with table_id)
	expectedHeaderOld := []string{"notification_id", "client_id", "file_id", "chunk_number", "is_last_chunk", "is_last_from_table"}
	expectedHeaderNew := []string{"notification_id", "client_id", "file_id", "table_id", "chunk_number", "is_last_chunk", "is_last_from_table"}

	// Check if CSV uses new format (has table_id column)
	hasTableID := len(header) >= len(expectedHeaderNew) && header[3] == "table_id"
	if len(header) < len(expectedHeaderOld) {
		return nil, fmt.Errorf("invalid CSV header")
	}

	// Read data rows
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if len(row) < len(expectedHeaderOld) {
			continue // Skip malformed rows
		}

		// Parse row - handle both old and new formats
		notificationID := row[0]
		clientID := row[1]
		fileID := row[2]

		var tableID int
		var chunkNumber int
		var isLastChunk bool
		var isLastFromTable bool

		if hasTableID && len(row) >= len(expectedHeaderNew) {
			// New format with table_id
			tableID, err = strconv.Atoi(row[3])
			if err != nil {
				continue
			}
			chunkNumber, err = strconv.Atoi(row[4])
			if err != nil {
				continue
			}
			isLastChunk = row[5] == "1" || strings.ToLower(row[5]) == "true"
			isLastFromTable = row[6] == "1" || strings.ToLower(row[6]) == "true"
		} else {
			// Old format without table_id (default to 0 for backward compatibility)
			tableID = 0
			chunkNumber, err = strconv.Atoi(row[3])
			if err != nil {
				continue
			}
			isLastChunk = row[4] == "1" || strings.ToLower(row[4]) == "true"
			isLastFromTable = row[5] == "1" || strings.ToLower(row[5]) == "true"
		}

		// Create notification (MapWorkerID not persisted)
		notification := signals.NewChunkNotification(
			clientID,
			fileID,
			"", // MapWorkerID not persisted
			tableID,
			chunkNumber,
			isLastChunk,
			isLastFromTable,
		)
		// Set the ID explicitly (it's computed in NewChunkNotification, but we want the persisted one)
		notification.ID = notificationID

		notifications = append(notifications, notification)
	}

	return notifications, nil
}

// buildStatusFromNotifications rebuilds ClientStatus from a list of notifications
func (o *InFileJoinOrchestrator) buildStatusFromNotifications(clientID string, notifications []*signals.ChunkNotification) *shared.ClientStatus {
	clientStatus := &shared.ClientStatus{
		ClientID:           clientID,
		FileStatuses:       make(map[string]*shared.FileStatus),
		CompletedFiles:     0,
		TotalExpectedFiles: 0,
		ExpectedFilesKnown: false,
		IsCompleted:        false,
	}

	for _, notification := range notifications {
		fileKey := fmt.Sprintf("%s_%d", notification.FileID, notification.TableID)

		// Initialize file status if not exists
		if clientStatus.FileStatuses[fileKey] == nil {
			clientStatus.FileStatuses[fileKey] = &shared.FileStatus{
				FileID:            notification.FileID,
				TableID:           notification.TableID,
				ChunksReceived:    0,
				LastChunkNumber:   0,
				IsCompleted:       false,
				LastChunkReceived: false,
			}
		}

		fileStatus := clientStatus.FileStatuses[fileKey]

		// Update file status
		fileStatus.ChunksReceived++
		if notification.IsLastChunk {
			fileStatus.LastChunkNumber = notification.ChunkNumber
			fileStatus.LastChunkReceived = true
		}

		// Check if file is completed
		if fileStatus.LastChunkReceived && fileStatus.ChunksReceived >= fileStatus.LastChunkNumber {
			if !fileStatus.IsCompleted {
				fileStatus.IsCompleted = true
				clientStatus.CompletedFiles++
			}
		}

		// Infer total expected files
		if notification.IsLastFromTable && !clientStatus.ExpectedFilesKnown {
			if len(notification.FileID) >= 2 {
				fileNumberStr := notification.FileID[len(notification.FileID)-2:]
				if fileNumber, err := strconv.Atoi(fileNumberStr); err == nil {
					clientStatus.TotalExpectedFiles = fileNumber
					clientStatus.ExpectedFilesKnown = true
				} else {
					clientStatus.TotalExpectedFiles = 1
					clientStatus.ExpectedFilesKnown = true
				}
			}
		}
	}

	// Check if client is completed
	if clientStatus.ExpectedFilesKnown && clientStatus.CompletedFiles >= clientStatus.TotalExpectedFiles {
		clientStatus.IsCompleted = true
	}

	return clientStatus
}

// appendNotificationToCSV appends a notification to the client's CSV metadata file
func (o *InFileJoinOrchestrator) appendNotificationToCSV(notification *signals.ChunkNotification) error {
	csvPath := filepath.Join(o.metadataDir, notification.ClientID+".csv")

	// Ensure directory exists
	if err := os.MkdirAll(o.metadataDir, 0755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	file, err := os.OpenFile(csvPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header if file is new
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	if stat.Size() == 0 {
		header := []string{"notification_id", "client_id", "file_id", "table_id", "chunk_number", "is_last_chunk", "is_last_from_table"}
		if err := writer.Write(header); err != nil {
			return fmt.Errorf("failed to write header: %w", err)
		}
	}

	// Write data row
	isLastChunkStr := "0"
	if notification.IsLastChunk {
		isLastChunkStr = "1"
	}
	isLastFromTableStr := "0"
	if notification.IsLastFromTable {
		isLastFromTableStr = "1"
	}

	row := []string{
		notification.ID,
		notification.ClientID,
		notification.FileID,
		strconv.Itoa(notification.TableID),
		strconv.Itoa(notification.ChunkNumber),
		isLastChunkStr,
		isLastFromTableStr,
	}

	if err := writer.Write(row); err != nil {
		return fmt.Errorf("failed to write row: %w", err)
	}

	// Sync to ensure data is written to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// hasIncompleteLastRow checks if the last row in a CSV file is incomplete (missing \n)
func (o *InFileJoinOrchestrator) hasIncompleteLastRow(filePath string) (bool, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return false, err
	}

	if stat.Size() == 0 {
		return false, nil
	}

	// Read the last byte to check if file ends with newline
	lastByte := make([]byte, 1)
	_, err = file.ReadAt(lastByte, stat.Size()-1)
	if err != nil {
		return false, err
	}

	return lastByte[0] != '\n', nil
}

// removeIncompleteLastRow removes the incomplete last row by truncating the file
func (o *InFileJoinOrchestrator) removeIncompleteLastRow(filePath string) error {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	if stat.Size() == 0 {
		return nil
	}

	// Read file backward in chunks until we find a newline
	const chunkSize = int64(512)
	offset := stat.Size()

	for offset > 0 {
		readSize := chunkSize
		if offset < chunkSize {
			readSize = offset
		}

		readOffset := offset - readSize
		buffer := make([]byte, readSize)
		_, err = file.ReadAt(buffer, readOffset)
		if err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}

		// Search backward for newline
		lastNewline := -1
		for i := len(buffer) - 1; i >= 0; i-- {
			if buffer[i] == '\n' {
				lastNewline = i
				break
			}
		}

		if lastNewline != -1 {
			newSize := readOffset + int64(lastNewline) + 1
			return file.Truncate(newSize)
		}

		offset = readOffset
	}

	// No newline found, truncate to 0
	return file.Truncate(0)
}
