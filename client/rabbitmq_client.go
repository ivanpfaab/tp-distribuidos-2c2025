package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	batchpkg "github.com/tp-distribuidos-2c2025/protocol/batch"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

const (
	ServerBatchQueue = "server-batch-queue"
)

// Client handles RabbitMQ communication with the server
type Client struct {
	clientID      string
	batchProducer *workerqueue.QueueMiddleware
	ackConsumer   *workerqueue.QueueConsumer
	ackQueueName  string
	pendingAcks   chan string
	config        *middleware.ConnectionConfig
}

// generateClientID generates a unique 4-byte client ID
// Priority: CLIENT_ID env var > hostname > random ID
func generateClientID() string {
	// Try CLIENT_ID environment variable first
	if clientID := os.Getenv("CLIENT_ID"); clientID != "" {
		return ensureFourBytes(clientID)
	}

	// Try hostname
	if hostname, err := os.Hostname(); err == nil {
		return ensureFourBytes(hostname)
	}

	// Fallback to timestamp-based ID
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("%04d", timestamp%10000)
}

// ensureFourBytes ensures the string is exactly 4 bytes
func ensureFourBytes(s string) string {
	if len(s) >= 4 {
		return s[:4]
	}
	// Pad with zeros if less than 4 bytes
	return fmt.Sprintf("%-4s", s)
}

// NewClient creates a new RabbitMQ-based client
func NewClient(config *middleware.ConnectionConfig) (*Client, error) {
	clientID := generateClientID()
	log.Printf("Client initializing with ClientID: %s", clientID)

	client := &Client{
		clientID:     clientID,
		ackQueueName: fmt.Sprintf("client-%s-acks", clientID),
		pendingAcks:  make(chan string, 100),
		config:       config,
	}

	// Initialize batch producer (sends to server)
	client.batchProducer = workerqueue.NewMessageMiddlewareQueue(ServerBatchQueue, config)
	if client.batchProducer == nil {
		return nil, fmt.Errorf("failed to create batch producer")
	}

	// Declare the server batch queue
	if err := client.batchProducer.DeclareQueue(false, false, false, false); err != 0 {
		return nil, fmt.Errorf("failed to declare server batch queue: %v", err)
	}

	// Initialize acknowledgment consumer (receives from client-specific queue)
	client.ackConsumer = workerqueue.NewQueueConsumer(client.ackQueueName, config)
	if client.ackConsumer == nil {
		return nil, fmt.Errorf("failed to create ack consumer")
	}

	log.Printf("Client %s connected to RabbitMQ", clientID)
	return client, nil
}

// Start starts the acknowledgment consumer
func (c *Client) Start() error {
	// Declare the acknowledgment queue using QueueMiddleware before consuming
	ackQueueDeclarer := workerqueue.NewMessageMiddlewareQueue(c.ackQueueName, c.config)
	if ackQueueDeclarer == nil {
		return fmt.Errorf("failed to create queue declarer for ack queue")
	}

	if err := ackQueueDeclarer.DeclareQueue(false, false, false, false); err != 0 {
		ackQueueDeclarer.Close()
		return fmt.Errorf("failed to declare ack queue: %v", err)
	}
	log.Printf("Client %s: Declared acknowledgment queue %s", c.clientID, c.ackQueueName)

	// Close the declarer as we only needed it for queue declaration
	ackQueueDeclarer.Close()

	// Start consuming acknowledgments
	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			ackMessage := string(delivery.Body)
			log.Printf("Client %s received ACK: %s", c.clientID, ackMessage)

			// Send to pending acks channel for processing
			c.pendingAcks <- ackMessage

			// Acknowledge the message
			delivery.Ack(false)
		}
		done <- nil
	}

	if err := c.ackConsumer.StartConsuming(onMessageCallback); err != 0 {
		return fmt.Errorf("failed to start consuming acknowledgments: %v", err)
	}

	log.Printf("Client %s started consuming acknowledgments from %s", c.clientID, c.ackQueueName)
	return nil
}

// SendBatchMessage sends a batch message to the server via RabbitMQ
func (c *Client) SendBatchMessage(batchData *batchpkg.Batch) error {
	// Create batch message and serialize
	batchMsg := batchpkg.NewBatchMessage(batchData)
	serializedData, err := batchpkg.SerializeBatchMessage(batchMsg)
	if err != nil {
		return fmt.Errorf("failed to serialize batch message: %w", err)
	}

	// Send to server batch queue
	if err := c.batchProducer.Send(serializedData); err != 0 {
		return fmt.Errorf("failed to send batch to server: %v", err)
	}

	log.Printf("Sent batch message - ClientID: %s, FileID: %s, BatchNumber: %d",
		batchData.ClientID, batchData.FileID, batchData.BatchNumber)

	return nil
}

// WaitForAck waits for an acknowledgment with a timeout
func (c *Client) WaitForAck(timeout time.Duration) (string, error) {
	select {
	case ack := <-c.pendingAcks:
		return ack, nil
	case <-time.After(timeout):
		return "", fmt.Errorf("timeout waiting for acknowledgment")
	}
}

// GetClientID returns the client ID
func (c *Client) GetClientID() string {
	return c.clientID
}

// sendBatches reads CSV records, groups them into batches, and sends them via RabbitMQ
func (c *Client) sendBatches(r *csv.Reader, batchSize int, fileID string) (int, int, error) {
	r.FieldsPerRecord = -1 // allow variable columns

	var batch []string
	recordNum := 0
	batchNum := 0
	reachedEOF := false

	for {
		rec, err := r.Read()
		if err == io.EOF {
			reachedEOF = true
			break
		}
		if err != nil {
			return recordNum, batchNum, fmt.Errorf("CSV read error on record %d: %v", recordNum+1, err)
		}
		recordNum++

		line := strings.Join(rec, ",")
		batch = append(batch, line)

		if len(batch) == batchSize {
			batchNum++
			payload := strings.Join(batch, "\n") + "\n"
			// Create batch message
			batchData := &batchpkg.Batch{
				ClientID:    c.clientID,  // Use unique client ID
				FileID:      fileID,      // Use provided file ID
				IsEOF:       false,       // Not the last batch yet
				BatchNumber: batchNum,
				BatchSize:   len(batch),  // Number of rows in this batch
				BatchData:   payload,
			}

			// Send message to server
			err := c.SendBatchMessage(batchData)
			if err != nil {
				log.Printf("Failed to send message: %v", err)
				break
			}
			fmt.Printf("Sent batch %d with %d records\n", batchNum, len(batch))
			batch = batch[:0]
		}
	}

	// Send final partial batch (this is the last batch of the file)
	if len(batch) > 0 {
		batchNum++
		payload := strings.Join(batch, "\n") + "\n"
		// Create batch message
		batchData := &batchpkg.Batch{
			ClientID:    c.clientID,  // Use unique client ID
			FileID:      fileID,      // Use provided file ID
			IsEOF:       true,        // This is the last batch of the file
			BatchNumber: batchNum,
			BatchSize:   len(batch),  // Number of rows in this batch
			BatchData:   payload,
		}
		err := c.SendBatchMessage(batchData)
		if err != nil {
			return recordNum, batchNum, fmt.Errorf("failed to send final batch %d: %w", batchNum, err)
		}
		fmt.Printf("Sent final batch %d with %d records (EOF)\n", batchNum, len(batch))
	} else if reachedEOF && batchNum > 0 {
		// If we reached EOF but have no remaining batch, the last sent batch was the final one
		// We need to send a special EOF batch to indicate the file is complete
		batchNum++
		payload := "" // Empty payload for EOF marker
		batchData := &batchpkg.Batch{
			ClientID:    c.clientID, // Use unique client ID
			FileID:      fileID,     // Use provided file ID
			IsEOF:       true,       // This is the EOF marker
			BatchNumber: batchNum,
			BatchSize:   0,          // 0 rows in EOF marker
			BatchData:   payload,
		}
		err := c.SendBatchMessage(batchData)
		if err != nil {
			return recordNum, batchNum, fmt.Errorf("failed to send EOF batch %d: %w", batchNum, err)
		}
		fmt.Printf("Sent EOF batch %d (file complete)\n", batchNum)
	}

	return recordNum, batchNum, nil
}

// Close closes all connections
func (c *Client) Close() error {
	log.Printf("Client %s closing connections", c.clientID)

	var lastErr error

	if c.ackConsumer != nil {
		if err := c.ackConsumer.Close(); err != 0 {
			lastErr = fmt.Errorf("failed to close ack consumer: %v", err)
		}
	}

	if c.batchProducer != nil {
		if err := c.batchProducer.Close(); err != 0 {
			lastErr = fmt.Errorf("failed to close batch producer: %v", err)
		}
	}

	close(c.pendingAcks)

	return lastErr
}
