package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/batch"
)

// TCPClient handles direct connection to the server
type TCPClient struct {
	conn net.Conn
}

// NewTCPClient creates a new TCP client
func NewTCPClient(serverAddr string) (*TCPClient, error) {
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	return &TCPClient{conn: conn}, nil
}

// SendBatchMessage sends a batch message to the server
func (c *TCPClient) SendBatchMessage(batchData *batch.Batch) error {
	// Create batch message and serialize
	batchMsg := batch.NewBatchMessage(batchData)
	serializedData, err := batch.SerializeBatchMessage(batchMsg)
	if err != nil {
		return fmt.Errorf("failed to serialize batch message: %w", err)
	}

	// Send data to server
	_, err = c.conn.Write(serializedData)
	if err != nil {
		return fmt.Errorf("failed to send data to server: %w", err)
	}

	log.Printf("Sent batch message - ClientID: %s, FileID: %s, BatchNumber: %d, Data: %s",
		batchData.ClientID, batchData.FileID, batchData.BatchNumber, batchData.BatchData)

	return nil
}

// Close closes the connection
func (c *TCPClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// sendBatches reads CSV records, groups them into batches, and sends them.
func (c *TCPClient) sendBatches(r *csv.Reader, batchSize int) (int, int, error) {
	r.FieldsPerRecord = -1 // allow variable columns

	var batch []string
	recordNum := 0
	batchNum := 0

	for {
		rec, err := r.Read()
		if err == io.EOF {
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
			batchData := &batch.Batch{
				ClientID:    "1234", // Exactly 4 bytes
				FileID:      "5678", // Exactly 4 bytes
				IsEOF:       strings.ToLower(payload) == "exit",
				BatchNumber: batchNum,
				BatchSize:   len(payload),
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

	// Send final partial batch
	if len(batch) > 0 {
		batchNum++
		payload := strings.Join(batch, "\n") + "\n"
		// Create batch message
		batchData := &batch.Batch{
			ClientID:    "1234", // Exactly 4 bytes
			FileID:      "5678", // Exactly 4 bytes
			IsEOF:       strings.ToLower(payload) == "exit",
			BatchNumber: batchNum,
			BatchSize:   len(payload),
			BatchData:   payload,
		}
		err := c.SendBatchMessage(batchData)
		if err != nil {
			return recordNum, batchNum, fmt.Errorf("failed to send final batch %d: %w", batchNum, err)
		}
		fmt.Printf("Sent final batch %d with %d records\n", batchNum, len(batch))
	}

	return recordNum, batchNum, nil
}

func (c *TCPClient) StartServerReader() {
	go func() {
		sc := bufio.NewScanner(c.conn)
		sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
		for sc.Scan() {
			fmt.Printf("(Client) Server: %s\n", sc.Text())
		}
		if err := sc.Err(); err != nil && err != io.EOF {
			log.Printf("(Client) Error reading from server: %v", err)
		}
	}()
}

// runClient runs the client with the given input file
func runClient(inputFile string, serverAddr string) error {
	// Open the input file
	f, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("Failed to open CSV: %v", err)
	}
	defer f.Close()
	// Connect to server
	client, err := NewTCPClient(serverAddr)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	fmt.Printf("Connected to server at %s\n", serverAddr)
	fmt.Printf("Reading messages from file: %s\n", inputFile)

	// Start goroutine for server responses
	client.StartServerReader()

	// Read lines from file and send to server
	r := csv.NewReader(f)
	sentRecords, sentBatches, err := client.sendBatches(r, 5)
	if err != nil {
		return fmt.Errorf("error sending batches: %w", err)
	}

	fmt.Printf("Finished sending. Total records: %d, Total batches: %d\n", sentRecords, sentBatches)
	return nil
}
