package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	batchpkg "github.com/tp-distribuidos-2c2025/protocol/batch"
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
func (c *TCPClient) SendBatchMessage(batchData *batchpkg.Batch) error {
	// Create batch message and serialize
	batchMsg := batchpkg.NewBatchMessage(batchData)
	serializedData, err := batchpkg.SerializeBatchMessage(batchMsg)
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

// scanCSVFiles scans the data folder and returns all CSV files with their file IDs
func scanCSVFiles(dataFolder string) (map[string]string, error) {
	fileMap := make(map[string]string)
	fileIDCounter := 1

	err := filepath.Walk(dataFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Only process CSV files
		if !info.IsDir() && strings.HasSuffix(strings.ToLower(path), ".csv") {
			// Generate a unique 4-character file ID
			fileID := fmt.Sprintf("%04d", fileIDCounter)
			fileMap[path] = fileID
			fileIDCounter++
			log.Printf("Found CSV file: %s (FileID: %s)", path, fileID)
		}

		return nil
	})

	return fileMap, err
}

// sendBatches reads CSV records, groups them into batches, and sends them.
func (c *TCPClient) sendBatches(r *csv.Reader, batchSize int, fileID string) (int, int, error) {
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
			batchData := &batchpkg.Batch{
				ClientID:    "1234", // Exactly 4 bytes
				FileID:      fileID, // Use provided file ID
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
		batchData := &batchpkg.Batch{
			ClientID:    "1234", // Exactly 4 bytes
			FileID:      fileID, // Use provided file ID
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

// runClient runs the client with the given data folder
func runClient(dataFolder string, serverAddr string) error {
	// Scan for CSV files in the data folder
	fileMap, err := scanCSVFiles(dataFolder)
	if err != nil {
		return fmt.Errorf("failed to scan data folder: %w", err)
	}

	if len(fileMap) == 0 {
		return fmt.Errorf("no CSV files found in data folder: %s", dataFolder)
	}

	// Connect to server
	client, err := NewTCPClient(serverAddr)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	fmt.Printf("Connected to server at %s\n", serverAddr)
	fmt.Printf("Found %d CSV files in folder: %s\n", len(fileMap), dataFolder)

	// Start goroutine for server responses
	client.StartServerReader()

	totalRecords := 0
	totalBatches := 0
	totalFiles := 0

	// Process each CSV file
	for filePath, fileID := range fileMap {
		fmt.Printf("\nProcessing file: %s (FileID: %s)\n", filePath, fileID)

		// Open the CSV file
		f, err := os.Open(filePath)
		if err != nil {
			log.Printf("Failed to open CSV file %s: %v", filePath, err)
			continue
		}

		// Read lines from file and send to server
		r := csv.NewReader(f)
		sentRecords, sentBatches, err := client.sendBatches(r, 1000, fileID)
		if err != nil {
			log.Printf("Error sending batches for file %s: %v", filePath, err)
			f.Close()
			continue
		}

		f.Close()
		totalRecords += sentRecords
		totalBatches += sentBatches
		totalFiles++

		fmt.Printf("Completed file %s: %d records, %d batches\n", filePath, sentRecords, sentBatches)

		// Small delay between files
		time.Sleep(100 * time.Millisecond)
	}

	sleep := time.After(10 * time.Second)
	<-sleep

	fmt.Printf("\nFinished sending all files. Total files: %d, Total records: %d, Total batches: %d\n",
		totalFiles, totalRecords, totalBatches)
	return nil
}
