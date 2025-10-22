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
	"sort"
	"strings"
	"time"

	batchpkg "github.com/tp-distribuidos-2c2025/protocol/batch"
)

// TCPClient handles direct connection to the server
type TCPClient struct {
	conn     net.Conn
	clientID string
}

// NewTCPClient creates a new TCP client
func NewTCPClient(serverAddr string, clientID string) (*TCPClient, error) {
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	return &TCPClient{
		conn:     conn,
		clientID: clientID,
	}, nil
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

	log.Printf("Sent batch message - ClientID: %s, FileID: %s, BatchNumber: %d",
		batchData.ClientID, batchData.FileID, batchData.BatchNumber)

	return nil
}

// Close closes the connection
func (c *TCPClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// FileTypeInfo represents information about a file type group
type FileTypeInfo struct {
	TypeCode string
	Files    []string
}

// scanCSVFiles scans the data folder and returns all CSV files with their file IDs
func scanCSVFiles(dataFolder string) (map[string]string, error) {
	fileMap := make(map[string]string)
	var csvFiles []string

	// First, collect all CSV files
	err := filepath.Walk(dataFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Only process CSV files
		if !info.IsDir() && strings.HasSuffix(strings.ToLower(path), ".csv") {
			csvFiles = append(csvFiles, path)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Sort files for deterministic FileID assignment
	sort.Strings(csvFiles)

	// Group files by type for dynamic numbering
	fileTypeGroups := groupFilesByType(csvFiles)

	// Assign FileIDs based on file type/name patterns with dynamic numbering
	for _, path := range csvFiles {
		fileID := generateFileID(path, fileTypeGroups)
		fileMap[path] = fileID
		log.Printf("Found CSV file: %s (FileID: %s)", path, fileID)
	}

	return fileMap, nil
}

// groupFilesByType groups CSV files by their type for dynamic numbering
func groupFilesByType(csvFiles []string) map[string]*FileTypeInfo {
	typeGroups := make(map[string]*FileTypeInfo)

	for _, path := range csvFiles {
		fileType := determineFileType(path)

		if typeGroups[fileType] == nil {
			typeGroups[fileType] = &FileTypeInfo{
				TypeCode: getTypeCode(fileType),
				Files:    make([]string, 0),
			}
		}

		typeGroups[fileType].Files = append(typeGroups[fileType].Files, path)
	}

	return typeGroups
}

// determineFileType determines the file type based on path
func determineFileType(filePath string) string {
	dir := filepath.Dir(filePath)
	dirLower := strings.ToLower(dir)

	switch {
	case strings.Contains(dirLower, "stores"):
		return "stores"
	case strings.Contains(dirLower, "transactions"):
		return "transactions"
	case strings.Contains(dirLower, "transaction_items"):
		return "transaction_items"
	case strings.Contains(dirLower, "users"):
		return "users"
	case strings.Contains(dirLower, "menu_items"):
		return "menu_items"
	case strings.Contains(dirLower, "payment_methods"):
		return "payment_methods"
	case strings.Contains(dirLower, "vouchers"):
		return "vouchers"
	default:
		return "unknown"
	}
}

// getTypeCode returns the 2-character type code for a file type
func getTypeCode(fileType string) string {
	switch fileType {
	case "stores":
		return "ST"
	case "transactions":
		return "TR"
	case "transaction_items":
		return "TI"
	case "users":
		return "US"
	case "menu_items":
		return "MN"
	case "payment_methods":
		return "PM"
	case "vouchers":
		return "VC"
	default:
		return "XX"
	}
}

// generateFileID generates a deterministic FileID based on file path/name with dynamic numbering
// FileID Format: [TYPE][NUMBER] where:
//
//	TYPE: ST=Stores, TR=Transactions, TI=Transaction Items, US=Users, MN=Menu Items, PM=Payment Methods, VC=Vouchers, XX=Unknown
//	NUMBER: 01-99 assigned dynamically based on file order within each type
//
// Examples: ST01=stores.csv, TR01=transactions_202307.csv, TR02=transactions_202308.csv
func generateFileID(filePath string, fileTypeGroups map[string]*FileTypeInfo) string {
	fileType := determineFileType(filePath)
	typeInfo := fileTypeGroups[fileType]

	if typeInfo == nil {
		// Fallback for unknown types
		return fmt.Sprintf("XX%02d", 1)
	}

	// Find the position of this file within its type group
	fileNumber := 1
	for i, path := range typeInfo.Files {
		if path == filePath {
			fileNumber = i + 1
			break
		}
	}

	// Generate FileID with dynamic numbering
	return fmt.Sprintf("%s%02d", typeInfo.TypeCode, fileNumber)
}

// sendBatches reads CSV records, groups them into batches, and sends them.
func (c *TCPClient) sendBatches(r *csv.Reader, batchSize int, fileID string, isLastFromTable bool) (int, int, error) {
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
				ClientID:        c.clientID, // Use client ID
				FileID:          fileID,     // Use provided file ID
				IsEOF:           false,      // Not the last batch yet
				IsLastFromTable: false,      // Not the last batch of the file yet
				BatchNumber:     batchNum,
				BatchSize:       len(batch), // Number of rows in this batch
				BatchData:       payload,
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
			ClientID:        c.clientID,      // Use client ID
			FileID:          fileID,          // Use provided file ID
			IsEOF:           true,            // This is the last batch of the file
			IsLastFromTable: isLastFromTable, // True if this is the last file in the folder
			BatchNumber:     batchNum,
			BatchSize:       len(batch), // Number of rows in this batch
			BatchData:       payload,
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
			ClientID:        c.clientID,      // Use client ID
			FileID:          fileID,          // Use provided file ID
			IsEOF:           true,            // This is the EOF marker
			IsLastFromTable: isLastFromTable, // True if this is the last file in the folder
			BatchNumber:     batchNum,
			BatchSize:       0, // 0 rows in EOF marker
			BatchData:       payload,
		}
		err := c.SendBatchMessage(batchData)
		if err != nil {
			return recordNum, batchNum, fmt.Errorf("failed to send EOF batch %d: %w", batchNum, err)
		}
		fmt.Printf("Sent EOF batch %d (file complete)\n", batchNum)
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

// KeepConnectionOpen keeps the connection open after processing all files
func (c *TCPClient) KeepConnectionOpen() error {
	fmt.Println("Connection kept open. Press Ctrl+C to exit.")

	// Wait indefinitely to keep connection open
	select {}
}

// runClient runs the client with the given data folder and keeps connection open
func runClient(dataFolder string, serverAddr string, clientID string) error {
	// Scan for CSV files in the data folder
	fileMap, err := scanCSVFiles(dataFolder)
	if err != nil {
		return fmt.Errorf("failed to scan data folder: %w", err)
	}

	if len(fileMap) == 0 {
		return fmt.Errorf("no CSV files found in data folder: %s", dataFolder)
	}

	// Connect to server
	client, err := NewTCPClient(serverAddr, clientID)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	// Note: Removed defer client.Close() to keep connection open

	fmt.Printf("Connected to server at %s\n", serverAddr)
	fmt.Printf("Found %d CSV files in folder: %s\n", len(fileMap), dataFolder)

	// Start goroutine for server responses
	client.StartServerReader()

	totalRecords := 0
	totalBatches := 0
	totalFiles := 0

	// Get sorted list of files
	var csvFiles []string
	for filePath := range fileMap {
		csvFiles = append(csvFiles, filePath)
	}
	sort.Strings(csvFiles)

	// Group files by type to determine last file in each type
	fileTypeGroups := groupFilesByType(csvFiles)

	// Process each CSV file
	for _, filePath := range csvFiles {
		fileID := fileMap[filePath]
		fileType := determineFileType(filePath)

		// Check if this is the last file of its type
		var lastFileID string
		typeInfo := fileTypeGroups[fileType]
		if typeInfo != nil && len(typeInfo.Files) > 0 {
			lastFilePath := typeInfo.Files[len(typeInfo.Files)-1]
			lastFileID = fileMap[lastFilePath]
		}
		isLastFromTable := (fileID == lastFileID)

		fmt.Printf("\nProcessing file: %s (FileID: %s, IsLastFromTable: %t)\n", filePath, fileID, isLastFromTable)

		// Open the CSV file
		f, err := os.Open(filePath)
		if err != nil {
			log.Printf("Failed to open CSV file %s: %v", filePath, err)
			continue
		}

		// Read lines from file and send to server
		r := csv.NewReader(f)
		sentRecords, sentBatches, err := client.sendBatches(r, 10000, fileID, isLastFromTable)
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

	fmt.Printf("\nFinished sending all files. Total files: %d, Total records: %d, Total batches: %d\n",
		totalFiles, totalRecords, totalBatches)

	// Keep connection open
	fmt.Println("\n=== Connection kept open ===")

	// Keep connection open
	return client.KeepConnectionOpen()
}
