package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"batch"
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

// ReadResponse reads a response from the server
func (c *TCPClient) ReadResponse() (string, error) {
	// Set read timeout
	c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	// Read response
	buffer := make([]byte, 1024)
	n, err := c.conn.Read(buffer)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	response := string(buffer[:n])
	log.Printf("Received response: %s", response)
	return response, nil
}

// Close closes the connection
func (c *TCPClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// runClient runs the client with the given input file
func runClient(inputFile string, serverAddr string) error {
	// Open the input file
	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %v", err)
	}
	defer file.Close()

	// Connect to server
	client, err := NewTCPClient(serverAddr)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	fmt.Printf("Connected to server at %s\n", serverAddr)
	fmt.Printf("Reading messages from file: %s\n", inputFile)

	// Read lines from file and send to server
	scanner := bufio.NewScanner(file)
	lineCount := 0

	for scanner.Scan() {
		lineCount++
		message := strings.TrimSpace(scanner.Text())

		// Skip empty lines
		if message == "" {
			continue
		}

		fmt.Printf("Sending line %d: %s\n", lineCount, message)

		// Create batch message
		batchData := &batch.Batch{
			ClientID:    "1234", // Exactly 4 bytes
			FileID:      "5678", // Exactly 4 bytes
			IsEOF:       strings.ToLower(message) == "exit",
			BatchNumber: lineCount,
			BatchSize:   len(message),
			BatchData:   message,
		}

		// Send message to server
		err = client.SendBatchMessage(batchData)
		if err != nil {
			log.Printf("Failed to send message: %v", err)
			break
		}

		// Read response from server
		response, err := client.ReadResponse()
		if err != nil {
			log.Printf("Failed to read response: %v", err)
			break
		}

		fmt.Printf("Server response: %s\n", response)

		// Small delay between messages
		time.Sleep(100 * time.Millisecond)

		// Check for exit command
		if strings.ToLower(message) == "exit" {
			fmt.Println("Exit command found, stopping...")
			break
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading file: %v", err)
	}

	fmt.Printf("Finished sending %d lines from file\n", lineCount)
	fmt.Println("Client completed successfully!")
	return nil
}
