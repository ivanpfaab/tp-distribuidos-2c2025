package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

func main() {
	// Check if data folder is provided
	if len(os.Args) < 2 {
		log.Fatal("Usage: ./client <data_folder_path>")
	}

	dataFolder := os.Args[1]

	// Get RabbitMQ configuration from environment variables
	host := getEnv("RABBITMQ_HOST", "localhost")
	portStr := getEnv("RABBITMQ_PORT", "5672")
	portInt, err := strconv.Atoi(portStr)
	if err != nil {
		log.Printf("Invalid RabbitMQ port: %v, using default 5672", err)
		portInt = 5672
	}
	username := getEnv("RABBITMQ_USER", "admin")
	password := getEnv("RABBITMQ_PASS", "password")

	// Create RabbitMQ connection configuration
	config := &middleware.ConnectionConfig{
		Host:     host,
		Port:     portInt,
		Username: username,
		Password: password,
	}

	// Create RabbitMQ-based client
	client, err := NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Start the client (to consume acknowledgments)
	if err := client.Start(); err != nil {
		log.Fatalf("Failed to start client: %v", err)
	}

	// Run the client
	err = runClient(dataFolder, client)
	if err != nil {
		log.Fatalf("Client error: %v", err)
	}

	// Keep running to continue receiving acknowledgments
	fmt.Println("Client finished sending all batches. Press Ctrl+C to exit.")
	select {}
}

// Helper function to get environment variable with default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// runClient runs the client with the given data folder
func runClient(dataFolder string, client *Client) error {
	// Scan for CSV files in the data folder
	fileMap, err := scanCSVFiles(dataFolder)
	if err != nil {
		return fmt.Errorf("failed to scan data folder: %w", err)
	}

	if len(fileMap) == 0 {
		return fmt.Errorf("no CSV files found in data folder: %s", dataFolder)
	}

	fmt.Printf("Connected to RabbitMQ\n")
	fmt.Printf("Found %d CSV files in folder: %s\n", len(fileMap), dataFolder)

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
		sentRecords, sentBatches, err := client.sendBatches(r, 10000, fileID)
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

	// Wait a bit for final acknowledgments
	time.Sleep(5 * time.Second)

	return nil
}
