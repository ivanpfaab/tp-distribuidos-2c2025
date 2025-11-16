package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	// Check if data folder is provided
	if len(os.Args) < 2 {
		log.Fatal("Usage: ./client <data_folder_path> [server_address]")
	}

	dataFolder := os.Args[1]

	// Get server address from command line or use default
	serverAddr := "proxy:8080"
	if len(os.Args) >= 3 {
		serverAddr = os.Args[2]
	}

	// Get client ID from environment variable or use default
	clientID := getEnv("CLIENT_ID", "1234")

	// Ensure client ID is exactly 4 bytes
	if len(clientID) < 4 {
		clientID = clientID + "    " // Pad with spaces
	}
	clientID = clientID[:4]

	// Run the client (now keeps connection open)
	err := runClient(dataFolder, serverAddr, clientID)
	if err != nil {
		log.Fatalf("Client error: %v", err)
	}

	// Client will exit gracefully after connection is closed
	fmt.Println("Client program terminated.")
}

// Helper function to get environment variable with default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
