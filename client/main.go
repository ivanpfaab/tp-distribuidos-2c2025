package main

import (
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
	serverAddr := "localhost:8080"
	if len(os.Args) >= 3 {
		serverAddr = os.Args[2]
	}

	// Run the client
	err := runClient(dataFolder, serverAddr)
	if err != nil {
		log.Fatalf("Client error: %v", err)
	}

	select {}

}
