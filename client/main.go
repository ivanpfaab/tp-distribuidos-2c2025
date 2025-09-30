package main

import (
	"log"
	"os"
)

func main() {
	// Check if input file is provided
	if len(os.Args) < 2 {
		log.Fatal("Usage: ./client <input_file.csv> [server_address]")
	}

	inputFile := os.Args[1]

	// Get server address from command line or use default
	serverAddr := "localhost:8080"
	if len(os.Args) >= 3 {
		serverAddr = os.Args[2]
	}

	// Run the client
	err := runClient(inputFile, serverAddr)
	if err != nil {
		log.Fatalf("Client error: %v", err)
	}

	select {}

}
