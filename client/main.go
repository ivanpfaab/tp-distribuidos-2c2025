package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

func main() {
	// Check if input file is provided
	if len(os.Args) < 2 {
		log.Fatal("Usage: ./client <input_file.txt>")
	}
	
	inputFile := os.Args[1]
	
	// Open the input file
	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("Failed to open input file: %v", err)
	}
	defer file.Close()
	
	// Connect to the server
	serverAddr := "server:8080" // Use service name for Docker networking
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()
	
	fmt.Printf("Connected to echo server at %s\n", serverAddr)
	fmt.Printf("Reading messages from file: %s\n", inputFile)
	
	// Start a goroutine to read responses from server
	go func() {
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			response := scanner.Text()
			fmt.Printf("Server response: %s\n", response)
		}
		if err := scanner.Err(); err != nil {
			log.Printf("Error reading from server: %v", err)
		}
	}()
	
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
		
		// Send message to server
		_, err := conn.Write([]byte(message + "\n"))
		if err != nil {
			log.Printf("Failed to send message: %v", err)
			break
		}
		
		// Small delay between messages to see responses clearly
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
}
