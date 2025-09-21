package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

func writeAll(w net.Conn, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			// If some bytes were written, trim and decide whether to retry
			data = data[n:]
			// Retry only on temporary errors; otherwise return
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return err
		}
		data = data[n:]
	}
	return nil
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: ./client <input_file.txt>")
	}
	inputFile := os.Args[1]

	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("Failed to open input file: %v", err)
	}
	defer file.Close()

	serverAddr := "server:8080"
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	fmt.Printf("Connected to echo server at %s\n", serverAddr)
	fmt.Printf("Reading messages from file: %s\n", inputFile)

	// Reader goroutine
	go func() {
		scanner := bufio.NewScanner(conn)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			fmt.Printf("Server response: %s\n", scanner.Text())
		}
		if err := scanner.Err(); err != nil && err != io.EOF {
			log.Printf("Error reading from server: %v", err)
		}
	}()

	scanner := bufio.NewScanner(file)
	lineCount := 0

	for scanner.Scan() {
		lineCount++
		message := strings.TrimSpace(scanner.Text())
		if message == "" {
			continue
		}
		fmt.Printf("Sending line %d: %s\n", lineCount, message)

		payload := []byte(message + "\n")
		if err := writeAll(conn, payload); err != nil {
			log.Printf("Failed to send message fully (line %d): %v", lineCount, err)
			// Close the connection to avoid the server acting on a truncated line
			_ = conn.Close()
			break
		}

		time.Sleep(100 * time.Millisecond)

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
