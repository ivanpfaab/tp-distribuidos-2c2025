package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()
	
	fmt.Printf("Client connected: %s\n", conn.RemoteAddr())
	
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		message := scanner.Text()
		fmt.Printf("Received: %s\n", message)
		
		// Echo the message back to the client
		response := fmt.Sprintf("Echo: %s", message)
		conn.Write([]byte(response + "\n"))
		
		// Check for exit command
		if strings.ToLower(strings.TrimSpace(message)) == "exit" {
			fmt.Printf("Client %s disconnected\n", conn.RemoteAddr())
			break
		}
	}
	
	if err := scanner.Err(); err != nil {
		log.Printf("Error reading from connection: %v", err)
	}
}

func main() {
	port := "8080"
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	defer listener.Close()
	
	fmt.Printf("Echo server listening on port %s\n", port)
	
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		
		// Handle each connection in a separate goroutine
		go handleConnection(conn)
	}
}
