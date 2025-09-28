package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"common"
)

// TCPServer handles direct client connections
type TCPServer struct {
	listener net.Listener
}

// NewTCPServer creates a new TCP server
func NewTCPServer(port string) *TCPServer {
	return &TCPServer{}
}

// Start starts the TCP server
func (s *TCPServer) Start(port string) error {
	var err error
	s.listener, err = net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to start TCP server: %w", err)
	}

	log.Printf("TCP Server listening on port %s", port)

	// Accept connections in a loop
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		// Handle each connection in a goroutine
		go s.handleConnection(conn)
	}
}

// handleConnection handles a single client connection
func (s *TCPServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	log.Printf("New client connected: %s", conn.RemoteAddr())

	// Create a buffer to read data
	buffer := make([]byte, 4096)

	for {
		// Set read timeout
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		// Read data from client
		n, err := conn.Read(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				log.Printf("Client %s timed out", conn.RemoteAddr())
			} else {
				log.Printf("Client %s disconnected: %v", conn.RemoteAddr(), err)
			}
			break
		}

		// Process the received data
		data := buffer[:n]
		response, err := s.processBatchMessage(data)
		if err != nil {
			log.Printf("Failed to process message from %s: %v", conn.RemoteAddr(), err)
			response = []byte("ERROR: " + err.Error())
		}

		// Send response back to client
		_, err = conn.Write(response)
		if err != nil {
			log.Printf("Failed to send response to %s: %v", conn.RemoteAddr(), err)
			break
		}

		log.Printf("Sent response to %s: %s", conn.RemoteAddr(), string(response))
	}
}

// processBatchMessage processes a batch message and returns a response
func (s *TCPServer) processBatchMessage(data []byte) ([]byte, error) {
	// Deserialize the message
	message, err := common.Deserialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %w", err)
	}

	// Check if it's a Batch message (only type we handle)
	batchMsg, ok := message.(*common.Batch)
	if !ok {
		return nil, fmt.Errorf("expected batch message, got %T", message)
	}

	// Log the received batch
	log.Printf("Received batch - ClientID: %s, FileID: %s, BatchNumber: %d, Data: %s",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber, batchMsg.BatchData)

	// Create acknowledgment response
	response := fmt.Sprintf("ACK: Batch received - ClientID: %s, FileID: %s, BatchNumber: %d",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber)

	return []byte(response), nil
}

// Stop stops the TCP server
func (s *TCPServer) Stop() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}
