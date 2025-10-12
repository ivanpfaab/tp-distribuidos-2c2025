package main

import (
	"fmt"
	"log"
	"net"
	"strconv"

	client_request_handler "github.com/tp-distribuidos-2c2025/server/controller/client_request_handler"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// TCPServer handles direct client connections
type TCPServer struct {
	listener             net.Listener
	clientRequestHandler *client_request_handler.ClientRequestHandler
}

// NewTCPServer creates a new TCP server
func NewTCPServer(port string) *TCPServer {
	// Get configuration from environment variables or use defaults
	host := getEnv("RABBITMQ_HOST", "localhost")
	portStr := getEnv("RABBITMQ_PORT", "5672")
	portInt, err := strconv.Atoi(portStr)
	if err != nil {
		log.Printf("Invalid RabbitMQ port: %v", err)
		portInt = 5672
	}
	username := getEnv("RABBITMQ_USER", "admin")
	password := getEnv("RABBITMQ_PASS", "password")

	// Create connection configuration
	config := &middleware.ConnectionConfig{
		Host:     host,
		Port:     portInt,
		Username: username,
		Password: password,
	}

	// Create client request handler
	clientRequestHandler := client_request_handler.NewClientRequestHandler(config)

	return &TCPServer{
		clientRequestHandler: clientRequestHandler,
	}
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

		// Handle each connection in a goroutine using the client request handler
		go s.clientRequestHandler.HandleConnection(conn)
	}
}

// Stop stops the TCP server
func (s *TCPServer) Stop() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}
