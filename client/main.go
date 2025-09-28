package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"batch"

	amqp "github.com/rabbitmq/amqp091-go"
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

	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://admin:password@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Create a channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	// Declare the request queue
	requestQueue, err := ch.QueueDeclare(
		"echo_requests", // name
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare request queue: %v", err)
	}

	// Declare the response queue
	responseQueue, err := ch.QueueDeclare(
		"echo_responses", // name
		true,             // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare response queue: %v", err)
	}

	// Set up consumer for responses
	msgs, err := ch.Consume(
		responseQueue.Name, // queue
		"",                 // consumer
		true,               // auto-ack
		false,              // exclusive
		false,              // no-local
		false,              // no-wait
		nil,                // args
	)
	if err != nil {
		log.Fatalf("Failed to register consumer: %v", err)
	}

	fmt.Printf("Connected to RabbitMQ\n")
	fmt.Printf("Reading messages from file: %s\n", inputFile)

	// Start a goroutine to read responses from server
	go func() {
		for d := range msgs {
			response := string(d.Body)
			fmt.Printf("Server response: %s\n", response)
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

		// Create batch message
		batchData := &batch.Batch{
			ClientID:    "client-123",
			FileID:      "file-456",
			IsEOF:       strings.ToLower(message) == "exit",
			BatchNumber: lineCount,
			BatchSize:   len(message),
			BatchData:   message,
		}

		// Create batch message and serialize
		batchMsg := batch.NewBatchMessage(batchData)
		serializedData, err := batch.SerializeBatchMessage(batchMsg)
		if err != nil {
			log.Printf("Failed to serialize batch message: %v", err)
			break
		}

		// Send message to request queue
		err = ch.Publish(
			"",                // exchange
			requestQueue.Name, // routing key
			false,             // mandatory
			false,             // immediate
			amqp.Publishing{
				ContentType: "application/octet-stream",
				Body:        serializedData,
				ReplyTo:     responseQueue.Name,
			},
		)
		if err != nil {
			log.Printf("Failed to publish message: %v", err)
			break
		}

		// Small delay between messages to see responses clearly
		time.Sleep(100 * time.Millisecond)

		// Check for exit command
		if strings.ToLower(message) == "exit" {
			fmt.Println("Exit command found, waiting for response before stopping...")
			// Wait a bit longer for the final response
			time.Sleep(500 * time.Millisecond)
			break
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading file: %v", err)
	}

	fmt.Printf("Finished sending %d lines from file\n", lineCount)
	fmt.Println("Client completed successfully!")
}
