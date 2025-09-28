package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"shared/rabbitmq"
	"strings"

	"github.com/streadway/amqp"
)

// startResponseReader launches a goroutine that prints responses from the server.
func startResponseReader(conn *rabbitmq.Connection, responseQueue amqp.Queue) {
	go func() {
		// Set up consumer for responses
		msgs, err := conn.ConsumeMessages(responseQueue.Name)
		if err != nil {
			log.Fatalf("Failed to register consumer: %v", err)
		}
		for d := range msgs {
			response := string(d.Body)
			fmt.Printf("Server response: %s\n", response)
		}
	}()
}

// sendBatches reads CSV records, groups them into batches, and sends them.
func readCSAndSendBatches(conn *rabbitmq.Connection, requestQueue amqp.Queue, responseQueue amqp.Queue, r *csv.Reader, batchSize int) (int, int, error) {
	recordsSent := 0
	batchesSent := 0
	var batch []string

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return recordsSent, batchesSent, fmt.Errorf("error reading CSV: %v", err)
		}

		batch = append(batch, strings.Join(record, ","))
		if len(batch) >= batchSize {
			err = conn.PublishMessage(requestQueue.Name, strings.Join(batch, "\n"), responseQueue.Name)
			if err != nil {
				return recordsSent, batchesSent, fmt.Errorf("error publishing message: %v", err)
			}
			recordsSent += len(batch)
			batchesSent++
			batch = batch[:0] // Reset batch
		}
	}
	// Send any remaining records in the last batch
	if len(batch) > 0 {
		err := conn.PublishMessage(requestQueue.Name, strings.Join(batch, "\n"), responseQueue.Name)
		if err != nil {
			return recordsSent, batchesSent, fmt.Errorf("error publishing message: %v", err)
		}
		recordsSent += len(batch)
		batchesSent++
	}
	return recordsSent, batchesSent, nil

}

func connectRabbitMQandDeclareQueues() (*rabbitmq.Connection, amqp.Queue, amqp.Queue) {

	// Connect to RabbitMQ using the shared module
	config := rabbitmq.DefaultConfig()
	conn, err := rabbitmq.NewConnection(config)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Declare the request queue
	requestQueue, err := conn.DeclareQueue("echo_requests")
	if err != nil {
		log.Fatalf("Failed to declare request queue: %v", err)
	}

	// Declare the response queue
	responseQueue, err := conn.DeclareQueue("echo_responses")
	if err != nil {
		log.Fatalf("Failed to declare response queue: %v", err)
	}

	return conn, requestQueue, responseQueue
}

func main() {
	batchSize := flag.Int("batch", 10, "Number of CSV records per batch")
	flag.Parse()

	if flag.NArg() < 1 {
		log.Fatal("Usage: ./client [-batch N] <input.csv>")
	}
	inputFile := flag.Arg(0)

	f, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("Failed to open CSV: %v", err)
	}
	defer f.Close()

	fmt.Printf("Connected to RabbitMQ\n")
	fmt.Printf("Reading CSV: %s | batch size: %d\n", inputFile, *batchSize)

	conn, requestQueue, responseQueue := connectRabbitMQandDeclareQueues()

	// Start goroutine for server responses
	startResponseReader(conn, responseQueue)

	// Read CSV and send batches
	r := csv.NewReader(f)
	records, batches, err := readCSAndSendBatches(conn, requestQueue, responseQueue, r, *batchSize)
	if err != nil {
		log.Fatalf("Transmission error: %v", err)
	}

	fmt.Printf("Finished sending %d record(s) in %d batch(es)\n", records, batches)
}
