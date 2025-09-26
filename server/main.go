package main

import (
	"fmt"
	"log"

	"shared/rabbitmq"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	// Connect to RabbitMQ using the shared module
	config := rabbitmq.DefaultConfig()
	conn, err := rabbitmq.NewConnection(config)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// Declare the request queue
	requestQueue, err := conn.DeclareQueue("echo_requests")
	failOnError(err, "Failed to declare request queue")

	// Declare the response queue
	responseQueue, err := conn.DeclareQueue("echo_responses")
	failOnError(err, "Failed to declare response queue")

	// Set up consumer for requests
	msgs, err := conn.ConsumeMessages(requestQueue.Name)
	failOnError(err, "Failed to register consumer")

	fmt.Printf("Echo server listening for messages on queue: %s\n", requestQueue.Name)

	// Process messages
	for d := range msgs {
		message := string(d.Body)
		fmt.Printf("Received: %s\n", message)

		// Echo the message back to the client
		response := fmt.Sprintf("Echo: %s", message)

		// Get the reply-to queue from the message properties
		replyTo := d.ReplyTo
		if replyTo == "" {
			// If no reply-to specified, use the response queue
			replyTo = responseQueue.Name
		}

		// Publish response
		err = conn.PublishMessage(replyTo, response, "")
		if err != nil {
			log.Printf("Failed to publish response: %v", err)
		} else {
			fmt.Printf("Sent response: %s\n", response)
		}
	}
}
