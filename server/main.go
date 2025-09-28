package main

import (
	"fmt"
	"log"

	"shared/rabbitmq"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func connectRabbitMQandDeclareQueues() (*rabbitmq.Connection, amqp.Queue, amqp.Queue) {
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
	return conn, requestQueue, responseQueue
}

func processMessages(conn *rabbitmq.Connection, requestQueue amqp.Queue, responseQueue amqp.Queue) {
	go func() {
		// Process messages
		// Set up consumer for requests
		msgs, err := conn.ConsumeMessages(requestQueue.Name)
		failOnError(err, "Failed to register consumer")

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

	}()
}

func main() {

	conn, requestQueue, responseQueue := connectRabbitMQandDeclareQueues()
	processMessages(conn, requestQueue, responseQueue)
}
