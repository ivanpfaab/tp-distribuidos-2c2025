package main

import (
	"fmt"
	"log"

	"echo-server/controllers"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://admin:password@rabbitmq:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// Create a channel
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
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
	failOnError(err, "Failed to declare request queue")

	// Set up consumer for requests
	msgs, err := ch.Consume(
		requestQueue.Name, // queue
		"",                // consumer
		false,             // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	failOnError(err, "Failed to register consumer")

	// Create client request handler
	requestHandler := controllers.NewClientRequestHandler()

	// Create publish function for the handler
	publishFunc := func(queueName, message, routingKey string) error {
		return ch.Publish(
			"",        // exchange
			queueName, // routing key
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(message),
			},
		)
	}

	fmt.Printf("Server with Client Request Handler listening for messages on queue: %s\n", requestQueue.Name)

	// Process messages using the client request handler
	for d := range msgs {
		err := requestHandler.HandleRequest(d, publishFunc)
		if err != nil {
			log.Printf("Failed to handle request: %v", err)
		}
	}
}
