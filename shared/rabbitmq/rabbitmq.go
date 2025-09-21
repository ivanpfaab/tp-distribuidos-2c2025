package rabbitmq

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

// Config holds RabbitMQ connection configuration
type Config struct {
	URL      string
	Username string
	Password string
	Host     string
	Port     string
	VHost    string
}

// DefaultConfig returns a default RabbitMQ configuration
func DefaultConfig() *Config {
	return &Config{
		Username: "admin",
		Password: "password",
		Host:     "rabbitmq",
		Port:     "5672",
		VHost:    "/",
	}
}

// Connection wraps RabbitMQ connection and channel
type Connection struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	config  *Config
}

// NewConnection creates a new RabbitMQ connection with retry logic
func NewConnection(config *Config) (*Connection, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Build connection URL
	if config.URL == "" {
		config.URL = fmt.Sprintf("amqp://%s:%s@%s:%s%s", 
			config.Username, config.Password, config.Host, config.Port, config.VHost)
	}

	var conn *amqp.Connection
	var err error
	maxRetries := 10

	for i := 0; i < maxRetries; i++ {
		conn, err = amqp.Dial(config.URL)
		if err == nil {
			break
		}
		log.Printf("Failed to connect to RabbitMQ (attempt %d/%d): %v", i+1, maxRetries, err)
		if i < maxRetries-1 {
			time.Sleep(2 * time.Second)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ after %d attempts: %v", maxRetries, err)
	}

	// Create channel
	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %v", err)
	}

	return &Connection{
		conn:    conn,
		channel: channel,
		config:  config,
	}, nil
}

// Close closes the RabbitMQ connection and channel
func (c *Connection) Close() error {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// DeclareQueue declares a queue with the given name
func (c *Connection) DeclareQueue(name string) (amqp.Queue, error) {
	return c.channel.QueueDeclare(
		name,  // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
}

// PublishMessage publishes a message to a queue
func (c *Connection) PublishMessage(queueName, message string, replyTo string) error {
	return c.channel.Publish(
		"",        // exchange
		queueName, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
			ReplyTo:     replyTo,
		},
	)
}

// ConsumeMessages starts consuming messages from a queue
func (c *Connection) ConsumeMessages(queueName string) (<-chan amqp.Delivery, error) {
	return c.channel.Consume(
		queueName, // queue
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
}

// GetChannel returns the underlying AMQP channel
func (c *Connection) GetChannel() *amqp.Channel {
	return c.channel
}

// GetConnection returns the underlying AMQP connection
func (c *Connection) GetConnection() *amqp.Connection {
	return c.conn
}
