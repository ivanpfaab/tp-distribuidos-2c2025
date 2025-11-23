package middleware

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ConnectionConfig holds configuration for RabbitMQ connections
type ConnectionConfig struct {
	URL      string
	Username string
	Password string
	Host     string
	Port     int
	VHost    string
}

// DefaultConnectionConfig returns a default configuration for local RabbitMQ
func DefaultConnectionConfig() *ConnectionConfig {
	return &ConnectionConfig{
		URL:      "amqp://guest:guest@localhost:5672/",
		Username: "guest",
		Password: "guest",
		Host:     "localhost",
		Port:     5672,
		VHost:    "/",
	}
}

// BuildURL constructs a RabbitMQ URL from the configuration
func (c *ConnectionConfig) BuildURL() string {
	if c.URL != "" {
		return c.URL
	}
	return fmt.Sprintf("amqp://%s:%s@%s:%d%s", c.Username, c.Password, c.Host, c.Port, c.VHost)
}

// TestConnection tests the RabbitMQ connection
func TestConnection(config *ConnectionConfig) error {
	conn, err := amqp.Dial(config.BuildURL())
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	return nil
}

// WaitForConnection waits for RabbitMQ to be available with retries
func WaitForConnection(config *ConnectionConfig, maxRetries int, retryInterval time.Duration) error {
	for i := 0; i < maxRetries; i++ {
		if err := TestConnection(config); err == nil {
			return nil
		}
		if i < maxRetries-1 {
			time.Sleep(retryInterval)
		}
	}
	return fmt.Errorf("failed to connect to RabbitMQ after %d retries", maxRetries)
}

// CreateConnection creates a new RabbitMQ connection
func CreateConnection(config *ConnectionConfig) (*amqp.Connection, error) {
	conn, err := amqp.Dial(config.BuildURL())
	if err != nil {
		return nil, fmt.Errorf("failed to create RabbitMQ connection: %w", err)
	}
	return conn, nil
}

// CreateChannel creates a new channel from a connection
func CreateChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}
	return ch, nil
}

// CreateMiddlewareChannel creates a channel for our middleware with QoS settings
func CreateMiddlewareChannel(config *ConnectionConfig) (MiddlewareChannel, error) {
	conn, err := CreateConnection(config)
	if err != nil {
		return nil, err
	}

	ch, err := CreateChannel(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}

	// Set QoS to prefetch only 1 message at a time
	// This prevents RabbitMQ from delivering all messages at once,
	// which can cause high memory consumption (2GB+)
	err = ch.Qos(
		1,     // prefetch count - only 1 message at a time
		0,     // prefetch size - 0 means no byte limit
		false, // global - false means apply per-consumer
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	return ch, nil
}
