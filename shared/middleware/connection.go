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
