package config

import (
	"os"
	"strconv"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// GetEnv gets an environment variable with a default value
func GetEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// ServerConfig holds server configuration
type ServerConfig struct {
	Port string
}

// NewServerConfig creates a new server configuration from environment
func NewServerConfig() *ServerConfig {
	return &ServerConfig{
		Port: GetEnv("SERVER_PORT", "8080"),
	}
}

// RabbitMQConfig holds RabbitMQ connection configuration
type RabbitMQConfig struct {
	Host     string
	Port     int
	Username string
	Password string
}

// NewRabbitMQConfig creates a new RabbitMQ configuration from environment
func NewRabbitMQConfig() *RabbitMQConfig {
	host := GetEnv("RABBITMQ_HOST", "localhost")
	portStr := GetEnv("RABBITMQ_PORT", "5672")
	portInt, err := strconv.Atoi(portStr)
	if err != nil {
		// Default to 5672 if invalid
		portInt = 5672
	}
	username := GetEnv("RABBITMQ_USER", "admin")
	password := GetEnv("RABBITMQ_PASS", "password")

	return &RabbitMQConfig{
		Host:     host,
		Port:     portInt,
		Username: username,
		Password: password,
	}
}

// ToConnectionConfig converts RabbitMQConfig to middleware.ConnectionConfig
func (r *RabbitMQConfig) ToConnectionConfig() *middleware.ConnectionConfig {
	return &middleware.ConnectionConfig{
		Host:     r.Host,
		Port:     r.Port,
		Username: r.Username,
		Password: r.Password,
	}
}

