package test

import (
	"os"
	"testing"

	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/partitioner"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name           string
		envVars        map[string]string
		expectError    bool
		expectedQuery  int
		expectedParts  int
		expectedBuffer int
	}{
		{
			name: "Valid Query Type 2 with defaults",
			envVars: map[string]string{
				"QUERY_TYPE": "2",
			},
			expectError:    false,
			expectedQuery:  2,
			expectedParts:  10,  // Default NUM_PARTITIONS
			expectedBuffer: 200, // Default MAX_BUFFER_SIZE
		},
		{
			name: "Valid Query Type 3 with custom values",
			envVars: map[string]string{
				"QUERY_TYPE":      "3",
				"NUM_PARTITIONS":  "15",
				"MAX_BUFFER_SIZE": "500",
				"RABBITMQ_URL":    "amqp://test:test@localhost:5672/",
			},
			expectError:    false,
			expectedQuery:  3,
			expectedParts:  15,
			expectedBuffer: 500,
		},
		{
			name: "Valid Query Type 4 with custom values",
			envVars: map[string]string{
				"QUERY_TYPE":      "4",
				"NUM_PARTITIONS":  "5",
				"MAX_BUFFER_SIZE": "100",
			},
			expectError:    false,
			expectedQuery:  4,
			expectedParts:  5,
			expectedBuffer: 100,
		},
		{
			name: "Missing QUERY_TYPE",
			envVars: map[string]string{
				"NUM_PARTITIONS": "10",
			},
			expectError: true,
		},
		{
			name: "Invalid QUERY_TYPE",
			envVars: map[string]string{
				"QUERY_TYPE": "invalid",
			},
			expectError: true,
		},
		{
			name: "Invalid QUERY_TYPE - out of range",
			envVars: map[string]string{
				"QUERY_TYPE": "1",
			},
			expectError: true,
		},
		{
			name: "Invalid QUERY_TYPE - too high",
			envVars: map[string]string{
				"QUERY_TYPE": "5",
			},
			expectError: true,
		},
		{
			name: "Invalid NUM_PARTITIONS",
			envVars: map[string]string{
				"QUERY_TYPE":     "2",
				"NUM_PARTITIONS": "invalid",
			},
			expectError:    false, // Should fall back to default
			expectedQuery:  2,
			expectedParts:  10,  // Default
			expectedBuffer: 200, // Default
		},
		{
			name: "Invalid MAX_BUFFER_SIZE",
			envVars: map[string]string{
				"QUERY_TYPE":      "2",
				"MAX_BUFFER_SIZE": "invalid",
			},
			expectError:    false, // Should fall back to default
			expectedQuery:  2,
			expectedParts:  10,  // Default
			expectedBuffer: 200, // Default
		},
		{
			name: "Zero NUM_PARTITIONS",
			envVars: map[string]string{
				"QUERY_TYPE":     "2",
				"NUM_PARTITIONS": "0",
			},
			expectError:    false, // Should fall back to default
			expectedQuery:  2,
			expectedParts:  10,  // Default
			expectedBuffer: 200, // Default
		},
		{
			name: "Zero MAX_BUFFER_SIZE",
			envVars: map[string]string{
				"QUERY_TYPE":      "2",
				"MAX_BUFFER_SIZE": "0",
			},
			expectError:    false, // Should fall back to default
			expectedQuery:  2,
			expectedParts:  10,  // Default
			expectedBuffer: 200, // Default
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear environment variables
			os.Clearenv()

			// Set test environment variables
			for key, value := range tt.envVars {
				os.Setenv(key, value)
			}

			config, err := partitioner.LoadConfig()

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if config.QueryType != tt.expectedQuery {
				t.Errorf("Expected QueryType %d, got %d", tt.expectedQuery, config.QueryType)
			}

			if config.NumPartitions != tt.expectedParts {
				t.Errorf("Expected NumPartitions %d, got %d", tt.expectedParts, config.NumPartitions)
			}

			if config.MaxBufferSize != tt.expectedBuffer {
				t.Errorf("Expected MaxBufferSize %d, got %d", tt.expectedBuffer, config.MaxBufferSize)
			}

			// Check queue name based on query type
			expectedQueueName := getExpectedQueueName(tt.expectedQuery)
			if config.QueueName != expectedQueueName {
				t.Errorf("Expected QueueName %s, got %s", expectedQueueName, config.QueueName)
			}

			// Check RabbitMQ URL
			expectedURL := tt.envVars["RABBITMQ_URL"]
			if expectedURL == "" {
				expectedURL = "amqp://guest:guest@localhost:5672/"
			}
			if config.ConnectionConfig.URL != expectedURL {
				t.Errorf("Expected URL %s, got %s", expectedURL, config.ConnectionConfig.URL)
			}
		})
	}
}

func getExpectedQueueName(queryType int) string {
	switch queryType {
	case 2:
		return "query2-map-queue"
	case 3:
		return "query3-map-queue"
	case 4:
		return "query4-map-queue"
	default:
		return ""
	}
}
