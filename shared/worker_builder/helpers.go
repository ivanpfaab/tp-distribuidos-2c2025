package worker_builder

import (
	"fmt"
	"os"
	"path/filepath"

	completioncleaner "github.com/tp-distribuidos-2c2025/shared/completion_cleaner"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// BuildResult holds all resources created by the builder
// Workers can use this to extract resources after building
type BuildResult struct {
	Config             *middleware.ConnectionConfig
	QueueConsumers     map[string]*workerqueue.QueueConsumer
	QueueProducers     map[string]*workerqueue.QueueMiddleware
	ExchangeConsumers  map[string]*exchange.ExchangeConsumer
	ExchangeProducers  map[string]*exchange.ExchangeMiddleware
	MessageManager     interface{} // *messagemanager.MessageManager
	StateManager       interface{} // *statefulworker.StatefulWorkerManager
	PartitionManager   interface{} // *partitionmanager.PartitionManager
	DictionaryConfig   *DictionaryManagerConfig
	CompletionTrackers map[string]interface{} // *shared.CompletionTracker
	CompletionCleaner  *completioncleaner.CompletionCleaner
	ResourceTracker    *ResourceTracker
}

// Build extracts all resources from the builder into a BuildResult
// This makes it easy for workers to access all their resources
func (wb *WorkerBuilder) Build() (*BuildResult, error) {
	// Validate before building
	if err := wb.Validate(); err != nil {
		return nil, wb.CleanupOnError(err)
	}

	result := &BuildResult{
		Config:             wb.config,
		QueueConsumers:     make(map[string]*workerqueue.QueueConsumer),
		QueueProducers:     make(map[string]*workerqueue.QueueMiddleware),
		ExchangeConsumers:  make(map[string]*exchange.ExchangeConsumer),
		ExchangeProducers:  make(map[string]*exchange.ExchangeMiddleware),
		CompletionTrackers: make(map[string]interface{}),
		ResourceTracker:    wb.resourceTracker,
	}

	// Extract all resources by type
	tracker := wb.resourceTracker

	// Extract queue consumers
	queueConsumers := tracker.GetAllByType(ResourceTypeQueueConsumer)
	for _, resource := range queueConsumers {
		if consumer, ok := resource.(*workerqueue.QueueConsumer); ok {
			// Get queue name from embedded MessageMiddlewareQueue
			queueName := consumer.MessageMiddlewareQueue.QueueName
			if queueName != "" {
				result.QueueConsumers[queueName] = consumer
			}
		}
	}

	// Extract queue producers
	queueProducers := tracker.GetAllByType(ResourceTypeQueueProducer)
	for _, resource := range queueProducers {
		if producer, ok := resource.(*workerqueue.QueueMiddleware); ok {
			queueName := producer.MessageMiddlewareQueue.QueueName
			if queueName != "" {
				result.QueueProducers[queueName] = producer
			}
		}
	}

	// Extract exchange consumers
	exchangeConsumers := tracker.GetAllByType(ResourceTypeExchangeConsumer)
	for _, resource := range exchangeConsumers {
		if consumer, ok := resource.(*exchange.ExchangeConsumer); ok {
			exchangeName := consumer.MessageMiddlewareExchange.ExchangeName
			if exchangeName != "" {
				result.ExchangeConsumers[exchangeName] = consumer
			}
		}
	}

	// Extract exchange producers
	exchangeProducers := tracker.GetAllByType(ResourceTypeExchangeProducer)
	for _, resource := range exchangeProducers {
		if producer, ok := resource.(*exchange.ExchangeMiddleware); ok {
			exchangeName := producer.MessageMiddlewareExchange.ExchangeName
			if exchangeName != "" {
				result.ExchangeProducers[exchangeName] = producer
			}
		}
	}

	// Extract MessageManager
	if msgMgr := tracker.Get(ResourceTypeMessageManager, "message-manager"); msgMgr != nil {
		result.MessageManager = msgMgr
	}

	// Extract StateManager
	if stateMgr := tracker.Get(ResourceTypeStateManager, "state-manager"); stateMgr != nil {
		result.StateManager = stateMgr
	}

	// Extract PartitionManager
	if partMgr := tracker.Get(ResourceTypePartitionManager, "partition-manager"); partMgr != nil {
		result.PartitionManager = partMgr
	}

	// Extract DictionaryManager config
	if dictConfig := tracker.Get(ResourceTypeDictionaryManager, "dictionary-config"); dictConfig != nil {
		if config, ok := dictConfig.(DictionaryManagerConfig); ok {
			result.DictionaryConfig = &config
		}
	}

	// Extract CompletionTrackers
	completionTrackers := tracker.GetAllByType(ResourceTypeCompletionTracker)
	for _, resource := range completionTrackers {
		// We'll use Get() to find by name - but we need to know the names
		// For now, store all trackers and let workers access by type assertion
		// Workers can use GetCompletionTracker() method instead
		result.CompletionTrackers["completion-tracker"] = resource
	}

	// Extract CompletionCleaner
	if cleaner := tracker.Get(ResourceTypeCompletionCleaner, "completion-cleaner"); cleaner != nil {
		if cc, ok := cleaner.(*completioncleaner.CompletionCleaner); ok {
			result.CompletionCleaner = cc
		}
	}

	// Clear the resource tracker (resources are now owned by the worker)
	tracker.Clear()

	return result, nil
}

// WithStandardStateDirectory is a convenience method that sets up a standard state directory
// and MessageManager in one call
func (wb *WorkerBuilder) WithStandardStateDirectory(stateDir string) *WorkerBuilder {
	return wb.
		WithDirectory(stateDir, 0755).
		WithMessageManager(filepath.Join(stateDir, "processed-ids.txt"))
}

// WithStandardMetadataDirectory is a convenience method that sets up a standard metadata directory
// for StatefulWorkerManager
func (wb *WorkerBuilder) WithStandardMetadataDirectory(baseDir string) *WorkerBuilder {
	metadataDir := filepath.Join(baseDir, "metadata")
	return wb.WithDirectory(metadataDir, 0755)
}

// WithStandardDictionaryDirectory is a convenience method that sets up a standard dictionary directory
func (wb *WorkerBuilder) WithStandardDictionaryDirectory(baseDir string) *WorkerBuilder {
	dictDir := filepath.Join(baseDir, "dictionaries")
	return wb.WithDirectory(dictDir, 0755)
}

// LoadConfigFromEnv is a convenience function to load RabbitMQ config from environment variables
func LoadConfigFromEnv() (*middleware.ConnectionConfig, error) {
	host := getEnv("RABBITMQ_HOST", "rabbitmq")
	portStr := getEnv("RABBITMQ_PORT", "5672")
	username := getEnv("RABBITMQ_USER", "admin")
	password := getEnv("RABBITMQ_PASS", "password")

	port, err := parseInt(portStr)
	if err != nil {
		return nil, fmt.Errorf("invalid RABBITMQ_PORT: %w", err)
	}

	return &middleware.ConnectionConfig{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
	}, nil
}

// Helper functions for environment variable loading
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func parseInt(s string) (int, error) {
	var result int
	_, err := fmt.Sscanf(s, "%d", &result)
	return result, err
}
