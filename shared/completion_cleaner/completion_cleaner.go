package completion_cleaner

import (
	"fmt"
	"log"

	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
)

// CleanupHandler defines the interface for resources that can clean up client-specific data
type CleanupHandler interface {
	CleanClient(clientID string) error
}

// CompletionCleaner handles client completion signals and triggers cleanup for all registered handlers
type CompletionCleaner struct {
	consumer        *exchange.ExchangeConsumer
	cleanupHandlers []CleanupHandler
	workerID        string
}

// NewCompletionCleaner creates a new CompletionCleaner instance
// exchangeName: Name of the fanout exchange (declared by client request handler)
// workerID: Unique identifier for this worker (used to generate queue name: {workerID}-cleanup)
// cleanupHandlers: List of resources that implement CleanupHandler interface
// config: RabbitMQ connection configuration
func NewCompletionCleaner(
	exchangeName string,
	workerID string,
	cleanupHandlers []CleanupHandler,
	config *middleware.ConnectionConfig,
) (*CompletionCleaner, error) {
	if exchangeName == "" {
		return nil, fmt.Errorf("exchangeName is required")
	}
	if workerID == "" {
		return nil, fmt.Errorf("workerID is required")
	}
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}

	// Create exchange consumer (fanout exchange, no routing keys needed)
	consumer := exchange.NewExchangeConsumer(exchangeName, []string{}, config)
	if consumer == nil {
		return nil, fmt.Errorf("failed to create exchange consumer for '%s'", exchangeName)
	}

	// Set queue name: {workerID}-cleanup
	queueName := fmt.Sprintf("%s-cleanup", workerID)
	consumer.SetQueueName(queueName)

	return &CompletionCleaner{
		consumer:        consumer,
		cleanupHandlers: cleanupHandlers,
		workerID:        workerID,
	}, nil
}

// Start starts consuming completion signals and processing cleanup
func (cc *CompletionCleaner) Start() middleware.MessageMiddlewareError {
	log.Printf("Completion Cleaner (%s): Starting to listen for client completion signals...", cc.workerID)
	return cc.consumer.StartConsuming(cc.createCallback())
}

// Close closes the completion cleaner
func (cc *CompletionCleaner) Close() error {
	if cc.consumer != nil {
		if err := cc.consumer.Close(); err != 0 {
			return fmt.Errorf("failed to close completion cleaner consumer: %v", err)
		}
	}
	return nil
}

// createCallback creates the message processing callback
func (cc *CompletionCleaner) createCallback() func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Printf("Completion Cleaner (%s): Callback started, waiting for completion signals...", cc.workerID)
		for delivery := range *consumeChannel {
			// Deserialize the ClientCompletionSignal
			signal, err := signals.DeserializeClientCompletionSignal(delivery.Body)
			if err != nil {
				log.Printf("Completion Cleaner (%s): Failed to deserialize completion signal: %v", cc.workerID, err)
				delivery.Ack(false) // ACK to avoid infinite retries on malformed messages
				continue
			}

			// Process the completion signal
			if err := cc.processCompletionSignal(signal); err != nil {
				log.Printf("Completion Cleaner (%s): Failed to process completion signal for client %s: %v", cc.workerID, signal.ClientID, err)
				delivery.Nack(false, true) // Reject and requeue on processing failure
				continue
			}

			// ACK only after successful cleanup
			delivery.Ack(false)
			log.Printf("Completion Cleaner (%s): Successfully cleaned up resources for client %s", cc.workerID, signal.ClientID)
		}
		log.Printf("Completion Cleaner (%s): Consume channel closed", cc.workerID)
		done <- nil
	}
}

// processCompletionSignal processes a client completion signal by calling all cleanup handlers
func (cc *CompletionCleaner) processCompletionSignal(signal *signals.ClientCompletionSignal) error {
	clientID := signal.ClientID
	log.Printf("Completion Cleaner (%s): Processing completion signal for client %s", cc.workerID, clientID)

	// Call all cleanup handlers synchronously
	// Collect all errors and return if any handler fails
	var errors []error
	for i, handler := range cc.cleanupHandlers {
		if handler == nil {
			log.Printf("Completion Cleaner (%s): Warning - cleanup handler %d is nil, skipping", cc.workerID, i)
			continue
		}

		if err := handler.CleanClient(clientID); err != nil {
			log.Printf("Completion Cleaner (%s): Cleanup handler %d failed for client %s: %v", cc.workerID, i, clientID, err)
			errors = append(errors, fmt.Errorf("handler %d: %w", i, err))
		} else {
			log.Printf("Completion Cleaner (%s): Cleanup handler %d succeeded for client %s", cc.workerID, i, clientID)
		}
	}

	// Return error if any handler failed
	if len(errors) > 0 {
		return fmt.Errorf("cleanup failed for %d handler(s): %v", len(errors), errors)
	}

	log.Printf("Completion Cleaner (%s): All cleanup handlers completed successfully for client %s", cc.workerID, clientID)
	return nil
}
