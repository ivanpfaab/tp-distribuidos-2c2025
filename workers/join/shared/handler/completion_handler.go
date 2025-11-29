package handler

import (
	"fmt"
	"os"
	"path/filepath"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/workers/join/shared/dictionary"
)

// CompletionHandler processes completion signals for cleanup
type CompletionHandler[T any] struct {
	manager    *dictionary.Manager[T]
	dictDir    string
	workerName string // For logging
}

// NewCompletionHandler creates a new completion handler
func NewCompletionHandler[T any](
	manager *dictionary.Manager[T],
	dictDir string,
	workerName string,
) *CompletionHandler[T] {
	return &CompletionHandler[T]{
		manager:    manager,
		dictDir:    dictDir,
		workerName: workerName,
	}
}

// ProcessMessage processes a completion signal
func (ch *CompletionHandler[T]) ProcessMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	// Deserialize the completion signal
	completionSignal, err := signals.DeserializeJoinCompletionSignal(delivery.Body)
	if err != nil {
		fmt.Printf("%s: Failed to deserialize completion signal: %v\n", ch.workerName, err)
		delivery.Ack(false)
		return middleware.MessageMiddlewareMessageError
	}

	fmt.Printf("%s: Received completion signal for client %s\n", ch.workerName, completionSignal.ClientID)

	// Clean up client data if exists
	if ch.manager.HasClient(completionSignal.ClientID) {
		ch.manager.CleanupClient(completionSignal.ClientID)
		fmt.Printf("%s: Cleaned up data for client %s\n", ch.workerName, completionSignal.ClientID)
	} else {
		fmt.Printf("%s: Client %s not in dictionary, ignoring signal\n", ch.workerName, completionSignal.ClientID)
	}

	// Delete dictionary file
	filePath := filepath.Join(ch.dictDir, completionSignal.ClientID+".csv")
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		fmt.Printf("%s: Warning - failed to delete dictionary file for client %s: %v\n", ch.workerName, completionSignal.ClientID, err)
	} else if err == nil {
		fmt.Printf("%s: Deleted dictionary file for client %s\n", ch.workerName, completionSignal.ClientID)
	}

	delivery.Ack(false) // Acknowledge the message
	return 0
}

