package notifier

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// CompletionNotifier sends completion signals
type CompletionNotifier struct {
	completionProducer *workerqueue.QueueMiddleware
	workerID           string
	entityType         string // "items", "stores", "users"
}

// NewCompletionNotifier creates a new completion notifier
func NewCompletionNotifier(
	completionProducer *workerqueue.QueueMiddleware,
	workerID string,
	entityType string,
) *CompletionNotifier {
	return &CompletionNotifier{
		completionProducer: completionProducer,
		workerID:           workerID,
		entityType:         entityType,
	}
}

// SendCompletionNotification sends a completion signal for a client
func (cn *CompletionNotifier) SendCompletionNotification(clientID string) error {
	completionSignal := signals.NewJoinCompletionSignal(clientID, cn.entityType, cn.workerID)

	messageData, err := signals.SerializeJoinCompletionSignal(completionSignal)
	if err != nil {
		return fmt.Errorf("failed to serialize completion signal: %w", err)
	}

	if err := cn.completionProducer.Send(messageData); err != 0 {
		return fmt.Errorf("failed to send completion signal: %v", err)
	}

	return nil
}
