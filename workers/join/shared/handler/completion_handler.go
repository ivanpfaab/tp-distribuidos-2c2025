package handler

import (
	"fmt"
	"os"
	"path/filepath"

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
func (ch *CompletionHandler[T]) ProcessMessage(completionSignal *signals.JoinCompletionSignal) middleware.MessageMiddlewareError {

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

	return 0
}
