package notifier

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// OrchestratorNotifier sends chunk notifications to orchestrator
type OrchestratorNotifier struct {
	orchestratorProducer *workerqueue.QueueMiddleware
	workerType           string // "itemid-join-worker", "storeid-join-worker", "userid-join-worker"
}

// NewOrchestratorNotifier creates a new orchestrator notifier
func NewOrchestratorNotifier(
	orchestratorProducer *workerqueue.QueueMiddleware,
	workerType string,
) *OrchestratorNotifier {
	return &OrchestratorNotifier{
		orchestratorProducer: orchestratorProducer,
		workerType:           workerType,
	}
}

// SendChunkNotification sends a chunk notification to orchestrator
func (on *OrchestratorNotifier) SendChunkNotification(chunkMsg *chunk.Chunk) error {
	notification := signals.NewChunkNotification(
		chunkMsg.ClientID,
		chunkMsg.FileID,
		on.workerType,
		int(chunkMsg.TableID),
		int(chunkMsg.ChunkNumber),
		chunkMsg.IsLastChunk,
		chunkMsg.IsLastFromTable,
	)

	messageData, err := signals.SerializeChunkNotification(notification)
	if err != nil {
		return fmt.Errorf("failed to serialize chunk notification: %w", err)
	}

	if err := on.orchestratorProducer.Send(messageData); err != 0 {
		return fmt.Errorf("failed to send chunk notification: %v", err)
	}

	return nil
}
