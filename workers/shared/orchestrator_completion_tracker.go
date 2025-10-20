package shared

import (
	"fmt"
	"log"
	"sync"

	"github.com/tp-distribuidos-2c2025/protocol/signals"
)

// FileStatus represents the status of a file being processed for a specific client
type FileStatus struct {
	FileID            string
	TableID           int
	ChunksReceived    int
	LastChunkNumber   int
	IsCompleted       bool
	LastChunkReceived bool
}

// ClientStatus represents the status of a client's data processing
type ClientStatus struct {
	ClientID           string
	FileStatuses       map[string]*FileStatus // Key: "fileID_tableID"
	CompletedFiles     int
	TotalExpectedFiles int  // Dynamically inferred from IsLastFromTable
	ExpectedFilesKnown bool // Whether we've inferred the total expected files yet
	IsCompleted        bool
}

// CompletionCallback is called when all files for a client are completed
type CompletionCallback func(clientID string, clientStatus *ClientStatus)

// CompletionTracker manages the tracking of chunk processing completion for multiple clients
type CompletionTracker struct {
	clientStatuses map[string]*ClientStatus
	mutex          sync.RWMutex
	onCompletion   CompletionCallback
	trackerName    string // For logging purposes (e.g., "Query2", "JoinOrchestrator")
}

// NewCompletionTracker creates a new completion tracker
func NewCompletionTracker(trackerName string, onCompletion CompletionCallback) *CompletionTracker {
	return &CompletionTracker{
		clientStatuses: make(map[string]*ClientStatus),
		onCompletion:   onCompletion,
		trackerName:    trackerName,
	}
}

// ProcessChunkNotification processes a chunk notification and updates the completion status
func (ct *CompletionTracker) ProcessChunkNotification(notification *signals.ChunkNotification) error {
	ct.mutex.Lock()
	defer ct.mutex.Unlock()

	// Initialize client status if not exists
	if ct.clientStatuses[notification.ClientID] == nil {
		ct.clientStatuses[notification.ClientID] = &ClientStatus{
			ClientID:           notification.ClientID,
			FileStatuses:       make(map[string]*FileStatus),
			CompletedFiles:     0,
			TotalExpectedFiles: 0,
			ExpectedFilesKnown: false,
			IsCompleted:        false,
		}
	}

	clientStatus := ct.clientStatuses[notification.ClientID]
	fileKey := fmt.Sprintf("%s_%d", notification.FileID, notification.TableID)

	// Initialize file status if not exists
	if clientStatus.FileStatuses[fileKey] == nil {
		clientStatus.FileStatuses[fileKey] = &FileStatus{
			FileID:            notification.FileID,
			TableID:           notification.TableID,
			ChunksReceived:    0,
			LastChunkNumber:   0,
			IsCompleted:       false,
			LastChunkReceived: false,
		}
	}

	fileStatus := clientStatus.FileStatuses[fileKey]

	// Increment chunks received
	fileStatus.ChunksReceived++

	log.Printf("[%s] Client %s - File %s (Table %d): Received chunk %d/%d (Total chunks: %d)",
		ct.trackerName, notification.ClientID, notification.FileID, notification.TableID,
		notification.ChunkNumber, fileStatus.LastChunkNumber,
		fileStatus.ChunksReceived)

	// If this is the last chunk, update last chunk info
	if notification.IsLastChunk {
		fileStatus.LastChunkNumber = notification.ChunkNumber
		fileStatus.LastChunkReceived = true

		log.Printf("[%s] Client %s - File %s (Table %d): Received LAST chunk %d",
			ct.trackerName, notification.ClientID, notification.FileID, notification.TableID, notification.ChunkNumber)

		// Check if file is completed
		if fileStatus.ChunksReceived == notification.ChunkNumber {
			ct.completeFileForClient(notification.ClientID, fileStatus)
		}
	} else if fileStatus.LastChunkReceived {
		// We already received the last chunk, check if this completes the file
		if fileStatus.ChunksReceived == fileStatus.LastChunkNumber {
			ct.completeFileForClient(notification.ClientID, fileStatus)
		}
	}

	// Infer total expected files from IsLastFromTable if not already known
	if notification.IsLastFromTable && !clientStatus.ExpectedFilesKnown {
		// FileID has 4 characters, last 2 are the file number
		if len(notification.FileID) >= 2 {
			fileNumberStr := notification.FileID[len(notification.FileID)-2:]
			// Parse the file number (e.g., "01" -> 1, "05" -> 5)
			var fileNumber int
			if _, err := fmt.Sscanf(fileNumberStr, "%d", &fileNumber); err == nil {
				clientStatus.TotalExpectedFiles = fileNumber
				clientStatus.ExpectedFilesKnown = true
				log.Printf("[%s] Client %s: Inferred total expected files = %d from FileID %s",
					ct.trackerName, notification.ClientID, fileNumber, notification.FileID)
			} else {
				log.Printf("[%s] Client %s: Failed to parse file number from FileID %s: %v",
					ct.trackerName, notification.ClientID, notification.FileID, err)
			}
		}
	}

	// Check if all files are completed for this client (only if we know the expected count)
	if clientStatus.ExpectedFilesKnown && clientStatus.CompletedFiles >= clientStatus.TotalExpectedFiles {
		log.Printf("[%s] Client %s: All %d files completed! Triggering completion callback...",
			ct.trackerName, notification.ClientID, clientStatus.TotalExpectedFiles)

		// Mark client as completed before calling callback
		clientStatus.IsCompleted = true

		// Call the completion callback
		if ct.onCompletion != nil {
			ct.onCompletion(notification.ClientID, clientStatus)
		}
	}

	return nil
}

// completeFileForClient marks a file as completed for a specific client
func (ct *CompletionTracker) completeFileForClient(clientID string, fileStatus *FileStatus) {
	if fileStatus.IsCompleted {
		return // Already completed
	}

	fileStatus.IsCompleted = true
	clientStatus := ct.clientStatuses[clientID]
	clientStatus.CompletedFiles++

	if clientStatus.ExpectedFilesKnown {
		log.Printf("[%s] ✅ Client %s - File %s (Table %d) COMPLETED! (%d/%d files completed)",
			ct.trackerName, clientID, fileStatus.FileID, fileStatus.TableID,
			clientStatus.CompletedFiles, clientStatus.TotalExpectedFiles)
	} else {
		log.Printf("[%s] ✅ Client %s - File %s (Table %d) COMPLETED! (%d files completed, total expected unknown yet)",
			ct.trackerName, clientID, fileStatus.FileID, fileStatus.TableID, clientStatus.CompletedFiles)
	}
}

// GetClientStatus returns a read-only copy of the client status
func (ct *CompletionTracker) GetClientStatus(clientID string) *ClientStatus {
	ct.mutex.RLock()
	defer ct.mutex.RUnlock()

	if status, exists := ct.clientStatuses[clientID]; exists {
		// Return a copy to prevent external modification
		statusCopy := *status
		return &statusCopy
	}
	return nil
}

// GetAllClientIDs returns all client IDs being tracked
func (ct *CompletionTracker) GetAllClientIDs() []string {
	ct.mutex.RLock()
	defer ct.mutex.RUnlock()

	clientIDs := make([]string, 0, len(ct.clientStatuses))
	for clientID := range ct.clientStatuses {
		clientIDs = append(clientIDs, clientID)
	}
	return clientIDs
}

// IsClientCompleted checks if a client has completed all files
func (ct *CompletionTracker) IsClientCompleted(clientID string) bool {
	ct.mutex.RLock()
	defer ct.mutex.RUnlock()

	if status, exists := ct.clientStatuses[clientID]; exists {
		return status.IsCompleted
	}
	return false
}

// ClearClientState removes all state for a specific client
func (ct *CompletionTracker) ClearClientState(clientID string) {
	ct.mutex.Lock()
	defer ct.mutex.Unlock()

	if _, exists := ct.clientStatuses[clientID]; exists {
		delete(ct.clientStatuses, clientID)
		log.Printf("[%s] Cleared state for client %s", ct.trackerName, clientID)
	}
}
