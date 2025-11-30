package worker_builder

import (
	"fmt"
	"os"
	"path/filepath"

	messagemanager "github.com/tp-distribuidos-2c2025/shared/message_manager"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// WorkerBuilder is the base builder for creating workers
type WorkerBuilder struct {
	workerName      string
	config          *middleware.ConnectionConfig
	resourceTracker *ResourceTracker
	errors          []error
}

// NewWorkerBuilder creates a new worker builder
func NewWorkerBuilder(workerName string) *WorkerBuilder {
	return &WorkerBuilder{
		workerName:      workerName,
		resourceTracker: NewResourceTracker(),
		errors:          make([]error, 0),
	}
}

// WithConfig sets the RabbitMQ connection configuration
func (wb *WorkerBuilder) WithConfig(config *middleware.ConnectionConfig) *WorkerBuilder {
	if config == nil {
		wb.addError(fmt.Errorf("config cannot be nil"))
		return wb
	}
	wb.config = config
	return wb
}

// WithMessageManager initializes a MessageManager for deduplication
// stateFilePath: Path to the file storing processed message IDs
func (wb *WorkerBuilder) WithMessageManager(stateFilePath string) *WorkerBuilder {
	if wb.config == nil {
		wb.addError(fmt.Errorf("config must be set before adding MessageManager"))
		return wb
	}

	// Ensure parent directory exists
	parentDir := filepath.Dir(stateFilePath)
	if err := wb.resourceTracker.EnsureDirectory(parentDir, 0755); err != nil {
		wb.addError(fmt.Errorf("failed to create state directory: %w", err))
		return wb
	}

	// Create MessageManager
	messageManager := messagemanager.NewMessageManager(stateFilePath)

	// Try to load processed IDs (non-fatal if file doesn't exist)
	if err := messageManager.LoadProcessedIDs(); err != nil {
		// Log warning but continue - starting with empty state is acceptable
		fmt.Printf("%s: Warning - failed to load processed IDs: %v (starting with empty state)\n", wb.workerName, err)
	} else {
		count := messageManager.GetProcessedCount()
		fmt.Printf("%s: Loaded %d processed IDs\n", wb.workerName, count)
	}

	// Register for cleanup
	wb.resourceTracker.Register(
		ResourceTypeMessageManager,
		"message-manager",
		messageManager,
		func() error {
			return messageManager.Close()
		},
	)

	return wb
}

// WithDirectory ensures a directory exists and registers it for tracking
func (wb *WorkerBuilder) WithDirectory(path string, perm os.FileMode) *WorkerBuilder {
	if err := wb.resourceTracker.EnsureDirectory(path, perm); err != nil {
		wb.addError(fmt.Errorf("failed to create directory %s: %w", path, err))
		return wb
	}
	return wb
}

// GetConfig returns the connection configuration
func (wb *WorkerBuilder) GetConfig() *middleware.ConnectionConfig {
	return wb.config
}

// GetResourceTracker returns the resource tracker
func (wb *WorkerBuilder) GetResourceTracker() *ResourceTracker {
	return wb.resourceTracker
}

// GetWorkerName returns the worker name
func (wb *WorkerBuilder) GetWorkerName() string {
	return wb.workerName
}

// addError adds an error to the error list
func (wb *WorkerBuilder) addError(err error) {
	wb.errors = append(wb.errors, err)
}

// HasErrors returns true if there are any errors
func (wb *WorkerBuilder) HasErrors() bool {
	return len(wb.errors) > 0
}

// GetErrors returns all accumulated errors
func (wb *WorkerBuilder) GetErrors() []error {
	return wb.errors
}

// Validate checks if the builder is in a valid state
func (wb *WorkerBuilder) Validate() error {
	if wb.config == nil {
		return fmt.Errorf("config is required")
	}
	if len(wb.errors) > 0 {
		return fmt.Errorf("builder has %d error(s): %v", len(wb.errors), wb.errors)
	}
	return nil
}

// CleanupOnError cleans up all resources and returns a formatted error
func (wb *WorkerBuilder) CleanupOnError(err error) error {
	cleanupErr := wb.resourceTracker.CleanupAll()
	if cleanupErr != nil {
		return fmt.Errorf("error: %w; cleanup error: %v", err, cleanupErr)
	}
	return err
}

// CleanupFromIndex cleans up resources from a specific index
func (wb *WorkerBuilder) CleanupFromIndex(index int) error {
	return wb.resourceTracker.CleanupFromIndex(index)
}

// GetLastResourceIndex returns the current number of tracked resources
func (wb *WorkerBuilder) GetLastResourceIndex() int {
	return wb.resourceTracker.GetLastIndex()
}

