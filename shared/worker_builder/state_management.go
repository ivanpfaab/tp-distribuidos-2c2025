package worker_builder

import (
	"fmt"

	partitionmanager "github.com/tp-distribuidos-2c2025/shared/partition_manager"
	statefulworker "github.com/tp-distribuidos-2c2025/shared/stateful_worker"
	"github.com/tp-distribuidos-2c2025/workers/shared"
)

// StatefulWorkerManagerConfig holds configuration for StatefulWorkerManager
type StatefulWorkerManagerConfig struct {
	MetadataDir     string
	BuildStatus     statefulworker.StatusBuilderFunc
	ExtractMetadata statefulworker.MetadataExtractor
	CSVColumns      []string
	RebuildOnStart  bool
}

// WithStatefulWorkerManager adds a StatefulWorkerManager for CSV-based state persistence
func (wb *WorkerBuilder) WithStatefulWorkerManager(config StatefulWorkerManagerConfig) *WorkerBuilder {
	if config.MetadataDir == "" {
		wb.addError(fmt.Errorf("metadataDir is required for StatefulWorkerManager"))
		return wb
	}
	if config.BuildStatus == nil {
		wb.addError(fmt.Errorf("buildStatus function is required for StatefulWorkerManager"))
		return wb
	}
	if config.ExtractMetadata == nil {
		wb.addError(fmt.Errorf("extractMetadata function is required for StatefulWorkerManager"))
		return wb
	}
	if len(config.CSVColumns) == 0 {
		wb.addError(fmt.Errorf("csvColumns is required for StatefulWorkerManager"))
		return wb
	}

	// Ensure metadata directory exists
	if err := wb.resourceTracker.EnsureDirectory(config.MetadataDir, 0755); err != nil {
		wb.addError(fmt.Errorf("failed to create metadata directory: %w", err))
		return wb
	}

	// Create StatefulWorkerManager
	stateManager := statefulworker.NewStatefulWorkerManager(
		config.MetadataDir,
		config.BuildStatus,
		config.ExtractMetadata,
		config.CSVColumns,
	)

	// Register for cleanup
	wb.resourceTracker.Register(
		ResourceTypeStateManager,
		"state-manager",
		stateManager,
		func() error {
			// StatefulWorkerManager doesn't have a Close method, but we could clean up client states
			return nil
		},
	)

	// Rebuild state on startup if requested
	if config.RebuildOnStart {
		if err := stateManager.RebuildState(); err != nil {
			// Log warning but don't fail - state rebuild is best effort
			fmt.Printf("%s: Warning - failed to rebuild state: %v\n", wb.workerName, err)
		} else {
			fmt.Printf("%s: State rebuilt successfully from metadata\n", wb.workerName)
		}
	}

	return wb
}

// PartitionManagerConfig holds configuration for PartitionManager
type PartitionManagerConfig struct {
	PartitionsDir           string
	NumPartitions           int
	RecoverIncompleteWrites bool
}

// WithPartitionManager adds a PartitionManager for fault-tolerant partition file writing
func (wb *WorkerBuilder) WithPartitionManager(config PartitionManagerConfig) *WorkerBuilder {
	if config.PartitionsDir == "" {
		wb.addError(fmt.Errorf("partitionsDir is required for PartitionManager"))
		return wb
	}
	if config.NumPartitions <= 0 {
		wb.addError(fmt.Errorf("numPartitions must be greater than 0"))
		return wb
	}

	// Ensure partitions directory exists
	if err := wb.resourceTracker.EnsureDirectory(config.PartitionsDir, 0755); err != nil {
		wb.addError(fmt.Errorf("failed to create partitions directory: %w", err))
		return wb
	}

	// Create PartitionManager
	partitionManager, err := partitionmanager.NewPartitionManager(config.PartitionsDir, config.NumPartitions)
	if err != nil {
		wb.addError(fmt.Errorf("failed to create partition manager: %w", err))
		return wb
	}

	// Register for cleanup
	wb.resourceTracker.Register(
		ResourceTypePartitionManager,
		"partition-manager",
		partitionManager,
		func() error {
			// PartitionManager doesn't have a Close method
			return nil
		},
	)

	// Recover incomplete writes on startup if requested
	if config.RecoverIncompleteWrites {
		fixedCount, err := partitionManager.DeleteIncompleteLines()
		if err != nil {
			fmt.Printf("%s: Warning - failed to recover incomplete writes: %v\n", wb.workerName, err)
		} else if fixedCount > 0 {
			fmt.Printf("%s: Fixed %d incomplete last lines on startup\n", wb.workerName, fixedCount)
		}
	}

	return wb
}

// DictionaryManagerConfig holds configuration for DictionaryManager
type DictionaryManagerConfig struct {
	DictDir        string
	ParseFunc      interface{} // dictionary.ParseCallback[T] - generic function
	RebuildOnStart bool
}

// WithDictionaryManager adds a DictionaryManager for in-memory dictionary management
// Note: This is a generic function, so we use interface{} for the parseFunc
// The actual DictionaryManager will be created by the worker using the appropriate type
func (wb *WorkerBuilder) WithDictionaryManager(config DictionaryManagerConfig) *WorkerBuilder {
	if config.DictDir == "" {
		wb.addError(fmt.Errorf("dictDir is required for DictionaryManager"))
		return wb
	}
	if config.ParseFunc == nil {
		wb.addError(fmt.Errorf("parseFunc is required for DictionaryManager"))
		return wb
	}

	// Ensure dictionary directory exists
	if err := wb.resourceTracker.EnsureDirectory(config.DictDir, 0755); err != nil {
		wb.addError(fmt.Errorf("failed to create dictionary directory: %w", err))
		return wb
	}

	// Store dictionary config for later use (DictionaryManager is created by worker with specific type)
	// We'll register the config itself so workers can retrieve it
	wb.resourceTracker.Register(
		ResourceTypeDictionaryManager,
		"dictionary-config",
		config,
		func() error {
			// No cleanup needed for config
			return nil
		},
	)

	// Note: DictionaryManager rebuild is handled by the worker itself since it needs the specific type
	// This builder just ensures the directory exists and stores the config

	return wb
}

// CompletionTrackerConfig holds configuration for CompletionTracker
type CompletionTrackerConfig struct {
	TrackerName string
	Callback    shared.CompletionCallback
}

// WithCompletionTracker adds a CompletionTracker for chunk completion tracking
func (wb *WorkerBuilder) WithCompletionTracker(config CompletionTrackerConfig) *WorkerBuilder {
	if config.TrackerName == "" {
		wb.addError(fmt.Errorf("trackerName is required for CompletionTracker"))
		return wb
	}
	if config.Callback == nil {
		wb.addError(fmt.Errorf("callback is required for CompletionTracker"))
		return wb
	}

	// Create CompletionTracker
	completionTracker := shared.NewCompletionTracker(config.TrackerName, config.Callback)

	// Register for cleanup
	wb.resourceTracker.Register(
		ResourceTypeCompletionTracker,
		config.TrackerName,
		completionTracker,
		func() error {
			// CompletionTracker doesn't have a Close method, but we could clear all states
			// For now, no cleanup needed
			return nil
		},
	)

	return wb
}

// GetStatefulWorkerManager retrieves the StatefulWorkerManager
func (wb *WorkerBuilder) GetStatefulWorkerManager() *statefulworker.StatefulWorkerManager {
	resource := wb.resourceTracker.Get(ResourceTypeStateManager, "state-manager")
	if resource == nil {
		return nil
	}
	stateManager, ok := resource.(*statefulworker.StatefulWorkerManager)
	if !ok {
		return nil
	}
	return stateManager
}

// GetPartitionManager retrieves the PartitionManager
func (wb *WorkerBuilder) GetPartitionManager() *partitionmanager.PartitionManager {
	resource := wb.resourceTracker.Get(ResourceTypePartitionManager, "partition-manager")
	if resource == nil {
		return nil
	}
	partitionManager, ok := resource.(*partitionmanager.PartitionManager)
	if !ok {
		return nil
	}
	return partitionManager
}

// GetDictionaryManagerConfig retrieves the DictionaryManager configuration
func (wb *WorkerBuilder) GetDictionaryManagerConfig() *DictionaryManagerConfig {
	resource := wb.resourceTracker.Get(ResourceTypeDictionaryManager, "dictionary-config")
	if resource == nil {
		return nil
	}
	config, ok := resource.(DictionaryManagerConfig)
	if !ok {
		return nil
	}
	return &config
}

// GetCompletionTracker retrieves a CompletionTracker by name
func (wb *WorkerBuilder) GetCompletionTracker(trackerName string) *shared.CompletionTracker {
	resource := wb.resourceTracker.Get(ResourceTypeCompletionTracker, trackerName)
	if resource == nil {
		return nil
	}
	tracker, ok := resource.(*shared.CompletionTracker)
	if !ok {
		return nil
	}
	return tracker
}

