package worker_builder

import (
	"fmt"
	"os"
)

// ResourceType represents the type of resource being tracked
type ResourceType int

const (
	ResourceTypeQueueConsumer ResourceType = iota
	ResourceTypeQueueProducer
	ResourceTypeExchangeConsumer
	ResourceTypeExchangeProducer
	ResourceTypeMessageManager
	ResourceTypeStateManager
	ResourceTypePartitionManager
	ResourceTypeDictionaryManager
	ResourceTypeCompletionTracker
	ResourceTypeDirectory
)

// ClosableResource represents any resource that can be closed
type ClosableResource interface {
	Close() error
}

// Resource represents a tracked resource
type Resource struct {
	Type     ResourceType
	Name     string
	Resource interface{}
	closer   func() error
}

// ResourceTracker tracks all created resources for cleanup on error
type ResourceTracker struct {
	resources []*Resource
}

// NewResourceTracker creates a new resource tracker
func NewResourceTracker() *ResourceTracker {
	return &ResourceTracker{
		resources: make([]*Resource, 0),
	}
}

// Register registers a new resource for tracking
func (rt *ResourceTracker) Register(resourceType ResourceType, name string, resource interface{}, closer func() error) {
	rt.resources = append(rt.resources, &Resource{
		Type:     resourceType,
		Name:     name,
		Resource: resource,
		closer:   closer,
	})
}

// Get retrieves a resource by type and name
func (rt *ResourceTracker) Get(resourceType ResourceType, name string) interface{} {
	for _, r := range rt.resources {
		if r.Type == resourceType && r.Name == name {
			return r.Resource
		}
	}
	return nil
}

// GetAllByType retrieves all resources of a specific type
func (rt *ResourceTracker) GetAllByType(resourceType ResourceType) []interface{} {
	var results []interface{}
	for _, r := range rt.resources {
		if r.Type == resourceType {
			results = append(results, r.Resource)
		}
	}
	return results
}

// CleanupAll closes all tracked resources in reverse order
func (rt *ResourceTracker) CleanupAll() error {
	var lastErr error
	// Cleanup in reverse order
	for i := len(rt.resources) - 1; i >= 0; i-- {
		r := rt.resources[i]
		if r.closer != nil {
			if err := r.closer(); err != nil {
				lastErr = fmt.Errorf("failed to cleanup %s (%v): %w", r.Name, r.Type, err)
				// Continue cleanup even if one fails
			}
		}
	}
	return lastErr
}

// CleanupFromIndex closes all resources from the given index onwards (in reverse)
func (rt *ResourceTracker) CleanupFromIndex(index int) error {
	var lastErr error
	// Cleanup in reverse order from the index
	for i := len(rt.resources) - 1; i >= index; i-- {
		r := rt.resources[i]
		if r.closer != nil {
			if err := r.closer(); err != nil {
				lastErr = fmt.Errorf("failed to cleanup %s (%v): %w", r.Name, r.Type, err)
				// Continue cleanup even if one fails
			}
		}
	}
	return lastErr
}

// GetLastIndex returns the current number of tracked resources
func (rt *ResourceTracker) GetLastIndex() int {
	return len(rt.resources)
}

// Clear removes all tracked resources (without closing them)
// Used after successful build when resources are transferred to worker
func (rt *ResourceTracker) Clear() {
	rt.resources = make([]*Resource, 0)
}

// RegisterDirectory registers a directory that was created
func (rt *ResourceTracker) RegisterDirectory(path string) {
	rt.Register(ResourceTypeDirectory, path, path, func() error {
		// Directories are typically not deleted on error, but we could if needed
		return nil
	})
}

// EnsureDirectory creates a directory and registers it for tracking
func (rt *ResourceTracker) EnsureDirectory(path string, perm os.FileMode) error {
	if err := os.MkdirAll(path, perm); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", path, err)
	}
	rt.RegisterDirectory(path)
	return nil
}
