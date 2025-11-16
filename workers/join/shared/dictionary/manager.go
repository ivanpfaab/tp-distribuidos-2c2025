package dictionary

import (
	"fmt"
	"sync"
)

// Manager is a generic dictionary manager that handles client-specific dictionaries
// T is the entity type stored in the dictionary (MenuItem, Store, etc.)
type Manager[T any] struct {
	// dictionaries stores client-specific dictionaries: clientID -> key -> entity
	dictionaries map[string]map[string]T
	// ready tracks whether dictionary is ready for each client
	ready map[string]bool
	// mutex protects concurrent access
	mutex sync.RWMutex
}

// NewManager creates a new dictionary manager
func NewManager[T any]() *Manager[T] {
	return &Manager[T]{
		dictionaries: make(map[string]map[string]T),
		ready:        make(map[string]bool),
	}
}

// GetOrCreateClientDictionary gets or creates a dictionary for a client
func (m *Manager[T]) GetOrCreateClientDictionary(clientID string) map[string]T {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.dictionaries[clientID] == nil {
		m.dictionaries[clientID] = make(map[string]T)
	}
	return m.dictionaries[clientID]
}

// GetClientDictionary gets the dictionary for a client (read-only)
func (m *Manager[T]) GetClientDictionary(clientID string) (map[string]T, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	dict, exists := m.dictionaries[clientID]
	return dict, exists
}

// SetEntity sets an entity in a client's dictionary
func (m *Manager[T]) SetEntity(clientID string, key string, entity T) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.dictionaries[clientID] == nil {
		m.dictionaries[clientID] = make(map[string]T)
	}
	m.dictionaries[clientID][key] = entity
}

// GetEntity gets an entity from a client's dictionary
func (m *Manager[T]) GetEntity(clientID string, key string) (T, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if m.dictionaries[clientID] == nil {
		var zero T
		return zero, false
	}
	entity, exists := m.dictionaries[clientID][key]
	return entity, exists
}

// IsReady checks if dictionary is ready for a client
func (m *Manager[T]) IsReady(clientID string) bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.ready[clientID]
}

// SetReady marks dictionary as ready for a client
func (m *Manager[T]) SetReady(clientID string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.ready[clientID] = true
}

// CleanupClient removes all data for a client
func (m *Manager[T]) CleanupClient(clientID string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.dictionaries, clientID)
	delete(m.ready, clientID)
}

// HasClient checks if a client has dictionary data
func (m *Manager[T]) HasClient(clientID string) bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	_, exists := m.dictionaries[clientID]
	return exists
}

// GetClientCount returns the number of clients with dictionaries
func (m *Manager[T]) GetClientCount() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.dictionaries)
}

// ParseCallback is a function type for parsing CSV data into entities
// Returns: key -> entity mapping
type ParseCallback[T any] func(csvData string, clientID string) (map[string]T, error)

// LoadFromCSV parses CSV data and loads it into the dictionary for a client
func (m *Manager[T]) LoadFromCSV(clientID string, csvData string, parseFunc ParseCallback[T]) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Parse the CSV data
	entities, err := parseFunc(csvData, clientID)
	if err != nil {
		return fmt.Errorf("failed to parse CSV data: %w", err)
	}

	// Initialize client dictionary if needed
	if m.dictionaries[clientID] == nil {
		m.dictionaries[clientID] = make(map[string]T)
	}

	// Merge parsed entities into dictionary
	for key, entity := range entities {
		m.dictionaries[clientID][key] = entity
	}

	return nil
}
