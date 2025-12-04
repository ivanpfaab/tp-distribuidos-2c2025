package dictionary

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	// dictDir is the directory where dictionary files are stored (for cleanup)
	dictDir string
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

// CleanupClient removes all data for a client (in-memory only)
func (m *Manager[T]) CleanupClient(clientID string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.dictionaries, clientID)
	delete(m.ready, clientID)
}

// SetDictDir sets the directory where dictionary files are stored
func (m *Manager[T]) SetDictDir(dictDir string) {
	m.dictDir = dictDir
}

// CleanClient removes all data for a client (memory + disk)
// Implements the CleanupHandler interface for CompletionCleaner
func (m *Manager[T]) CleanClient(clientID string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Clean in-memory data
	delete(m.dictionaries, clientID)
	delete(m.ready, clientID)

	// Clean dictionary file from disk if dictDir is set
	if m.dictDir != "" {
		filePath := filepath.Join(m.dictDir, fmt.Sprintf("%s.csv", clientID))
		if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to delete dictionary file: %w", err)
		}
	}

	return nil
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

// SaveDictionaryChunkToFile appends dictionary chunk CSV data directly to file
func (m *Manager[T]) SaveDictionaryChunkToFile(clientID string, filePath string, csvData string) error {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Check if file exists
	fileExists := false
	if _, err := os.Stat(filePath); err == nil {
		fileExists = true
	}

	// Open file in append mode
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// If file is new, write the CSV data as-is (includes header)
	// If file exists, skip header and write only data rows
	if fileExists {
		// Parse CSV to skip header
		reader := csv.NewReader(strings.NewReader(csvData))
		records, err := reader.ReadAll()
		if err != nil {
			return fmt.Errorf("failed to parse CSV: %w", err)
		}

		// Skip header row (first row)
		if len(records) > 1 {
			writer := csv.NewWriter(file)
			for _, record := range records[1:] {
				if err := writer.Write(record); err != nil {
					return fmt.Errorf("failed to write row: %w", err)
				}
			}
			// Flush writer buffer to file and check for errors
			writer.Flush()
			if err := writer.Error(); err != nil {
				return fmt.Errorf("failed to flush writer: %w", err)
			}
		}
	} else {
		// New file - write CSV data as-is (includes header)
		if _, err := file.WriteString(csvData); err != nil {
			return fmt.Errorf("failed to write CSV data: %w", err)
		}
	}

	// Sync to ensure data is written to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// RebuildDictionaryFromFile rebuilds dictionary from a CSV file
func (m *Manager[T]) RebuildDictionaryFromFile(filePath string, parseFunc ParseCallback[T]) (string, error) {
	// Extract clientID from filename (format: {clientID}.csv)
	filename := filepath.Base(filePath)
	clientID := strings.TrimSuffix(filename, ".csv")

	// Read file
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Read all content
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return "", fmt.Errorf("failed to read CSV: %w", err)
	}

	// Convert records back to CSV string
	var content strings.Builder
	for i, record := range records {
		if i > 0 {
			content.WriteString("\n")
		}
		content.WriteString(strings.Join(record, ","))
	}

	// Load into dictionary
	if err := m.LoadFromCSV(clientID, content.String(), parseFunc); err != nil {
		return "", fmt.Errorf("failed to load dictionary: %w", err)
	}

	return clientID, nil
}

// RebuildAllDictionaries scans directory and rebuilds all dictionaries
func (m *Manager[T]) RebuildAllDictionaries(dictDir string, parseFunc ParseCallback[T]) (int, error) {
	// Ensure directory exists
	if err := os.MkdirAll(dictDir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create directory: %w", err)
	}

	// Read directory
	entries, err := os.ReadDir(dictDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // Directory doesn't exist, nothing to rebuild
		}
		return 0, fmt.Errorf("failed to read directory: %w", err)
	}

	rebuiltCount := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".csv") {
			continue
		}

		filePath := filepath.Join(dictDir, entry.Name())
		clientID, err := m.RebuildDictionaryFromFile(filePath, parseFunc)
		if err != nil {
			// Log error but continue with other files
			continue
		}

		// Mark as ready (file exists = dictionary ready)
		m.SetReady(clientID)
		rebuiltCount++
	}

	return rebuiltCount, nil
}
