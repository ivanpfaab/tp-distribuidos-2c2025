package file

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// PartitionManager handles file-based partition operations
type PartitionManager struct {
	sharedDataDir string
	numPartitions int
}

// NewPartitionManager creates a new partition manager
func NewPartitionManager(sharedDataDir string, numPartitions int) *PartitionManager {
	return &PartitionManager{
		sharedDataDir: sharedDataDir,
		numPartitions: numPartitions,
	}
}

// GetPartition returns the partition number for a given ID
func (pm *PartitionManager) GetPartition(id string) (int, error) {
	// Parse ID (handle both int and float formats)
	idFloat, err := parseIDToFloat(id)
	if err != nil {
		return 0, fmt.Errorf("invalid ID %s: %w", id, err)
	}
	idInt := int(idFloat)

	// Using mod to determine partition
	return idInt % pm.numPartitions, nil
}

// GetPartitionPath returns the full path to a partition file
func (pm *PartitionManager) GetPartitionPath(clientID string, partition int) string {
	filename := fmt.Sprintf("%s-users-partition-%03d.csv", clientID, partition)
	return filepath.Join(pm.sharedDataDir, filename)
}

// LookupEntity looks up an entity from a partition file
type EntityParser func(record []string) (map[string]string, error)

// LookupEntity looks up an entity from a partition file
func (pm *PartitionManager) LookupEntity(clientID string, entityID string, parser EntityParser) (map[string]string, error) {
	// Normalize entity ID (remove decimal point if present)
	normalizedID := strings.TrimSuffix(entityID, ".0")

	// Get partition
	partition, err := pm.GetPartition(normalizedID)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition for entity %s: %w", normalizedID, err)
	}

	// Open partition file
	filePath := pm.GetPartitionPath(clientID, partition)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("partition file not ready: %s", filePath)
		}
		return nil, fmt.Errorf("failed to open partition file %s: %w", filePath, err)
	}
	defer file.Close()

	// Read and search for entity
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV from file %s: %w", filePath, err)
	}

	// Search for the specific entity ID (skip header)
	for _, record := range records {
		if len(record) == 0 {
			continue
		}

		// Skip header
		if strings.Contains(strings.ToLower(record[0]), "user_id") {
			continue
		}

		// Check if this is the entity we're looking for
		if len(record) > 0 && record[0] == normalizedID {
			return parser(record)
		}
	}

	// Entity not found
	return nil, fmt.Errorf("entity %s not found in partition %d", normalizedID, partition)
}

// CleanupClientFiles deletes all partition files for a client
// Called by readers after processing completes (readers share volume with their paired writer)
func (pm *PartitionManager) CleanupClientFiles(clientID string) error {
	pattern := filepath.Join(pm.sharedDataDir, fmt.Sprintf("%s-users-partition-*.csv", clientID))
	
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("error finding files for client %s: %w", clientID, err)
	}

	var deleteErrors []error
	deletedCount := 0
	
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			// Ignore "file not found" (already deleted)
			if !os.IsNotExist(err) {
				deleteErrors = append(deleteErrors, fmt.Errorf("error deleting file %s: %w", file, err))
			}
		} else {
			deletedCount++
		}
	}

	if deletedCount > 0 {
		fmt.Printf("Cleanup: Successfully deleted %d partition files for client %s\n", deletedCount, clientID)
	}

	if len(deleteErrors) > 0 {
		return fmt.Errorf("errors cleaning up files: %v", deleteErrors)
	}

	return nil
}

// parseIDToFloat parses an ID string to float64
func parseIDToFloat(id string) (float64, error) {
	id = strings.TrimSpace(id)
	id = strings.TrimSuffix(id, ".0")

	var result float64
	_, err := fmt.Sscanf(id, "%f", &result)
	if err != nil {
		return 0, fmt.Errorf("invalid ID format: %w", err)
	}
	return result, nil
}
