package groupby_blackbox_test

import (
	"encoding/csv"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

const (
	TestClientID       = "TEST"
	TestFileID         = "TI01"
	GroupByDataDir     = "/app/groupby-data"
	VolumeMonitoringMs = 10 // Monitor every 10ms to catch fast cleanup
)

// TestGroupByQuery2 tests the Query 2 group by system end-to-end
func TestGroupByQuery2(t *testing.T) {
	// Connect to RabbitMQ FIRST and declare the queue
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	t.Log("Declaring queue before services start...")
	// Create queue producer to send chunks to the partitioner queue
	producer := workerqueue.NewMessageMiddlewareQueue(queues.Query2GroupByQueue, config)
	if producer == nil {
		t.Fatal("Failed to create queue producer")
	}
	defer producer.Close()

	// Declare the queue to ensure it exists BEFORE partitioners try to consume
	if err := producer.DeclareQueue(false, false, false, false); err != 0 {
		t.Fatalf("Failed to declare queue: %v", err)
	}
	t.Log("Queue declared successfully!")

	// Wait for services to be ready
	t.Log("Waiting for services to be ready...")
	time.Sleep(10 * time.Second)

	// Load test data
	testDataPath := "/app/testdata/transaction_items.csv"
	t.Logf("Loading test data from: %s", testDataPath)

	chunks, err := loadTestDataAsChunks(testDataPath, t)
	if err != nil {
		t.Fatalf("Failed to load test data: %v", err)
	}

	t.Logf("Loaded %d chunks from test data", len(chunks))

	t.Logf("Sending %d chunks to queue: %s", len(chunks), queues.Query2GroupByQueue)

	// Send all chunks to the queue
	for i, chunkData := range chunks {
		chunkMsg := chunk.NewChunkMessage(chunkData)
		serialized, err := chunk.SerializeChunkMessage(chunkMsg)
		if err != nil {
			t.Fatalf("Failed to serialize chunk %d: %v", i, err)
		}

		if err := producer.Send(serialized); err != 0 {
			t.Fatalf("Failed to send chunk %d: %v", i, err)
		}

		t.Logf("Sent chunk %d (size: %d bytes, records: ~%d)", i+1, len(serialized), chunkData.ChunkSize)
	}

	t.Log("All chunks sent successfully!")

	// Measure initial volume size
	initialSize, err := getDirectorySize(GroupByDataDir)
	if err != nil {
		t.Logf("Warning: Failed to get initial directory size: %v", err)
		initialSize = 0
	}
	t.Logf("Initial volume size: %s (%d bytes)", formatBytes(initialSize), initialSize)

	// Wait for workers to process and create files
	t.Log("Waiting for workers to process chunks (20 seconds)...")
	time.Sleep(20 * time.Second)

	// Measure volume size during processing
	midSize, err := getDirectorySize(GroupByDataDir)
	if err != nil {
		t.Logf("Warning: Failed to get mid directory size: %v", err)
		midSize = 0
	}
	t.Logf("Mid-processing volume size: %s (%d bytes)", formatBytes(midSize), midSize)

	// Wait for orchestrator to complete and cleanup
	t.Log("Waiting for orchestrator to complete cleanup (20 seconds)...")
	time.Sleep(20 * time.Second)

	// Check orchestrator logs for cleanup completion
	t.Log("Checking orchestrator logs for cleanup evidence...")
	cleanupEvents := parseOrchestratorLogs(t)

	// Measure final volume size
	finalSize, err := getDirectorySize(GroupByDataDir)
	if err != nil {
		t.Logf("Warning: Failed to get final directory size: %v", err)
		finalSize = 0
	}
	t.Logf("Final volume size: %s (%d bytes)", formatBytes(finalSize), finalSize)

	// Report results
	t.Logf("\n=== CLEANUP VERIFICATION RESULTS ===")
	t.Logf("\n[Volume Size Progression]")
	t.Logf("  Initial:        %s (%d bytes)", formatBytes(initialSize), initialSize)
	t.Logf("  Mid-processing: %s (%d bytes)", formatBytes(midSize), midSize)
	t.Logf("  Final:          %s (%d bytes)", formatBytes(finalSize), finalSize)

	t.Logf("\n[Orchestrator Cleanup Activity]")
	t.Logf("  Cleanup operations: %d", cleanupEvents.CleanupCount)
	t.Logf("  Files deleted:      %d", cleanupEvents.FilesDeleted)
	t.Logf("  Directories deleted: %d", cleanupEvents.DirsDeleted)

	// Verify cleanup
	t.Logf("\n[Verification]")
	testPassed := true

	// Check 1: Files were created
	if midSize > 0 {
		t.Logf("  ✓ Files were created during processing (%s)", formatBytes(midSize))
	} else {
		t.Logf("  ⚠ No files observed during processing (might be too fast)")
	}

	// Check 2: Cleanup happened
	if cleanupEvents.CleanupCount > 0 && cleanupEvents.FilesDeleted > 0 {
		t.Logf("  ✓ Orchestrator performed cleanup (%d files deleted)", cleanupEvents.FilesDeleted)
	} else {
		t.Logf("  ✗ No cleanup detected in orchestrator logs")
		testPassed = false
	}

	// Check 3: Volume is clean
	if finalSize == 0 {
		t.Logf("  ✓ Volume is completely clean (0 bytes)")
	} else if finalSize < 1024*1024 {
		t.Logf("  ✓ Volume is reasonably clean (%s remaining)", formatBytes(finalSize))
	} else {
		t.Logf("  ✗ Volume still has significant data (%s)", formatBytes(finalSize))
		testPassed = false
	}

	// Check 4: Size decreased
	if finalSize < midSize {
		reduction := float64(midSize-finalSize) / float64(midSize) * 100
		t.Logf("  ✓ Volume size decreased by %.1f%%", reduction)
	} else if midSize == 0 && cleanupEvents.FilesDeleted > 0 {
		t.Logf("  ✓ Cleanup confirmed via logs (files too transient to measure)")
	} else {
		t.Logf("  ⚠ Volume size did not decrease")
	}

	// Final verdict
	t.Logf("\n[Final Verdict]")
	if testPassed {
		t.Logf("✓ TEST PASSED: File cleanup is working correctly!")
	} else {
		t.Logf("✗ TEST FAILED: Cleanup issues detected")
		t.FailNow()
	}

	t.Log("\nTest completed!")
}

// loadTestDataAsChunks reads a CSV file and converts it into chunks
func loadTestDataAsChunks(filePath string, t *testing.T) ([]*chunk.Chunk, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open test data file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(records) < 2 {
		return nil, fmt.Errorf("CSV file has insufficient data")
	}

	// First row is header
	header := records[0]
	dataRecords := records[1:]

	t.Logf("CSV has %d data records with schema: %v", len(dataRecords), header)

	// Split data into chunks (e.g., 10 records per chunk)
	chunkSize := 10
	var chunks []*chunk.Chunk

	for i := 0; i < len(dataRecords); i += chunkSize {
		end := i + chunkSize
		if end > len(dataRecords) {
			end = len(dataRecords)
		}

		chunkRecords := dataRecords[i:end]
		chunkNumber := (i / chunkSize) + 1
		isLastChunk := end >= len(dataRecords)

		// Build CSV data for this chunk (with header)
		var csvBuilder strings.Builder
		csvBuilder.WriteString(strings.Join(header, ","))
		csvBuilder.WriteString("\n")

		for _, record := range chunkRecords {
			csvBuilder.WriteString(strings.Join(record, ","))
			csvBuilder.WriteString("\n")
		}

		chunkData := csvBuilder.String()

		// Create chunk
		chunkObj := chunk.NewChunk(
			TestClientID,
			TestFileID,
			2, // QueryType 2
			chunkNumber,
			isLastChunk,
			isLastChunk, // isLastFromTable - for test purposes, same as isLastChunk
			1,           // step
			len(chunkData),
			1, // tableID
			chunkData,
		)

		chunks = append(chunks, chunkObj)
	}

	return chunks, nil
}

// CleanupEvents holds parsed orchestrator cleanup information
type CleanupEvents struct {
	CleanupCount int
	FilesDeleted int
	DirsDeleted  int
}

// parseOrchestratorLogs parses orchestrator container logs to find cleanup events
func parseOrchestratorLogs(t *testing.T) CleanupEvents {
	cmd := exec.Command("docker", "logs", "groupby-orchestrator-test")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("Warning: Failed to get orchestrator logs: %v", err)
		return CleanupEvents{}
	}

	logs := string(output)
	lines := strings.Split(logs, "\n")

	events := CleanupEvents{}

	for _, line := range lines {
		// Look for cleanup messages
		if strings.Contains(line, "All") && strings.Contains(line, "chunks sent, now cleaning up files") {
			events.CleanupCount++
		}
		if strings.Contains(line, "Deleted file:") {
			events.FilesDeleted++
		}
		if strings.Contains(line, "Deleted client directory:") {
			events.DirsDeleted++
		}
	}

	return events
}

// getDirectorySize calculates the total size of a directory and its contents
func getDirectorySize(path string) (int64, error) {
	var totalSize int64

	err := filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			// If the directory doesn't exist yet, return 0
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})

	return totalSize, err
}

// getDockerVolumeSize gets the size of a docker volume using docker system df
func getDockerVolumeSize(volumeName string) (int64, error) {
	cmd := exec.Command("docker", "system", "df", "-v", "--format", "{{.Name}}\t{{.Size}}")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("failed to run docker command: %v, output: %s", err, string(output))
	}

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.Contains(line, volumeName) {
			parts := strings.Split(line, "\t")
			if len(parts) >= 2 {
				sizeStr := strings.TrimSpace(parts[1])
				// Parse size (could be in various formats like "10.5MB", "1.2GB", etc.)
				return parseDockerSize(sizeStr)
			}
		}
	}

	return 0, fmt.Errorf("volume %s not found in docker system df output", volumeName)
}

// parseDockerSize parses Docker size strings like "10.5MB", "1.2GB" to bytes
func parseDockerSize(sizeStr string) (int64, error) {
	sizeStr = strings.TrimSpace(sizeStr)
	if sizeStr == "" || sizeStr == "0B" {
		return 0, nil
	}

	// Extract numeric part and unit
	var numStr string
	var unit string

	for i, char := range sizeStr {
		if char >= '0' && char <= '9' || char == '.' {
			numStr += string(char)
		} else {
			unit = sizeStr[i:]
			break
		}
	}

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse size number: %v", err)
	}

	// Convert to bytes based on unit
	multiplier := int64(1)
	switch strings.ToUpper(unit) {
	case "B":
		multiplier = 1
	case "KB":
		multiplier = 1024
	case "MB":
		multiplier = 1024 * 1024
	case "GB":
		multiplier = 1024 * 1024 * 1024
	case "TB":
		multiplier = 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown size unit: %s", unit)
	}

	return int64(num * float64(multiplier)), nil
}

// formatBytes formats a byte count into a human-readable string
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	units := []string{"KB", "MB", "GB", "TB", "PB"}
	return fmt.Sprintf("%.2f %s", float64(bytes)/float64(div), units[exp])
}
