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
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
)

const (
	TestClientID       = "TEST"
	TestFileID         = "TI01"
	GroupByDataDir     = "/app/groupby-data"
	QueryType          = 2
	VolumeMonitoringMs = 10 // Monitor every 10ms to catch fast cleanup
)

// getClientDirectory returns the directory path for a specific client
func getClientDirectory(clientID string) string {
	return fmt.Sprintf("%s/q%d/%s", GroupByDataDir, QueryType, clientID)
}

// TestSingleClientSingleFileGroupBy tests the Query 2 group by system with a single client and single file
func TestSingleClientSingleFileGroupBy(t *testing.T) {
	testing_utils.LogTest("=== Single Client Single File GroupBy Test ===")

	// Connect to RabbitMQ FIRST and declare the queue
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	testing_utils.LogQuiet("Declaring queue before services start...")
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

	testing_utils.LogQuiet("Waiting for services to be ready...")
	time.Sleep(10 * time.Second)

	// Load test data
	testDataPath := "/app/testdata/transaction_items.csv"

	chunks, err := loadTestDataAsChunks(testDataPath, t)
	if err != nil {
		t.Fatalf("Failed to load test data: %v", err)
	}

	testing_utils.LogQuiet("Test", "Loaded %d chunks from test data", len(chunks))

	// Measure initial client directory size BEFORE sending any data
	clientDir := getClientDirectory(TestClientID)
	initialSize, err := getDirectorySize(clientDir)
	if err != nil {
		testing_utils.LogWarn("Test", "Failed to get initial directory size: %v", err)
		initialSize = 0
	}
	testing_utils.LogInfo("Test", "Initial client directory size: %s", formatBytes(initialSize))

	testing_utils.LogStep("Sending %d chunks to queue: %s", len(chunks), queues.Query2GroupByQueue)

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
	}

	testing_utils.LogInfo("All %d chunks sent successfully", len(chunks))

	// Wait for workers to process and create files (actively monitor orchestrator logs)
	testing_utils.LogStep("Waiting for workers to process chunks and orchestrator to detect files...")
	midSizeAtNotification, midSizeWhenReady, err := waitForFilesReadyAndMeasure(t, TestClientID, 60*time.Second)
	if err != nil {
		t.Fatalf("Failed to measure mid-processing size: %v", err)
	}

	// Wait for orchestrator to complete cleanup
	testing_utils.LogStep("Waiting for orchestrator to complete cleanup (20 seconds)...")
	time.Sleep(20 * time.Second)

	// Check orchestrator logs for cleanup completion
	testing_utils.LogStep("Checking orchestrator logs for cleanup evidence...")
	cleanupEvents := parseOrchestratorLogs(t)

	// Measure final client directory size
	finalSize, err := getDirectorySize(clientDir)
	if err != nil {
		testing_utils.LogWarn("Test", "Failed to get final directory size: %v", err)
		finalSize = 0
	}

	// Report results
	testing_utils.LogInfo("\n=== CLEANUP VERIFICATION RESULTS ===")
	testing_utils.LogStep("\n[Volume Size Progression]\n")
	testing_utils.LogStep("  Initial:                      %s (%d bytes)\n", formatBytes(initialSize), initialSize)
	testing_utils.LogStep("  Mid (at first notification):  %s (%d bytes)\n", formatBytes(midSizeAtNotification), midSizeAtNotification)
	testing_utils.LogStep("  Mid (when files ready):       %s (%d bytes)\n", formatBytes(midSizeWhenReady), midSizeWhenReady)
	testing_utils.LogStep("  Final:                        %s (%d bytes)\n", formatBytes(finalSize), finalSize)

	testing_utils.LogStep("\n[Orchestrator Cleanup Activity]\n")
	testing_utils.LogStep("  Cleanup operations:   %d\n", cleanupEvents.CleanupCount)
	testing_utils.LogStep("  Files deleted:        %d\n", cleanupEvents.FilesDeleted)
	testing_utils.LogStep("  Directories deleted:  %d\n", cleanupEvents.DirsDeleted)

	// Verify cleanup
	testing_utils.LogStep("\n[Verification]\n")
	testPassed := true

	// Check 2: Cleanup happened
	if cleanupEvents.CleanupCount > 0 && cleanupEvents.FilesDeleted > 0 {
		testing_utils.LogSuccess("  ✓ Orchestrator performed cleanup (%d files deleted)\n", cleanupEvents.FilesDeleted)
	} else {
		testing_utils.LogFailure("  ✗ No cleanup detected in orchestrator logs\n")
		testPassed = false
	}

	// Check 3: Volume is clean
	if finalSize == 0 {
		testing_utils.LogSuccess("  ✓ Volume is completely clean (0 bytes)\n")
	} else if finalSize < 1024*1024 {
		testing_utils.LogWarn("  ⚠ Volume is reasonably clean (%s remaining)\n", formatBytes(finalSize))
	} else {
		testing_utils.LogFailure("  ✗ Volume still has significant data (%s)\n", formatBytes(finalSize))
		testPassed = false
	}

	// Check 4: Size decreased
	if finalSize < midSizeWhenReady {
		reduction := float64(midSizeWhenReady-finalSize) / float64(midSizeWhenReady) * 100
		testing_utils.LogSuccess("  ✓ Volume size decreased by %.1f%%\n", reduction)
	} else if midSizeWhenReady == 0 && cleanupEvents.FilesDeleted > 0 {
		testing_utils.LogSuccess("  ✓ Cleanup confirmed via logs (files too transient to measure)\n")
	} else {
		testing_utils.LogWarn("  ⚠ Volume size did not decrease\n")
	}

	// Final verdict
	fmt.Printf("\n[Final Verdict]\n")
	if testPassed {
		testing_utils.LogSuccess("TEST PASSED: File cleanup is working correctly!")
	} else {
		testing_utils.LogFailure("TEST FAILED: Cleanup issues detected")
		t.FailNow()
	}
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


// waitForFilesReadyAndMeasure waits for the orchestrator to indicate files are ready, then measures directory size
// Measures at two points: (1) first chunk notification, (2) when files are ready
// Returns both sizes: (sizeAtFirstNotification, sizeWhenReady, error)
func waitForFilesReadyAndMeasure(t *testing.T, clientID string, timeout time.Duration) (int64, int64, error) {
	clientDir := getClientDirectory(clientID)
	startTime := time.Now()
	firstNotificationSeen := false
	var sizeAtFirstNotification int64

	for {
		if time.Since(startTime) > timeout {
			return 0, 0, fmt.Errorf("timeout waiting for files to be ready")
		}

		// Check orchestrator logs
		cmd := exec.Command("docker", "logs", "groupby-orchestrator-test")
		output, err := cmd.CombinedOutput()
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		logs := string(output)

		// First, look for the first chunk notification (workers starting to write)
		if !firstNotificationSeen && strings.Contains(logs, "Received chunk notification:") {
			firstNotificationSeen = true

			// Measure size at first notification
			size, err := getDirectorySize(clientDir)
			if err != nil {
				t.Logf("First chunk notification received, but failed to measure size: %v", err)
			} else {
				sizeAtFirstNotification = size
				t.Logf("First chunk notification received by orchestrator, directory size: %s", formatBytes(size))
			}
		}

		// Then look for "Found X partition files to process" for our client (workers finished)
		searchPattern := fmt.Sprintf("Client %s: Found", clientID)
		if strings.Contains(logs, searchPattern) {
			// Files are ready! Measure now (this is the final mid-processing size we return)
			size, err := getDirectorySize(clientDir)
			if err != nil {
				return 0, 0, fmt.Errorf("failed to measure directory size: %v", err)
			}
			t.Logf("Files detected for client %s (workers finished writing), measured size: %s", clientID, formatBytes(size))
			return sizeAtFirstNotification, size, nil
		}

		// Sleep before checking again
		time.Sleep(500 * time.Millisecond)
	}
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

	// Log orchestrator output for visibility
	t.Logf("\n=== ORCHESTRATOR LOGS ===")
	for _, line := range lines {
		// Filter to only show important lines to avoid spam
		if strings.Contains(line, "completed") ||
			strings.Contains(line, "cleaning up") ||
			strings.Contains(line, "cleaned up") {
			t.Logf("  %s", line)
		}

		// Count cleanup events
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
	t.Logf("\n=== END ORCHESTRATOR LOGS ===\n")

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

// TestSingleClientMultipleFilesGroupBy tests cleanup with multiple files from same client
func TestSingleClientMultipleFilesGroupBy(t *testing.T) {
	// Connect to RabbitMQ FIRST and declare the queue
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	t.Log("Declaring queue before services start...")
	producer := workerqueue.NewMessageMiddlewareQueue(queues.Query2GroupByQueue, config)
	if producer == nil {
		t.Fatal("Failed to create queue producer")
	}
	defer producer.Close()

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

	allChunks, err := loadTestDataAsChunks(testDataPath, t)
	if err != nil {
		t.Fatalf("Failed to load test data: %v", err)
	}

	// Split chunks into 3 "files" for the same client
	numFiles := 3
	chunksPerFile := len(allChunks) / numFiles

	t.Logf("Simulating %d files with ~%d chunks each", numFiles, chunksPerFile)

	// Measure initial client directory size BEFORE sending any data
	clientDir := getClientDirectory(TestClientID)
	initialSize, _ := getDirectorySize(clientDir)
	t.Logf("Initial client directory size: %s", formatBytes(initialSize))

	// Send chunks for each file
	for fileNum := 0; fileNum < numFiles; fileNum++ {
		startIdx := fileNum * chunksPerFile
		endIdx := startIdx + chunksPerFile
		if fileNum == numFiles-1 {
			endIdx = len(allChunks) // Last file gets remaining chunks
		}

		fileID := fmt.Sprintf("TI%02d", fileNum+1)
		fileChunks := allChunks[startIdx:endIdx]

		t.Logf("Sending file %s with %d chunks", fileID, len(fileChunks))

		for i, chunkData := range fileChunks {
			// Update chunk metadata for this file
			chunkData.FileID = fileID
			chunkData.ChunkNumber = i + 1
			chunkData.IsLastChunk = (i == len(fileChunks)-1)
			chunkData.IsLastFromTable = (fileNum == numFiles-1) && (i == len(fileChunks)-1)

			chunkMsg := chunk.NewChunkMessage(chunkData)
			serialized, err := chunk.SerializeChunkMessage(chunkMsg)
			if err != nil {
				t.Fatalf("Failed to serialize chunk: %v", err)
			}

			if err := producer.Send(serialized); err != 0 {
				t.Fatalf("Failed to send chunk: %v", err)
			}
		}
	}

	t.Logf("All %d files sent successfully!", numFiles)

	// Wait for workers to process and create files (actively monitor orchestrator logs)
	t.Log("Waiting for workers to process chunks and orchestrator to detect files...")
	midSizeAtNotification, midSizeWhenReady, err := waitForFilesReadyAndMeasure(t, TestClientID, 60*time.Second)
	if err != nil {
		t.Fatalf("Failed to measure mid-processing size: %v", err)
	}

	// Wait for cleanup
	t.Log("Waiting for orchestrator to complete cleanup (20 seconds)...")
	time.Sleep(20 * time.Second)

	cleanupEvents := parseOrchestratorLogs(t)
	finalSize, _ := getDirectorySize(clientDir)

	// Report results
	t.Logf("\n=== MULTIPLE FILES CLEANUP TEST ===")
	t.Logf("[Volume Size Progression]")
	t.Logf("  Initial:                      %s", formatBytes(initialSize))
	t.Logf("  Mid (at first notification):  %s", formatBytes(midSizeAtNotification))
	t.Logf("  Mid (when files ready):       %s", formatBytes(midSizeWhenReady))
	t.Logf("  Final:                        %s", formatBytes(finalSize))
	t.Logf("[Cleanup] Operations: %d, Files deleted: %d, Dirs deleted: %d",
		cleanupEvents.CleanupCount, cleanupEvents.FilesDeleted, cleanupEvents.DirsDeleted)

	// Verify
	if cleanupEvents.CleanupCount >= 1 && cleanupEvents.FilesDeleted > 0 && finalSize == 0 {
		t.Logf("✓ TEST PASSED: Multiple files cleaned up correctly")
	} else {
		t.Logf("✗ TEST FAILED: Expected at least %d cleanup operations and 0 final size", 1)
		t.FailNow()
	}
}

// TestMultipleClientsMultipleFilesGroupBy tests cleanup with multiple clients, each with multiple files
func TestMultipleClientsMultipleFilesGroupBy(t *testing.T) {
	// Connect to RabbitMQ
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	t.Log("Declaring queue...")
	producer := workerqueue.NewMessageMiddlewareQueue(queues.Query2GroupByQueue, config)
	if producer == nil {
		t.Fatal("Failed to create queue producer")
	}
	defer producer.Close()

	if err := producer.DeclareQueue(false, false, false, false); err != 0 {
		t.Fatalf("Failed to declare queue: %v", err)
	}
	t.Log("Queue declared successfully!")

	t.Log("Waiting for services...")
	time.Sleep(10 * time.Second)

	// Load test data
	testDataPath := "/app/testdata/transaction_items.csv"
	allChunks, err := loadTestDataAsChunks(testDataPath, t)
	if err != nil {
		t.Fatalf("Failed to load test data: %v", err)
	}

	// Simulate 2 clients, each with 2 files
	clients := []string{"CLI1", "CLI2"}
	filesPerClient := 2
	chunksPerFile := len(allChunks) / (len(clients) * filesPerClient)

	t.Logf("Simulating %d clients with %d files each", len(clients), filesPerClient)

	// Measure initial sizes for each client BEFORE sending any data
	var initialSize, midSizeAtNotification, midSizeWhenReady, finalSize int64
	for _, clientID := range clients {
		clientDir := getClientDirectory(clientID)
		size, _ := getDirectorySize(clientDir)
		initialSize += size
		t.Logf("Initial size for client %s: %s", clientID, formatBytes(size))
	}
	t.Logf("Total initial size: %s", formatBytes(initialSize))

	chunkIdx := 0
	for _, clientID := range clients {
		for fileNum := 0; fileNum < filesPerClient; fileNum++ {
			startIdx := chunkIdx
			endIdx := chunkIdx + chunksPerFile
			if endIdx > len(allChunks) {
				endIdx = len(allChunks)
			}

			fileID := fmt.Sprintf("TI%02d", fileNum+1)
			fileChunks := allChunks[startIdx:endIdx]

			t.Logf("Sending client %s, file %s with %d chunks", clientID, fileID, len(fileChunks))

			for i, chunkData := range fileChunks {
				chunkData.ClientID = clientID
				chunkData.FileID = fileID
				chunkData.ChunkNumber = i + 1
				chunkData.IsLastChunk = (i == len(fileChunks)-1)
				// Only last chunk of last file is LastFromTable
				chunkData.IsLastFromTable = (i == len(fileChunks)-1) && (fileNum+1 == filesPerClient)

				chunkMsg := chunk.NewChunkMessage(chunkData)
				serialized, err := chunk.SerializeChunkMessage(chunkMsg)
				if err != nil {
					t.Fatalf("Failed to serialize chunk: %v", err)
				}

				if err := producer.Send(serialized); err != 0 {
					t.Fatalf("Failed to send chunk: %v", err)
				}
			}

			chunkIdx = endIdx
		}
	}

	t.Log("All clients and files sent!")

	// Wait for workers to process and create files for each client (actively monitor)
	t.Log("Waiting for workers to process chunks and orchestrator to detect files for all clients...")
	for _, clientID := range clients {
		sizeAtNotif, sizeWhenReady, err := waitForFilesReadyAndMeasure(t, clientID, 60*time.Second)
		if err != nil {
			t.Fatalf("Failed to measure mid-processing size for client %s: %v", clientID, err)
		}
		midSizeAtNotification += sizeAtNotif
		midSizeWhenReady += sizeWhenReady
		t.Logf("Mid-processing sizes for client %s - At notification: %s, When ready: %s",
			clientID, formatBytes(sizeAtNotif), formatBytes(sizeWhenReady))
	}
	t.Logf("Total mid-processing size (at notification): %s", formatBytes(midSizeAtNotification))
	t.Logf("Total mid-processing size (when ready): %s", formatBytes(midSizeWhenReady))

	t.Log("Waiting for cleanup (30 seconds)...")
	time.Sleep(30 * time.Second)

	cleanupEvents := parseOrchestratorLogs(t)

	// Measure final sizes
	for _, clientID := range clients {
		clientDir := getClientDirectory(clientID)
		size, _ := getDirectorySize(clientDir)
		finalSize += size
		t.Logf("Final size for client %s: %s", clientID, formatBytes(size))
	}
	t.Logf("Total final size: %s", formatBytes(finalSize))

	// Report
	t.Logf("\n=== MULTIPLE CLIENTS CLEANUP TEST ===")
	t.Logf("[Client Directory Sizes]")
	t.Logf("  Initial:                      %s", formatBytes(initialSize))
	t.Logf("  Mid (at first notification):  %s", formatBytes(midSizeAtNotification))
	t.Logf("  Mid (when files ready):       %s", formatBytes(midSizeWhenReady))
	t.Logf("  Final:                        %s", formatBytes(finalSize))
	t.Logf("[Cleanup] Operations: %d, Files deleted: %d, Dirs deleted: %d",
		cleanupEvents.CleanupCount, cleanupEvents.FilesDeleted, cleanupEvents.DirsDeleted)

	expectedCleanupOps := len(clients)

	// Verify
	if cleanupEvents.CleanupCount >= expectedCleanupOps &&
	   cleanupEvents.DirsDeleted >= len(clients) &&
	   finalSize == 0 {
		t.Logf("✓ TEST PASSED: Multiple clients cleaned up correctly")
	} else {
		t.Logf("✗ TEST FAILED: Expected at least %d cleanup ops, %d dirs deleted, and 0 final size (got %s)",
			expectedCleanupOps, len(clients), formatBytes(finalSize))
		t.FailNow()
	}
}
