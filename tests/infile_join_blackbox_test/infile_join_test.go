package infile_join_blackbox_test

import (
	"encoding/csv"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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
	TestFileID    = "US01"
	SharedDataDir = "/shared-data"
	NumPartitions = 100 // Must match the worker configuration
)

// TestMultipleClientsInFileJoin tests the in-file join pipeline with 2 clients
func TestMultipleClientsInFileJoin(t *testing.T) {
	testing_utils.LogTest("=== Multiple Clients In-File Join Test ===")

	// Connect to RabbitMQ
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	testing_utils.LogQuiet("Declaring queues before services start...")
	// Create queue producer to send user chunks to the partitioner queue
	userProducer := workerqueue.NewMessageMiddlewareQueue(queues.JoinUserIdDictionaryQueue, config)
	if userProducer == nil {
		t.Fatal("Failed to create user queue producer")
	}
	defer userProducer.Close()

	// Create queue producer to send join chunks to the join worker queue
	joinProducer := workerqueue.NewMessageMiddlewareQueue(queues.UserIdChunkQueue, config)
	if joinProducer == nil {
		t.Fatal("Failed to create join queue producer")
	}
	defer joinProducer.Close()

	// Declare both queues
	if err := userProducer.DeclareQueue(false, false, false, false); err != 0 {
		t.Fatalf("Failed to declare user queue: %v", err)
	}
	if err := joinProducer.DeclareQueue(false, false, false, false); err != 0 {
		t.Fatalf("Failed to declare join queue: %v", err)
	}

	testing_utils.LogQuiet("Waiting for services to be ready...")
	time.Sleep(10 * time.Second)

	// Test with 2 clients
	clients := []string{"CLI1", "CLI2"}

	// Generate test data
	userTestDataPath := "/app/testdata/users_test.csv"
	joinTestDataPath := "/app/testdata/join_test.csv"
	testing_utils.LogQuiet("Generating test data...")

	userIDs, err := generateUserTestData(userTestDataPath, 1000)
	if err != nil {
		t.Fatalf("Failed to generate user test data: %v", err)
	}

	if err := generateJoinTestData(joinTestDataPath, userIDs); err != nil {
		t.Fatalf("Failed to generate join test data: %v", err)
	}

	// Measure initial sizes for each client BEFORE sending any data
	var initialSize, midSizeAtNotification, midSizeWhenReady, finalSize int64
	for _, clientID := range clients {
		size, _ := getClientFilesSize(clientID)
		initialSize += size
		testing_utils.LogQuiet("Initial size for client %s: %s", clientID, formatBytes(size))
	}
	testing_utils.LogInfo("Test", "Total initial size: %s", formatBytes(initialSize))

	// Load and send user data for each client
	for _, clientID := range clients {
		testing_utils.LogStep("Processing user data for client: %s", clientID)

		userChunks, err := loadTestDataAsChunks(userTestDataPath, clientID, t)
		if err != nil {
			t.Fatalf("Failed to load user test data for client %s: %v", clientID, err)
		}

		testing_utils.LogInfo("Test", "Loaded %d user chunks for client %s", len(userChunks), clientID)

		// Send all user chunks to the partitioner queue
		for i, chunkData := range userChunks {
			chunkMsg := chunk.NewChunkMessage(chunkData)
			serialized, err := chunk.SerializeChunkMessage(chunkMsg)
			if err != nil {
				t.Fatalf("Failed to serialize user chunk %d for client %s: %v", i, clientID, err)
			}

			if err := userProducer.Send(serialized); err != 0 {
				t.Fatalf("Failed to send user chunk %d for client %s: %v", i, clientID, err)
			}
		}

		testing_utils.LogInfo("Test", "Sent %d user chunks for client %s", len(userChunks), clientID)
	}

	testing_utils.LogSuccess("All user chunks sent for all clients")

	// Wait for workers to process and create files for each client (actively monitor)
	testing_utils.LogStep("Waiting for workers to process chunks and orchestrator to detect files for all clients...")
	for _, clientID := range clients {
		sizeAtNotif, sizeWhenReady, err := waitForFilesReadyAndMeasure(t, clientID, 60*time.Second)
		if err != nil {
			t.Fatalf("Failed to measure mid-processing size for client %s: %v", clientID, err)
		}
		midSizeAtNotification += sizeAtNotif
		midSizeWhenReady += sizeWhenReady
		testing_utils.LogInfo("Test", "Mid-processing sizes for client %s - At notification: %s, When ready: %s",
			clientID, formatBytes(sizeAtNotif), formatBytes(sizeWhenReady))
	}
	testing_utils.LogInfo("Test", "Total mid-processing size (at notification): %s", formatBytes(midSizeAtNotification))
	testing_utils.LogInfo("Test", "Total mid-processing size (when ready): %s", formatBytes(midSizeWhenReady))

	// Now send join data chunks for each client
	testing_utils.LogStep("Sending join data chunks for all clients...")
	for _, clientID := range clients {
		joinChunk, err := loadJoinDataAsChunk(joinTestDataPath, clientID, t)
		if err != nil {
			t.Fatalf("Failed to load join data for client %s: %v", clientID, err)
		}

		chunkMsg := chunk.NewChunkMessage(joinChunk)
		serialized, err := chunk.SerializeChunkMessage(chunkMsg)
		if err != nil {
			t.Fatalf("Failed to serialize join chunk for client %s: %v", clientID, err)
		}

		if err := joinProducer.Send(serialized); err != 0 {
			t.Fatalf("Failed to send join chunk for client %s: %v", clientID, err)
		}

		testing_utils.LogInfo("Test", "Sent join chunk for client %s", clientID)
	}
	testing_utils.LogInfo("Test", "All join chunks sent for all clients")

	// Wait for orchestrator to complete signaling
	testing_utils.LogStep("Waiting for orchestrator to complete signaling (10 seconds)...")
	time.Sleep(10 * time.Second)

	// Check orchestrator logs
	testing_utils.LogStep("Checking orchestrator logs...")
	completionEvents := parseOrchestratorLogs(t)

	// Measure final sizes (files are NOT deleted by orchestrator, join worker handles that)
	for _, clientID := range clients {
		size, _ := getClientFilesSize(clientID)
		finalSize += size
		testing_utils.LogInfo("Test", "Final size for client %s: %s", clientID, formatBytes(size))
	}
	testing_utils.LogInfo("Test", "Total final size: %s", formatBytes(finalSize))

	// Report
	testing_utils.LogInfo("Test", "\n=== IN-FILE JOIN TEST RESULTS ===")
	testing_utils.LogStep("\n[File Size Progression]\n")
	testing_utils.LogStep("  Initial:                      %s\n", formatBytes(initialSize))
	testing_utils.LogStep("  Mid (at first notification):  %s\n", formatBytes(midSizeAtNotification))
	testing_utils.LogStep("  Mid (when files ready):       %s\n", formatBytes(midSizeWhenReady))
	testing_utils.LogStep("  Final:                        %s\n", formatBytes(finalSize))

	testing_utils.LogStep("\n[Orchestrator Activity]\n")
	testing_utils.LogStep("  Completion signals sent: %d\n", completionEvents.CompletionCount)

	// Verify
	testing_utils.LogStep("\n[Verification]\n")
	testPassed := true

	// Check 1: Files were created
	if midSizeWhenReady > 0 {
		testing_utils.LogSuccess("  ✓ Files were created during processing (%s)\n", formatBytes(midSizeWhenReady))
	} else {
		testing_utils.LogFailure("  ✗ No files observed during processing\n")
		testPassed = false
	}

	// Check 2: Orchestrator sent completion signals
	expectedCompletions := len(clients)
	if completionEvents.CompletionCount >= expectedCompletions {
		testing_utils.LogSuccess("  ✓ Orchestrator sent completion signals (%d)\n", completionEvents.CompletionCount)
	} else {
		testing_utils.LogFailure("  ✗ Expected %d completion signals, got %d\n", expectedCompletions, completionEvents.CompletionCount)
		testPassed = false
	}

	// Check 3: Files remain (not deleted - join worker's responsibility)
	if finalSize == 0 {
		testing_utils.LogSuccess("  ✓ No files in volume - as expected\n")
	} else {
		testing_utils.LogFailure("  ✗ Remaining files in volume (unexpected)\n")
		testPassed = false
	}

	// Final verdict
	fmt.Printf("\n[Final Verdict]\n")
	if testPassed {
		testing_utils.LogSuccess("TEST PASSED: In-file join pipeline is working correctly!")
	} else {
		testing_utils.LogFailure("TEST FAILED: Issues detected in pipeline")
		t.FailNow()
	}
}

// generateUserTestData generates test user data and returns the list of user IDs
func generateUserTestData(filePath string, numRecords int) ([]string, error) {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	if err := writer.Write([]string{"user_id", "gender", "birthdate", "registered_at"}); err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	// Generate records and track user IDs
	genders := []string{"male", "female"}
	userIDs := make([]string, 0, numRecords)

	for i := 1; i <= numRecords; i++ {
		userID := fmt.Sprintf("%d", 70000+i)
		userIDs = append(userIDs, userID)
		gender := genders[i%2]
		birthdate := fmt.Sprintf("19%02d-%02d-%02d", 50+(i%40), 1+(i%12), 1+(i%28))
		registeredAt := fmt.Sprintf("2024-04-%02d %02d:%02d:%02d", 1+(i%30), i%24, (i*7)%60, (i*13)%60)

		if err := writer.Write([]string{userID, gender, birthdate, registeredAt}); err != nil {
			return nil, fmt.Errorf("failed to write record: %w", err)
		}
	}

	return userIDs, nil
}

// generateJoinTestData generates join data for the given user IDs
func generateJoinTestData(filePath string, userIDs []string) error {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	if err := writer.Write([]string{"user_id", "store_id", "purchase_count", "rank"}); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Generate join records for each user
	for i, userID := range userIDs {
		storeID := fmt.Sprintf("STORE%03d", (i%50)+1)
		purchaseCount := fmt.Sprintf("%d", (i%100)+1)
		rank := fmt.Sprintf("%d", (i%10)+1)

		if err := writer.Write([]string{userID, storeID, purchaseCount, rank}); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	return nil
}

// loadTestDataAsChunks reads a CSV file and converts it into chunks for a specific client
func loadTestDataAsChunks(filePath string, clientID string, t *testing.T) ([]*chunk.Chunk, error) {
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

	// Split data into chunks (e.g., 50 records per chunk)
	chunkSize := 50
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
			clientID,
			TestFileID,
			0, // QueryType 0 for users
			chunkNumber,
			isLastChunk,
			isLastChunk, // isLastFromTable - same as isLastChunk for this test
			1,           // step
			len(chunkData),
			1, // tableID
			chunkData,
		)

		chunks = append(chunks, chunkObj)
	}

	return chunks, nil
}

// loadJoinDataAsChunk reads join CSV file and converts it into a single chunk with isLastChunk=1
func loadJoinDataAsChunk(filePath string, clientID string, t *testing.T) (*chunk.Chunk, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open join data file: %w", err)
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

	// Build CSV data for the single chunk (with header)
	var csvBuilder strings.Builder
	csvBuilder.WriteString(strings.Join(header, ","))
	csvBuilder.WriteString("\n")

	for _, record := range dataRecords {
		csvBuilder.WriteString(strings.Join(record, ","))
		csvBuilder.WriteString("\n")
	}

	chunkData := csvBuilder.String()

	// Create a single chunk with isLastChunk=1 and isLastFromTable=1
	chunkObj := chunk.NewChunk(
		clientID,
		"ST01",
		0, // QueryType 0
		1, // chunkNumber 1
		true, // isLastChunk = true
		true, // isLastFromTable = true
		1,    // step
		len(chunkData),
		2, // tableID 2 for join data
		chunkData,
	)

	return chunkObj, nil
}

// CompletionEvents holds parsed orchestrator completion information
type CompletionEvents struct {
	CompletionCount int
}

// waitForFilesReadyAndMeasure waits for the orchestrator to indicate files are ready, then measures file size
// Returns both sizes: (sizeAtFirstNotification, sizeWhenReady, error)
func waitForFilesReadyAndMeasure(t *testing.T, clientID string, timeout time.Duration) (int64, int64, error) {
	startTime := time.Now()
	firstNotificationSeen := false
	var sizeAtFirstNotification int64

	for {
		if time.Since(startTime) > timeout {
			return 0, 0, fmt.Errorf("timeout waiting for files to be ready")
		}

		// Check orchestrator logs
		cmd := exec.Command("docker", "logs", "infile-join-orchestrator-test")
		output, err := cmd.CombinedOutput()
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		logs := string(output)

		// First, look for chunk notification received
		if !firstNotificationSeen && strings.Contains(logs, "Received chunk notification") {
			firstNotificationSeen = true

			// Measure size at first notification
			size, err := getClientFilesSize(clientID)
			if err != nil {
				testing_utils.LogWarn("Test", "Failed to measure size at first notification: %v", err)
			} else {
				sizeAtFirstNotification = size
				testing_utils.LogInfo("Test", "First chunk notification received, client %s directory size: %s", clientID, formatBytes(size))
			}
		}

		// Then look for "All files completed for client"
		searchPattern := fmt.Sprintf("All files completed for client %s", clientID)
		if strings.Contains(logs, searchPattern) {
			// Files are ready! Measure now
			size, err := getClientFilesSize(clientID)
			if err != nil {
				return 0, 0, fmt.Errorf("failed to measure file size: %v", err)
			}
			testing_utils.LogInfo("Test", "Files completed for client %s, measured size: %s", clientID, formatBytes(size))
			return sizeAtFirstNotification, size, nil
		}

		// Sleep before checking again
		time.Sleep(500 * time.Millisecond)
	}
}

// parseOrchestratorLogs parses orchestrator container logs to find completion events
func parseOrchestratorLogs(t *testing.T) CompletionEvents {
	cmd := exec.Command("docker", "logs", "infile-join-orchestrator-test")
	output, err := cmd.CombinedOutput()
	if err != nil {
		testing_utils.LogWarn("Test", "Failed to get orchestrator logs: %v", err)
		return CompletionEvents{}
	}

	logs := string(output)
	lines := strings.Split(logs, "\n")

	events := CompletionEvents{}

	// Log orchestrator output for visibility
	t.Logf("\n=== ORCHESTRATOR LOGS ===")
	for _, line := range lines {
		// Filter to only show important lines to avoid spam
		if strings.Contains(line, "completed") ||
			strings.Contains(line, "completion") ||
			strings.Contains(line, "Sent completion signal") {
			t.Logf("  %s", line)
		}

		// Count completion events
		if strings.Contains(line, "Sent completion signal for client") {
			events.CompletionCount++
		}
	}
	t.Logf("=== END ORCHESTRATOR LOGS ===\n")

	return events
}

// getClientFilesSize calculates the total size of all partition files for a client
func getClientFilesSize(clientID string) (int64, error) {
	var totalSize int64

	// Pattern: {clientID}-users-partition-*.csv
	pattern := filepath.Join(SharedDataDir, fmt.Sprintf("%s-users-partition-*.csv", clientID))

	matches, err := filepath.Glob(pattern)
	if err != nil {
		return 0, fmt.Errorf("failed to glob files: %w", err)
	}

	for _, filePath := range matches {
		info, err := os.Stat(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, fmt.Errorf("failed to stat file %s: %w", filePath, err)
		}
		totalSize += info.Size()
	}

	return totalSize, nil
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
