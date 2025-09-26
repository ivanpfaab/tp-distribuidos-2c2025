package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDockerMultiClientIntegration tests multiple client containers communicating
// with server container via RabbitMQ container over Docker network
func TestDockerMultiClientIntegration(t *testing.T) {
	// Skip if not running in Docker environment
	if os.Getenv("DOCKER_TEST") != "true" {
		t.Skip("Skipping Docker integration test - set DOCKER_TEST=true to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Test scenarios
	t.Run("MultipleClientContainers", func(t *testing.T) {
		testMultipleClientContainers(t, ctx)
	})

	t.Run("ConcurrentClientMessages", func(t *testing.T) {
		testConcurrentClientMessages(t, ctx)
	})

	t.Run("ClientServerCommunication", func(t *testing.T) {
		testClientServerCommunication(t, ctx)
	})
}

// testMultipleClientContainers tests multiple client containers sending messages
func testMultipleClientContainers(t *testing.T, ctx context.Context) {
	// Start the full stack: RabbitMQ + Server + Multiple Clients
	composeFile := "../docker-compose.yml"

	// Start services
	err := runDockerCompose(composeFile, "up", "-d", "--build")
	require.NoError(t, err, "Failed to start Docker Compose services")

	// Ensure cleanup
	defer func() {
		runDockerCompose(composeFile, "down", "-v")
	}()

	// Wait for services to be ready
	err = waitForServices(ctx, []string{
		"rabbitmq-server",
		"echo-server",
		"echo-client-1",
		"echo-client-2",
		"echo-client-3",
	})
	require.NoError(t, err, "Services not ready within timeout")

	// Wait for clients to process their input files
	time.Sleep(10 * time.Second)

	// Check that all client containers completed successfully
	clientContainers := []string{"echo-client-1", "echo-client-2", "echo-client-3"}

	for _, container := range clientContainers {
		exitCode, err := getContainerExitCode(container)
		require.NoError(t, err, "Failed to get exit code for %s", container)
		assert.Equal(t, 0, exitCode, "Container %s should exit with code 0", container)
	}

	// Check server logs for received messages
	serverLogs, err := getContainerLogs("echo-server")
	require.NoError(t, err, "Failed to get server logs")

	// Verify server received messages from all clients
	assert.Contains(t, serverLogs, "Received:", "Server should have received messages")

	// Count unique client messages (basic validation)
	receivedCount := strings.Count(serverLogs, "Received:")
	assert.Greater(t, receivedCount, 0, "Server should have received at least one message")

	t.Logf("Server received %d messages from clients", receivedCount)
	t.Logf("Server logs sample: %s", truncateString(serverLogs, 500))
}

// testConcurrentClientMessages tests concurrent message handling
func testConcurrentClientMessages(t *testing.T, ctx context.Context) {
	// This test is similar to the above but focuses on concurrent behavior
	// The Docker Compose setup with multiple clients already tests this
	// We just need to verify the behavior

	composeFile := "../docker-compose.yml"

	err := runDockerCompose(composeFile, "up", "-d", "--build")
	require.NoError(t, err, "Failed to start Docker Compose services")

	defer func() {
		runDockerCompose(composeFile, "down", "-v")
	}()

	// Wait for services
	err = waitForServices(ctx, []string{
		"rabbitmq-server",
		"echo-server",
		"echo-client-1",
		"echo-client-2",
		"echo-client-3",
	})
	require.NoError(t, err, "Services not ready within timeout")

	// Wait for processing
	time.Sleep(15 * time.Second)

	// Check that all clients completed
	clientContainers := []string{"echo-client-1", "echo-client-2", "echo-client-3"}
	successCount := 0

	for _, container := range clientContainers {
		exitCode, err := getContainerExitCode(container)
		if err == nil && exitCode == 0 {
			successCount++
		}
	}

	assert.Equal(t, len(clientContainers), successCount, "All client containers should complete successfully")

	// Verify server handled concurrent messages
	serverLogs, err := getContainerLogs("echo-server")
	require.NoError(t, err, "Failed to get server logs")

	// Check for concurrent processing indicators
	assert.Contains(t, serverLogs, "Received:", "Server should show received messages")
	assert.Contains(t, serverLogs, "Sent response:", "Server should show sent responses")

	t.Logf("Concurrent message test: %d/%d clients completed successfully", successCount, len(clientContainers))
}

// testClientServerCommunication tests the communication flow
func testClientServerCommunication(t *testing.T, ctx context.Context) {
	composeFile := "../docker-compose.yml"

	err := runDockerCompose(composeFile, "up", "-d", "--build")
	require.NoError(t, err, "Failed to start Docker Compose services")

	defer func() {
		runDockerCompose(composeFile, "down", "-v")
	}()

	// Wait for services
	err = waitForServices(ctx, []string{
		"rabbitmq-server",
		"echo-server",
		"echo-client-1",
	})
	require.NoError(t, err, "Services not ready within timeout")

	// Wait for one client to process
	time.Sleep(8 * time.Second)

	// Get logs from both client and server
	clientLogs, err := getContainerLogs("echo-client-1")
	require.NoError(t, err, "Failed to get client logs")

	serverLogs, err := getContainerLogs("echo-server")
	require.NoError(t, err, "Failed to get server logs")

	// Verify communication flow
	assert.Contains(t, clientLogs, "Connected to RabbitMQ", "Client should connect to RabbitMQ")
	assert.Contains(t, clientLogs, "Sending line", "Client should send messages")
	assert.Contains(t, clientLogs, "Server response:", "Client should receive responses")

	assert.Contains(t, serverLogs, "Echo server listening", "Server should be listening")
	assert.Contains(t, serverLogs, "Received:", "Server should receive messages")
	assert.Contains(t, serverLogs, "Sent response:", "Server should send responses")

	// Verify message flow
	clientSentCount := strings.Count(clientLogs, "Sending line")
	clientReceivedCount := strings.Count(clientLogs, "Server response:")
	serverReceivedCount := strings.Count(serverLogs, "Received:")
	serverSentCount := strings.Count(serverLogs, "Sent response:")

	assert.Greater(t, clientSentCount, 0, "Client should send messages")
	assert.Equal(t, clientSentCount, clientReceivedCount, "Client should receive responses for all sent messages")
	assert.Equal(t, serverReceivedCount, serverSentCount, "Server should send responses for all received messages")

	t.Logf("Communication flow verified:")
	t.Logf("  Client sent: %d, received: %d", clientSentCount, clientReceivedCount)
	t.Logf("  Server received: %d, sent: %d", serverReceivedCount, serverSentCount)
}

// Helper functions

// runDockerCompose runs a docker-compose command
func runDockerCompose(composeFile, command string, args ...string) error {
	// Convert relative path to absolute path
	absPath, err := filepath.Abs(composeFile)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %v", err)
	}

	cmdArgs := append([]string{"-f", absPath, command}, args...)
	cmd := exec.Command("docker-compose", cmdArgs...)
	cmd.Dir = ".." // Run from project root

	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Docker Compose command failed: %s", string(output))
		return fmt.Errorf("docker-compose %s failed: %v\nOutput: %s", command, err, string(output))
	}

	return nil
}

// waitForServices waits for Docker containers to be running
func waitForServices(ctx context.Context, containerNames []string) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for services: %v", ctx.Err())
		case <-ticker.C:
			allReady := true
			for _, container := range containerNames {
				if !isContainerRunning(container) {
					allReady = false
					break
				}
			}
			if allReady {
				return nil
			}
		}
	}
}

// isContainerRunning checks if a Docker container is running
func isContainerRunning(containerName string) bool {
	cmd := exec.Command("docker", "ps", "--filter", "name="+containerName, "--format", "{{.Status}}")
	output, err := cmd.Output()
	if err != nil {
		return false
	}

	status := strings.TrimSpace(string(output))
	return strings.Contains(status, "Up")
}

// getContainerExitCode gets the exit code of a container
func getContainerExitCode(containerName string) (int, error) {
	cmd := exec.Command("docker", "inspect", containerName, "--format", "{{.State.ExitCode}}")
	output, err := cmd.Output()
	if err != nil {
		return -1, err
	}

	var exitCode int
	_, err = fmt.Sscanf(strings.TrimSpace(string(output)), "%d", &exitCode)
	return exitCode, err
}

// getContainerLogs gets the logs from a container
func getContainerLogs(containerName string) (string, error) {
	cmd := exec.Command("docker", "logs", containerName)
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}

	return string(output), nil
}

// truncateString truncates a string to the specified length
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// TestDockerNetworkConnectivity tests that containers can communicate over Docker network
func TestDockerNetworkConnectivity(t *testing.T) {
	if os.Getenv("DOCKER_TEST") != "true" {
		t.Skip("Skipping Docker network test - set DOCKER_TEST=true to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Start only RabbitMQ and Server
	composeFile := "../docker-compose.yml"

	err := runDockerCompose(composeFile, "up", "-d", "rabbitmq", "server")
	require.NoError(t, err, "Failed to start RabbitMQ and Server")

	defer func() {
		runDockerCompose(composeFile, "down", "-v")
	}()

	// Wait for services
	err = waitForServices(ctx, []string{"rabbitmq-server", "echo-server"})
	require.NoError(t, err, "Services not ready within timeout")

	// Test network connectivity by running a client container
	// that connects to the server via RabbitMQ
	clientCmd := []string{
		"run", "--rm",
		"--network", "tp-distribuidos-2c2025_echo-network",
		"-v", fmt.Sprintf("%s/input.txt:/app/input.txt:ro", getProjectRoot()),
		"tp-distribuidos-2c2025_client",
		"./main", "/app/input.txt",
	}

	cmd := exec.CommandContext(ctx, "docker", clientCmd...)
	output, err := cmd.CombinedOutput()

	// The client should complete successfully
	assert.NoError(t, err, "Client container should complete successfully\nOutput: %s", string(output))

	// Verify the client connected and received responses
	outputStr := string(output)
	assert.Contains(t, outputStr, "Connected to RabbitMQ", "Client should connect to RabbitMQ")
	assert.Contains(t, outputStr, "Server response:", "Client should receive server responses")

	t.Logf("Network connectivity test passed. Client output:\n%s", outputStr)
}

// getProjectRoot returns the project root directory
func getProjectRoot() string {
	// Assuming we're in tests/ directory, go up one level
	return ".."
}
