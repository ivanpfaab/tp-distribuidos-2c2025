package docker_manager

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"time"
)

type DockerManager struct {
	restartTimeout time.Duration
}

func NewDockerManager(restartTimeout time.Duration) *DockerManager {
	return &DockerManager{
		restartTimeout: restartTimeout,
	}
}

func (dm *DockerManager) RestartContainer(containerName string) error {
	log.Printf("[DockerManager] Attempting to restart container: %s", containerName)

	ctx, cancel := context.WithTimeout(context.Background(), dm.restartTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "restart", containerName)
	output, err := cmd.CombinedOutput()

	if err != nil {
		log.Printf("[DockerManager] Failed to restart container %s: %v\nOutput: %s",
			containerName, err, string(output))
		return fmt.Errorf("failed to restart container %s: %w", containerName, err)
	}

	log.Printf("[DockerManager] Successfully restarted container: %s", containerName)
	return nil
}

func (dm *DockerManager) StopContainer(containerName string) error {
	log.Printf("[DockerManager] Attempting to stop container: %s", containerName)

	ctx, cancel := context.WithTimeout(context.Background(), dm.restartTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "stop", containerName)
	output, err := cmd.CombinedOutput()

	if err != nil {
		log.Printf("[DockerManager] Failed to stop container %s: %v\nOutput: %s",
			containerName, err, string(output))
		return fmt.Errorf("failed to stop container %s: %w", containerName, err)
	}

	log.Printf("[DockerManager] Successfully stopped container: %s", containerName)
	return nil
}

func (dm *DockerManager) KillContainer(containerName string) error {
	log.Printf("[DockerManager] Attempting to kill container: %s", containerName)

	ctx, cancel := context.WithTimeout(context.Background(), dm.restartTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "kill", containerName)
	output, err := cmd.CombinedOutput()

	if err != nil {
		log.Printf("[DockerManager] Failed to kill container %s: %v\nOutput: %s",
			containerName, err, string(output))
		return fmt.Errorf("failed to kill container %s: %w", containerName, err)
	}

	log.Printf("[DockerManager] Successfully killed container: %s", containerName)
	return nil
}

func (dm *DockerManager) IsDockerAvailable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "ps")
	err := cmd.Run()

	return err == nil
}

