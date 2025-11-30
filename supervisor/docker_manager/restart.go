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
	log.Printf("[DockerManager] Attempting to restart container: %s (stop + start)", containerName)

	ctx, cancel := context.WithTimeout(context.Background(), dm.restartTimeout)
	defer cancel()

	stopCmd := exec.CommandContext(ctx, "docker", "stop", containerName)
	stopOutput, stopErr := stopCmd.CombinedOutput()

	if stopErr != nil {
		log.Printf("[DockerManager] Failed to stop container %s: %v\nOutput: %s",
			containerName, stopErr, string(stopOutput))
		return fmt.Errorf("failed to stop container %s: %w", containerName, stopErr)
	}

	log.Printf("[DockerManager] Successfully stopped container: %s", containerName)

	ctx2, cancel2 := context.WithTimeout(context.Background(), dm.restartTimeout)
	defer cancel2()

	startCmd := exec.CommandContext(ctx2, "docker", "start", containerName)
	startOutput, startErr := startCmd.CombinedOutput()

	if startErr != nil {
		log.Printf("[DockerManager] Failed to start container %s: %v\nOutput: %s",
			containerName, startErr, string(startOutput))
		return fmt.Errorf("failed to start container %s: %w", containerName, startErr)
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

