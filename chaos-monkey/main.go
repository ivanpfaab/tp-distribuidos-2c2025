package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

type ChaosMonkey struct {
	config *ChaosConfig
}

func NewChaosMonkey(config *ChaosConfig) *ChaosMonkey {
	return &ChaosMonkey{
		config: config,
	}
}

func (cm *ChaosMonkey) Start() {
	log.Println("🐒 Chaos Monkey started!")
	log.Printf("Target containers: %v", cm.config.TargetContainers)
	log.Printf("Kill interval: %v", cm.config.KillInterval)
	log.Printf("Kill probability: %.2f%%", cm.config.KillProbability*100)

	rand.Seed(time.Now().UnixNano())

	ticker := time.NewTicker(cm.config.KillInterval)
	defer ticker.Stop()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-ticker.C:
			cm.maybeInjectFault()

		case <-sigChan:
			log.Println("Chaos Monkey stopping...")
			return
		}
	}
}

func (cm *ChaosMonkey) maybeInjectFault() {
	if rand.Float64() > cm.config.KillProbability {
		return
	}

	if len(cm.config.TargetContainers) == 0 {
		return
	}

	target := cm.config.TargetContainers[rand.Intn(len(cm.config.TargetContainers))]

	actions := []string{"kill", "pause", "stop"}
	action := actions[rand.Intn(len(actions))]

	switch action {
	case "kill":
		cm.killContainer(target)
	case "pause":
		cm.pauseContainer(target)
	case "stop":
		cm.stopContainer(target)
	}
}

func (cm *ChaosMonkey) killContainer(containerName string) {
	log.Printf("CHAOS: Killing container %s", containerName)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "kill", containerName)
	output, err := cmd.CombinedOutput()

	if err != nil {
		log.Printf("Failed to kill container: %v\nOutput: %s", err, string(output))
	} else {
		log.Printf("Successfully killed container %s", containerName)
	}
}

func (cm *ChaosMonkey) pauseContainer(containerName string) {
	log.Printf("CHAOS: Pausing container %s for %v", containerName, cm.config.PauseDuration)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "pause", containerName)
	if output, err := cmd.CombinedOutput(); err != nil {
		log.Printf("Failed to pause: %v\nOutput: %s", err, string(output))
		return
	}

	log.Printf("Container %s paused", containerName)

	time.Sleep(cm.config.PauseDuration)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	cmd2 := exec.CommandContext(ctx2, "docker", "unpause", containerName)
	if output, err := cmd2.CombinedOutput(); err != nil {
		log.Printf("Failed to unpause: %v\nOutput: %s", err, string(output))
		return
	}

	log.Printf("Container %s unpaused", containerName)
}

func (cm *ChaosMonkey) stopContainer(containerName string) {
	log.Printf("CHAOS: Stopping container %s", containerName)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "stop", containerName)
	output, err := cmd.CombinedOutput()

	if err != nil {
		log.Printf("Failed to stop container: %v\nOutput: %s", err, string(output))
	} else {
		log.Printf("Successfully stopped container %s", containerName)
	}
}

func main() {
	log.Println("=== Chaos Monkey Starting ===")

	config, err := LoadChaosConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	monkey := NewChaosMonkey(config)
	monkey.Start()

	log.Println("=== Chaos Monkey Stopped ===")
}

