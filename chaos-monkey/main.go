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
	rng    *rand.Rand
}

func NewChaosMonkey(config *ChaosConfig) *ChaosMonkey {
	return &ChaosMonkey{
		config: config,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (cm *ChaosMonkey) Start() {
	log.Println("Chaos Monkey started!")
	log.Printf("Strategy: %s", cm.config.Strategy)
	log.Printf("Target containers: %v", cm.config.TargetContainers)

	switch cm.config.Strategy {
	case StrategyPeriodic:
		cm.runPeriodicStrategy()
	case StrategyExponential:
		cm.runExponentialStrategy()
	default:
		log.Fatalf("Unknown strategy: %s", cm.config.Strategy)
	}
}

// runPeriodicStrategy kills containers at fixed intervals with a probability
func (cm *ChaosMonkey) runPeriodicStrategy() {
	log.Printf("Kill interval: %v", cm.config.KillInterval)
	log.Printf("Kill probability: %.2f%%", cm.config.KillProbability*100)

	ticker := time.NewTicker(cm.config.KillInterval)
	defer ticker.Stop()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-ticker.C:
			cm.maybeInjectFaultWithProbability()

		case <-sigChan:
			log.Println("Chaos Monkey stopping...")
			return
		}
	}
}

// runExponentialStrategy kills containers with exponentially distributed intervals
func (cm *ChaosMonkey) runExponentialStrategy() {
	meanInterval := cm.config.KillInterval
	log.Printf("Mean kill interval: %v (exponential distribution)", meanInterval)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	for {
		// Generate exponentially distributed wait time
		waitTime := cm.nextExponentialInterval(meanInterval)
		log.Printf("Next kill in: %v", waitTime.Round(time.Millisecond))

		select {
		case <-time.After(waitTime):
			cm.injectFault()

		case <-sigChan:
			log.Println("Chaos Monkey stopping...")
			return
		}
	}
}

// nextExponentialInterval generates an exponentially distributed duration
// The exponential distribution has mean = meanInterval
func (cm *ChaosMonkey) nextExponentialInterval(meanInterval time.Duration) time.Duration {
	// rand.ExpFloat64() returns exponentially distributed value with mean 1
	// Multiply by meanInterval to get the desired mean
	sample := cm.rng.ExpFloat64() * float64(meanInterval)
	return time.Duration(sample)
}

// maybeInjectFaultWithProbability is used by the periodic strategy
// It injects a fault based on the configured probability
func (cm *ChaosMonkey) maybeInjectFaultWithProbability() {
	if cm.rng.Float64() > cm.config.KillProbability {
		return
	}

	cm.injectFault()
}

// injectFault kills a random container from the target list
func (cm *ChaosMonkey) injectFault() {
	if len(cm.config.TargetContainers) == 0 {
		return
	}

	target := cm.config.TargetContainers[cm.rng.Intn(len(cm.config.TargetContainers))]
	cm.killContainer(target)
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
