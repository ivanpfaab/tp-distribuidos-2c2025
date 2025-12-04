package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// KillStrategy defines the strategy for timing container kills
type KillStrategy string

const (
	// StrategyPeriodic kills containers at fixed intervals with a probability
	StrategyPeriodic KillStrategy = "periodic"
	// StrategyExponential kills containers with exponentially distributed intervals
	StrategyExponential KillStrategy = "exponential"
)

type ChaosConfig struct {
	TargetContainers []string
	KillInterval     time.Duration // For periodic: fixed interval; For exponential: mean interval
	KillProbability  float64       // Only used for periodic strategy
	PauseDuration    time.Duration
	Strategy         KillStrategy
}

func LoadChaosConfig() (*ChaosConfig, error) {
	targetsStr := os.Getenv("TARGET_CONTAINERS")
	if targetsStr == "" {
		return nil, fmt.Errorf("TARGET_CONTAINERS environment variable is required")
	}

	targets := strings.Split(targetsStr, ",")
	for i := range targets {
		targets[i] = strings.TrimSpace(targets[i])
	}

	killIntervalStr := os.Getenv("KILL_INTERVAL")
	if killIntervalStr == "" {
		killIntervalStr = "5s"
	}
	killInterval, err := time.ParseDuration(killIntervalStr)
	if err != nil {
		return nil, fmt.Errorf("invalid KILL_INTERVAL: %w", err)
	}

	killProbStr := os.Getenv("KILL_PROBABILITY")
	if killProbStr == "" {
		killProbStr = "0.3"
	}
	killProb, err := strconv.ParseFloat(killProbStr, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid KILL_PROBABILITY: %w", err)
	}

	pauseDurStr := os.Getenv("PAUSE_DURATION")
	if pauseDurStr == "" {
		pauseDurStr = "3s"
	}
	pauseDur, err := time.ParseDuration(pauseDurStr)
	if err != nil {
		return nil, fmt.Errorf("invalid PAUSE_DURATION: %w", err)
	}

	strategyStr := os.Getenv("KILL_STRATEGY")
	if strategyStr == "" {
		strategyStr = "periodic"
	}
	strategy := KillStrategy(strings.ToLower(strategyStr))
	if strategy != StrategyPeriodic && strategy != StrategyExponential {
		return nil, fmt.Errorf("invalid KILL_STRATEGY: %s (must be 'periodic' or 'exponential')", strategyStr)
	}

	return &ChaosConfig{
		TargetContainers: targets,
		KillInterval:     killInterval,
		KillProbability:  killProb,
		PauseDuration:    pauseDur,
		Strategy:         strategy,
	}, nil
}
