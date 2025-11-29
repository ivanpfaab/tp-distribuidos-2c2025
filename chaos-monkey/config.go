package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type ChaosConfig struct {
	TargetContainers []string
	KillInterval     time.Duration
	KillProbability  float64
	PauseDuration    time.Duration
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

	return &ChaosConfig{
		TargetContainers: targets,
		KillInterval:     killInterval,
		KillProbability:  killProb,
		PauseDuration:    pauseDur,
	}, nil
}
