package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type SupervisorConfig struct {
	SupervisorID   int
	ElectionPort   string
	Peers          map[int]string
	MonitoredWorkers map[string]string
	ProbeTimeout   time.Duration
	ProbeInterval  time.Duration
	RestartTimeout time.Duration
}

func LoadConfig() (*SupervisorConfig, error) {
	supervisorIDStr := os.Getenv("SUPERVISOR_ID")
	if supervisorIDStr == "" {
		return nil, fmt.Errorf("SUPERVISOR_ID environment variable is required")
	}

	supervisorID, err := strconv.Atoi(supervisorIDStr)
	if err != nil {
		return nil, fmt.Errorf("invalid SUPERVISOR_ID: %w", err)
	}

	electionPort := os.Getenv("ELECTION_PORT")
	if electionPort == "" {
		electionPort = "9000"
	}

	peers, err := parsePeers(os.Getenv("SUPERVISOR_PEERS"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse SUPERVISOR_PEERS: %w", err)
	}

	workers, err := parseWorkers(os.Getenv("MONITORED_WORKERS"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse MONITORED_WORKERS: %w", err)
	}

	probeTimeout, err := parseDuration(os.Getenv("PROBE_TIMEOUT"), 2*time.Second)
	if err != nil {
		return nil, fmt.Errorf("invalid PROBE_TIMEOUT: %w", err)
	}

	probeInterval, err := parseDuration(os.Getenv("PROBE_INTERVAL"), 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("invalid PROBE_INTERVAL: %w", err)
	}

	restartTimeout, err := parseDuration(os.Getenv("RESTART_TIMEOUT"), 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("invalid RESTART_TIMEOUT: %w", err)
	}

	return &SupervisorConfig{
		SupervisorID:     supervisorID,
		ElectionPort:     electionPort,
		Peers:            peers,
		MonitoredWorkers: workers,
		ProbeTimeout:     probeTimeout,
		ProbeInterval:    probeInterval,
		RestartTimeout:   restartTimeout,
	}, nil
}

func parsePeers(peerStr string) (map[int]string, error) {
	peers := make(map[int]string)
	
	if peerStr == "" {
		return peers, nil
	}

	peerList := strings.Split(peerStr, ",")
	for _, peer := range peerList {
		peer = strings.TrimSpace(peer)
		if peer == "" {
			continue
		}

		parts := strings.Split(peer, ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid peer format: %s (expected format: supervisor-ID:host:port)", peer)
		}

		idParts := strings.Split(parts[0], "-")
		if len(idParts) != 2 {
			return nil, fmt.Errorf("invalid peer ID format: %s", parts[0])
		}

		id, err := strconv.Atoi(idParts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid peer ID: %s", idParts[1])
		}

		address := parts[1] + ":" + parts[2]
		peers[id] = address
	}

	return peers, nil
}

func parseWorkers(workerStr string) (map[string]string, error) {
	workers := make(map[string]string)
	
	if workerStr == "" {
		return workers, nil
	}

	workerList := strings.Split(workerStr, ",")
	for _, worker := range workerList {
		worker = strings.TrimSpace(worker)
		if worker == "" {
			continue
		}

		parts := strings.Split(worker, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid worker format: %s (expected format: container-name:port)", worker)
		}

		containerName := parts[0]
		address := parts[0] + ":" + parts[1]
		workers[containerName] = address
	}

	return workers, nil
}

func parseDuration(durationStr string, defaultDuration time.Duration) (time.Duration, error) {
	if durationStr == "" {
		return defaultDuration, nil
	}

	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		return 0, err
	}

	return duration, nil
}

