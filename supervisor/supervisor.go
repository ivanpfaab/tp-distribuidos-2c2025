package main

import (
	"log"
	"sync"

	"github.com/tp-distribuidos-2c2025/shared/health_server"
	"github.com/tp-distribuidos-2c2025/supervisor/docker_manager"
	supervisor_election "github.com/tp-distribuidos-2c2025/supervisor/election"
	"github.com/tp-distribuidos-2c2025/supervisor/monitor"
)

type Supervisor struct {
	config        *SupervisorConfig
	healthServer  *health_server.HealthServer
	election      *supervisor_election.BullyElection
	healthMonitor *monitor.HealthMonitor
	dockerManager *docker_manager.DockerManager
	mu            sync.Mutex
}

func NewSupervisor(config *SupervisorConfig) (*Supervisor, error) {
	healthServer := health_server.NewHealthServer("8888")

	dockerManager := docker_manager.NewDockerManager(config.RestartTimeout)

	if !dockerManager.IsDockerAvailable() {
		log.Printf("[Supervisor] WARNING: Docker is not available. Container restarts will fail.")
	}

	supervisor := &Supervisor{
		config:        config,
		healthServer:  healthServer,
		dockerManager: dockerManager,
	}

	bullyElection, err := supervisor_election.NewBullyElection(
		config.SupervisorID,
		config.Peers,
		config.ElectionPort,
		supervisor.onBecomeLeader,
		supervisor.onLeaderChange,
	)
	if err != nil {
		return nil, err
	}
	supervisor.election = bullyElection

	return supervisor, nil
}

func (s *Supervisor) Start() error {
	log.Printf("[Supervisor] Starting Supervisor %d", s.config.SupervisorID)

	if err := s.healthServer.Start(); err != nil {
		return err
	}
	log.Printf("[Supervisor] Health server started")

	if err := s.election.Start(); err != nil {
		return err
	}
	log.Printf("[Supervisor] Bully election started")

	s.election.StartElectionWithDelay()
	log.Printf("[Supervisor] Initial election will start after stagger delay")

	return nil
}

func (s *Supervisor) onBecomeLeader() {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[Supervisor] Node %d: Became LEADER - starting health monitoring", s.config.SupervisorID)

	if s.healthMonitor != nil {
		log.Printf("[Supervisor] Node %d: Stopping existing health monitor before starting new one", s.config.SupervisorID)
		s.healthMonitor.Stop()
		s.healthMonitor = nil
	}

	s.healthMonitor = monitor.NewHealthMonitor(
		s.config.MonitoredWorkers,
		s.config.ProbeTimeout,
		s.config.ProbeInterval,
		s.restartWorker,
	)

	s.healthMonitor.Start()
	log.Printf("[Supervisor] Node %d: Health monitoring started", s.config.SupervisorID)
}

func (s *Supervisor) onLeaderChange(newLeaderID int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[Supervisor] Node %d: New leader is %d - stopping health monitoring", s.config.SupervisorID, newLeaderID)

	if s.healthMonitor != nil {
		s.healthMonitor.Stop()
		s.healthMonitor = nil
		log.Printf("[Supervisor] Node %d: Health monitoring stopped", s.config.SupervisorID)
	}
}

func (s *Supervisor) restartWorker(containerName string) bool {
	log.Printf("[Supervisor] Restarting worker: %s", containerName)

	err := s.dockerManager.RestartContainer(containerName)
	if err != nil {
		log.Printf("[Supervisor] Failed to restart %s: %v", containerName, err)
		return false
	}

	log.Printf("[Supervisor] Successfully restarted worker: %s", containerName)
	return true
}

func (s *Supervisor) Stop() {
	log.Printf("[Supervisor] Stopping Supervisor %d", s.config.SupervisorID)

	if s.healthMonitor != nil {
		s.healthMonitor.Stop()
	}

	if s.election != nil {
		s.election.Stop()
	}

	if s.healthServer != nil {
		s.healthServer.Stop()
	}

	log.Printf("[Supervisor] Stopped")
}

func (s *Supervisor) IsLeader() bool {
	if s.election == nil {
		return false
	}
	return s.election.IsLeader()
}
