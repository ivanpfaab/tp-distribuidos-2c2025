package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	log.Println("=== Supervisor Starting ===")

	config, err := LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Supervisor ID: %d", config.SupervisorID)
	log.Printf("Election Port: %s", config.ElectionPort)
	log.Printf("Peers: %v", config.Peers)
	log.Printf("Monitoring %d workers", len(config.MonitoredWorkers))

	supervisor, err := NewSupervisor(config)
	if err != nil {
		log.Fatalf("Failed to create supervisor: %v", err)
	}

	if err := supervisor.Start(); err != nil {
		log.Fatalf("Failed to start supervisor: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("=== Supervisor Running ===")
	<-sigChan
	log.Println("=== Supervisor Shutting Down ===")

	supervisor.Stop()
	log.Println("=== Supervisor Stopped ===")
}

