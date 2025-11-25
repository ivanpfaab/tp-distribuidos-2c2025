package monitor

import (
	"log"
	"net"
	"sync"
	"time"
)

type RestartCallback func(containerName string)

type HealthMonitor struct {
	workers         map[string]*WorkerInfo
	mutex           sync.RWMutex
	probeTimeout    time.Duration
	probeInterval   time.Duration
	restartCallback RestartCallback
	stopChan        chan bool
	wg              sync.WaitGroup
	restartCooldown time.Duration
	lastRestart     map[string]time.Time
}

func NewHealthMonitor(
	workerList map[string]string,
	probeTimeout time.Duration,
	probeInterval time.Duration,
	restartCallback RestartCallback,
) *HealthMonitor {
	workers := make(map[string]*WorkerInfo)
	for containerName, address := range workerList {
		workers[containerName] = NewWorkerInfo(containerName, address)
	}

	return &HealthMonitor{
		workers:         workers,
		probeTimeout:    probeTimeout,
		probeInterval:   probeInterval,
		restartCallback: restartCallback,
		stopChan:        make(chan bool),
		restartCooldown: 30 * time.Second,
		lastRestart:     make(map[string]time.Time),
	}
}

func (hm *HealthMonitor) Start() {
	log.Printf("[HealthMonitor] Starting health monitor for %d workers", len(hm.workers))
	log.Printf("[HealthMonitor] Probe timeout: %v, Probe interval: %v", hm.probeTimeout, hm.probeInterval)

	hm.wg.Add(1)
	go hm.monitorLoop()
}

func (hm *HealthMonitor) monitorLoop() {
	defer hm.wg.Done()

	ticker := time.NewTicker(hm.probeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hm.probeAllWorkers()

		case <-hm.stopChan:
			log.Printf("[HealthMonitor] Stopping health monitor")
			return
		}
	}
}

func (hm *HealthMonitor) probeAllWorkers() {
	hm.mutex.RLock()
	workersCopy := make(map[string]*WorkerInfo)
	for k, v := range hm.workers {
		workersCopy[k] = v
	}
	hm.mutex.RUnlock()

	var wg sync.WaitGroup
	for containerName, worker := range workersCopy {
		wg.Add(1)
		go func(name string, w *WorkerInfo) {
			defer wg.Done()
			hm.probeWorker(name, w)
		}(containerName, worker)
	}
	wg.Wait()
}

func (hm *HealthMonitor) probeWorker(containerName string, worker *WorkerInfo) {
	conn, err := net.DialTimeout("tcp", worker.Address, hm.probeTimeout)
	if err != nil {
		hm.handleProbeFailure(containerName, worker)
		return
	}
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(hm.probeTimeout))
	_, err = conn.Write([]byte("PING"))
	if err != nil {
		hm.handleProbeFailure(containerName, worker)
		return
	}

	conn.SetReadDeadline(time.Now().Add(hm.probeTimeout))
	buf := make([]byte, 4)
	n, err := conn.Read(buf)
	if err != nil || n != 4 || string(buf[:4]) != "PONG" {
		hm.handleProbeFailure(containerName, worker)
		return
	}

	hm.handleProbeSuccess(containerName, worker)
}

func (hm *HealthMonitor) handleProbeSuccess(containerName string, worker *WorkerInfo) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	wasAlive := worker.IsAlive
	worker.IsAlive = true
	worker.LastProbe = time.Now()
	worker.FailureCount = 0

	if !wasAlive {
		log.Printf("[HealthMonitor] Worker %s (%s) is now ALIVE", containerName, worker.Address)
	}
}

func (hm *HealthMonitor) handleProbeFailure(containerName string, worker *WorkerInfo) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	worker.FailureCount++
	worker.LastProbe = time.Now()

	if worker.IsAlive && worker.FailureCount >= 2 { // TODO: make this configurable or at least a constant
		worker.IsAlive = false
		log.Printf("[HealthMonitor] Worker %s (%s) marked as DEAD after %d consecutive failures",
			containerName, worker.Address, worker.FailureCount)

		hm.mutex.Unlock()
		hm.attemptRestart(containerName)
		hm.mutex.Lock()
	}
}

func (hm *HealthMonitor) attemptRestart(containerName string) {
	hm.mutex.RLock()
	lastRestart, exists := hm.lastRestart[containerName]
	hm.mutex.RUnlock()

	if exists && time.Since(lastRestart) < hm.restartCooldown {
		log.Printf("[HealthMonitor] Skipping restart for %s (cooldown period)", containerName)
		return
	}

	log.Printf("[HealthMonitor] Attempting to restart worker: %s", containerName)

	hm.mutex.Lock()
	hm.lastRestart[containerName] = time.Now()
	hm.mutex.Unlock()

	if hm.restartCallback != nil {
		hm.restartCallback(containerName)
	}
}

func (hm *HealthMonitor) Stop() {
	close(hm.stopChan)
	hm.wg.Wait()
	log.Printf("[HealthMonitor] Stopped")
}

func (hm *HealthMonitor) GetWorkerStatus() map[string]*WorkerInfo {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()

	status := make(map[string]*WorkerInfo)
	for k, v := range hm.workers {
		status[k] = &WorkerInfo{
			ContainerName: v.ContainerName,
			Address:       v.Address,
			LastProbe:     v.LastProbe,
			IsAlive:       v.IsAlive,
			FailureCount:  v.FailureCount,
		}
	}
	return status
}
