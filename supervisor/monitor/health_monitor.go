package monitor

import (
	"log"
	"net"
	"sync"
	"time"

	"github.com/tp-distribuidos-2c2025/shared/netio"
)

// RestartCallback is called when a worker needs to be restarted.
// Returns true if the restart was initiated successfully, false otherwise.
type RestartCallback func(containerName string) bool

// HealthMonitorConfig holds configuration for the health monitor
type HealthMonitorConfig struct {
	ProbeTimeout          time.Duration
	ProbeInterval         time.Duration
	FailureThreshold      int           // Number of consecutive failures before triggering restart
	MaxRestartAttempts    int           // Maximum restart attempts before marking worker as dead
	RestartBackoffInitial time.Duration // Initial backoff time between restart attempts
	RestartBackoffMax     time.Duration // Maximum backoff time between restart attempts
	PostRestartGrace      time.Duration // Grace period after restart before probing
}

// DefaultHealthMonitorConfig returns a sensible default configuration
func DefaultHealthMonitorConfig() HealthMonitorConfig {
	return HealthMonitorConfig{
		ProbeTimeout:          2 * time.Second,
		ProbeInterval:         2 * time.Second,
		FailureThreshold:      2,
		MaxRestartAttempts:    5,
		RestartBackoffInitial: 2 * time.Second,
		RestartBackoffMax:     30 * time.Second,
		PostRestartGrace:      3 * time.Second,
	}
}

type HealthMonitor struct {
	workers         map[string]*workerContext
	mutex           sync.RWMutex
	config          HealthMonitorConfig
	restartCallback RestartCallback
	wg              sync.WaitGroup
}

func NewHealthMonitor(
	workerList map[string]string,
	probeTimeout time.Duration,
	probeInterval time.Duration,
	restartCallback RestartCallback,
) *HealthMonitor {
	workers := make(map[string]*workerContext)
	for containerName, address := range workerList {
		workers[containerName] = &workerContext{
			info:     NewWorkerInfo(containerName, address),
			stopChan: make(chan struct{}),
		}
	}

	config := DefaultHealthMonitorConfig()
	config.ProbeTimeout = probeTimeout
	config.ProbeInterval = probeInterval

	return &HealthMonitor{
		workers:         workers,
		config:          config,
		restartCallback: restartCallback,
	}
}

// NewHealthMonitorWithConfig creates a health monitor with full configuration control
func NewHealthMonitorWithConfig(
	workerList map[string]string,
	config HealthMonitorConfig,
	restartCallback RestartCallback,
) *HealthMonitor {
	workers := make(map[string]*workerContext)
	for containerName, address := range workerList {
		workers[containerName] = &workerContext{
			info:     NewWorkerInfo(containerName, address),
			stopChan: make(chan struct{}),
		}
	}

	return &HealthMonitor{
		workers:         workers,
		config:          config,
		restartCallback: restartCallback,
	}
}

func (hm *HealthMonitor) Start() {
	log.Printf("[HealthMonitor] Starting health monitor for %d workers", len(hm.workers))
	log.Printf("[HealthMonitor] Config: probe_timeout=%v, probe_interval=%v, failure_threshold=%d, max_restart_attempts=%d",
		hm.config.ProbeTimeout, hm.config.ProbeInterval, hm.config.FailureThreshold, hm.config.MaxRestartAttempts)

	// Start one goroutine per worker
	for containerName, ctx := range hm.workers {
		hm.wg.Add(1)
		go hm.monitorWorker(containerName, ctx)
	}
}

// monitorWorker is the main loop for monitoring a single worker.
// Each worker gets exactly one goroutine that handles all its state transitions.
func (hm *HealthMonitor) monitorWorker(containerName string, ctx *workerContext) {
	defer hm.wg.Done()

	log.Printf("[HealthMonitor] Started monitoring goroutine for worker: %s", containerName)

	for {
		select {
		case <-ctx.stopChan:
			log.Printf("[HealthMonitor] Stopping monitoring for worker: %s", containerName)
			return
		default:
			hm.probeAndHandleWorker(containerName, ctx)

			// Wait for next probe interval, but check for stop signal
			select {
			case <-ctx.stopChan:
				log.Printf("[HealthMonitor] Stopping monitoring for worker: %s", containerName)
				return
			case <-time.After(hm.config.ProbeInterval):
				// Continue to next probe
			}
		}
	}
}

// probeAndHandleWorker performs a single probe and handles the result
func (hm *HealthMonitor) probeAndHandleWorker(containerName string, ctx *workerContext) {
	// Skip probing if worker is marked as permanently dead
	hm.mutex.RLock()
	state := ctx.info.State
	hm.mutex.RUnlock()

	if state == StateDead {
		// Worker is permanently dead, keep probing in case it recovers externally
		// but don't attempt any more restarts
		if hm.probeWorker(ctx.info.Address) {
			hm.handleProbeSuccess(containerName, ctx)
		}
		return
	}

	// Perform the probe
	success := hm.probeWorker(ctx.info.Address)

	if success {
		hm.handleProbeSuccess(containerName, ctx)
	} else {
		hm.handleProbeFailure(containerName, ctx)
	}
}

// probeWorker attempts to connect and ping a worker, returns true if successful
func (hm *HealthMonitor) probeWorker(address string) bool {
	conn, err := net.DialTimeout("tcp", address, hm.config.ProbeTimeout)
	if err != nil {
		return false
	}
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(hm.config.ProbeTimeout))
	err = netio.WriteAll(conn, []byte("PING"))
	if err != nil {
		return false
	}

	conn.SetReadDeadline(time.Now().Add(hm.config.ProbeTimeout))
	buf := make([]byte, 4)
	err = netio.ReadFull(conn, buf)
	if err != nil || string(buf) != "PONG" {
		return false
	}

	return true
}

func (hm *HealthMonitor) handleProbeSuccess(containerName string, ctx *workerContext) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	prevState := ctx.info.State

	ctx.info.State = StateHealthy
	ctx.info.LastProbe = time.Now()
	ctx.info.FailureCount = 0
	// Reset restart attempts on successful probe - worker is healthy again
	ctx.info.RestartAttempts = 0

	if prevState != StateHealthy {
		log.Printf("[HealthMonitor] Worker %s (%s) is now HEALTHY (was %s)",
			containerName, ctx.info.Address, prevState)
	}
}

func (hm *HealthMonitor) handleProbeFailure(containerName string, ctx *workerContext) {
	hm.mutex.Lock()
	ctx.info.FailureCount++
	ctx.info.LastProbe = time.Now()
	failureCount := ctx.info.FailureCount
	state := ctx.info.State
	hm.mutex.Unlock()

	// If we haven't hit the failure threshold yet, just mark as suspected
	if failureCount < hm.config.FailureThreshold {
		hm.mutex.Lock()
		if ctx.info.State == StateHealthy {
			ctx.info.State = StateSuspected
			log.Printf("[HealthMonitor] Worker %s (%s) is SUSPECTED (failure %d/%d)",
				containerName, ctx.info.Address, failureCount, hm.config.FailureThreshold)
		}
		hm.mutex.Unlock()
		return
	}

	// We've hit the failure threshold - need to restart
	if state == StateRestarting {
		// Already restarting, don't trigger another restart
		return
	}

	// Attempt restart with retry logic
	hm.attemptRestartWithRetry(containerName, ctx)
}

// attemptRestartWithRetry handles the restart process with exponential backoff
func (hm *HealthMonitor) attemptRestartWithRetry(containerName string, ctx *workerContext) {
	hm.mutex.Lock()
	ctx.info.State = StateRestarting
	restartAttempt := ctx.info.RestartAttempts + 1
	ctx.info.RestartAttempts = restartAttempt
	maxAttempts := hm.config.MaxRestartAttempts
	hm.mutex.Unlock()

	// Check if we've exceeded max restart attempts
	if restartAttempt > maxAttempts {
		hm.mutex.Lock()
		ctx.info.State = StateDead
		hm.mutex.Unlock()
		log.Printf("[HealthMonitor] Worker %s (%s) marked as DEAD after %d restart attempts",
			containerName, ctx.info.Address, maxAttempts)
		return
	}

	// Calculate backoff time with exponential increase
	backoff := hm.calculateBackoff(restartAttempt)

	log.Printf("[HealthMonitor] Attempting restart %d/%d for worker %s (backoff: %v)",
		restartAttempt, maxAttempts, containerName, backoff)

	// Wait for backoff period (unless it's the first attempt)
	if restartAttempt > 1 {
		select {
		case <-ctx.stopChan:
			return
		case <-time.After(backoff):
		}
	}

	// Attempt the restart
	ctx.lastRestartTime = time.Now()
	restartSuccess := false
	if hm.restartCallback != nil {
		restartSuccess = hm.restartCallback(containerName)
	}

	if !restartSuccess {
		log.Printf("[HealthMonitor] Restart callback failed for worker %s (attempt %d/%d)",
			containerName, restartAttempt, maxAttempts)

		// The next probe failure will trigger another restart attempt
		// We keep the state as Restarting but the failure count will increment
		hm.mutex.Lock()
		ctx.info.FailureCount = 0 // Reset to give the restart attempt a fair chance
		ctx.info.State = StateSuspected
		hm.mutex.Unlock()
		return
	}

	log.Printf("[HealthMonitor] Restart initiated for worker %s (attempt %d/%d)",
		containerName, restartAttempt, maxAttempts)

	// Give the worker time to start up before probing
	select {
	case <-ctx.stopChan:
		return
	case <-time.After(hm.config.PostRestartGrace):
	}

	// Reset failure count to give the restarted worker a fair chance
	hm.mutex.Lock()
	ctx.info.FailureCount = 0
	ctx.info.State = StateSuspected // Will become Healthy on successful probe
	hm.mutex.Unlock()

	log.Printf("[HealthMonitor] Grace period ended for worker %s, resuming probes", containerName)
}

// calculateBackoff returns the backoff duration for a given restart attempt
func (hm *HealthMonitor) calculateBackoff(attempt int) time.Duration {
	if attempt <= 1 {
		return 0
	}

	// Exponential backoff: initial * 2^(attempt-2)
	backoff := hm.config.RestartBackoffInitial
	for i := 2; i < attempt; i++ {
		backoff *= 2
		if backoff > hm.config.RestartBackoffMax {
			backoff = hm.config.RestartBackoffMax
			break
		}
	}
	return backoff
}

func (hm *HealthMonitor) Stop() {
	log.Printf("[HealthMonitor] Stopping health monitor...")

	// Signal all worker goroutines to stop
	for _, ctx := range hm.workers {
		close(ctx.stopChan)
	}

	// Wait for all goroutines to finish
	hm.wg.Wait()
	log.Printf("[HealthMonitor] Stopped")
}

func (hm *HealthMonitor) GetWorkerStatus() map[string]*WorkerInfo {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()

	status := make(map[string]*WorkerInfo)
	for k, ctx := range hm.workers {
		// Return a copy to avoid race conditions
		status[k] = &WorkerInfo{
			ContainerName:   ctx.info.ContainerName,
			Address:         ctx.info.Address,
			LastProbe:       ctx.info.LastProbe,
			State:           ctx.info.State,
			FailureCount:    ctx.info.FailureCount,
			RestartAttempts: ctx.info.RestartAttempts,
		}
	}
	return status
}
