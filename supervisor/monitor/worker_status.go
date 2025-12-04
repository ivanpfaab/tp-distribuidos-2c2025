package monitor

import "time"

// WorkerState represents the current state of a worker
type WorkerState int

const (
	// StateHealthy means the worker is responding to probes
	StateHealthy WorkerState = iota
	// StateSuspected means the worker has failed some probes but not enough to trigger restart
	StateSuspected
	// StateRestarting means a restart has been triggered and we're waiting for it to recover
	StateRestarting
	// StateDead means the worker has exceeded max restart attempts
	StateDead
)

func (s WorkerState) String() string {
	switch s {
	case StateHealthy:
		return "HEALTHY"
	case StateSuspected:
		return "SUSPECTED"
	case StateRestarting:
		return "RESTARTING"
	case StateDead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}

// WorkerInfo holds the status information for a worker
type WorkerInfo struct {
	ContainerName   string
	Address         string
	LastProbe       time.Time
	State           WorkerState
	FailureCount    int
	RestartAttempts int
}

// workerContext holds the internal mutable state for a worker's monitoring goroutine
// This is separate from WorkerInfo to allow safe concurrent access
type workerContext struct {
	info            *WorkerInfo
	stopChan        chan struct{}
	lastRestartTime time.Time
}

func NewWorkerInfo(containerName, address string) *WorkerInfo {
	return &WorkerInfo{
		ContainerName:   containerName,
		Address:         address,
		LastProbe:       time.Time{},
		State:           StateHealthy,
		FailureCount:    0,
		RestartAttempts: 0,
	}
}

// IsAlive returns true if the worker is in a healthy or suspected state
// (i.e., not confirmed dead or currently restarting)
func (w *WorkerInfo) IsAlive() bool {
	return w.State == StateHealthy || w.State == StateSuspected
}
