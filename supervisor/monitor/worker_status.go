package monitor

import "time"

type WorkerInfo struct {
	ContainerName string
	Address       string
	LastProbe     time.Time
	IsAlive       bool
	FailureCount  int
}

func NewWorkerInfo(containerName, address string) *WorkerInfo {
	return &WorkerInfo{
		ContainerName: containerName,
		Address:       address,
		LastProbe:     time.Time{},
		IsAlive:       true,
		FailureCount:  0,
	}
}

