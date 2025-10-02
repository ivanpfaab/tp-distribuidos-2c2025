package main

import (
	"fmt"
	"sync"
)

// EOSMessage represents an End-Of-Signal message for a specific query
type EOSMessage struct {
	QueryType byte
	ClientID  string
	Step      int
}

// EOSTransmitter broadcasts EOS signals to all workers
type EOSTransmitter struct {
	signalIn       chan EOSMessage
	workerChannels []chan EOSMessage
	running        bool
	mu             sync.Mutex
}

// NewEOSTransmitter creates a new EOS transmitter
func NewEOSTransmitter(signalIn chan EOSMessage, numWorkers int) *EOSTransmitter {
	// Create channels for each worker
	workerChannels := make([]chan EOSMessage, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workerChannels[i] = make(chan EOSMessage, 1) // Buffered to prevent blocking
	}

	return &EOSTransmitter{
		signalIn:       signalIn,
		workerChannels: workerChannels,
		running:        false,
	}
}

// GetWorkerChannel returns the EOS channel for a specific worker
func (eos *EOSTransmitter) GetWorkerChannel(workerID int) <-chan EOSMessage {
	if workerID < 0 || workerID >= len(eos.workerChannels) {
		return nil
	}
	return eos.workerChannels[workerID]
}

// Start starts the EOS transmitter
func (eos *EOSTransmitter) Start() {
	eos.mu.Lock()
	eos.running = true
	eos.mu.Unlock()

	fmt.Printf("\033[35m[EOS TRANSMITTER] Started, listening for EOS signals\033[0m\n")

	for eos.running {
		select {
		case eosMsg, ok := <-eos.signalIn:
			if !ok {
				fmt.Printf("\033[35m[EOS TRANSMITTER] Signal channel closed, stopping\033[0m\n")
				eos.running = false
				break
			}

			fmt.Printf("\033[35m[EOS TRANSMITTER] Received EOS signal - QueryType: %d, ClientID: %s, Step: %d\033[0m\n",
				eosMsg.QueryType, eosMsg.ClientID, eosMsg.Step)

			// Broadcast to all workers
			for i, workerChannel := range eos.workerChannels {
				select {
				case workerChannel <- eosMsg:
					fmt.Printf("\033[35m[EOS TRANSMITTER] Sent EOS signal to worker %d\033[0m\n", i)
				default:
					fmt.Printf("\033[35m[EOS TRANSMITTER] Worker %d channel full, skipping\033[0m\n", i)
				}
			}
		}
	}

	// Close all worker channels
	for i, workerChannel := range eos.workerChannels {
		close(workerChannel)
		fmt.Printf("\033[35m[EOS TRANSMITTER] Closed channel for worker %d\033[0m\n", i)
	}

	fmt.Printf("\033[35m[EOS TRANSMITTER] Stopped\033[0m\n")
}

// SendEOSSignal allows workers to send EOS signals directly
func (eos *EOSTransmitter) SendEOSSignal(eosMsg EOSMessage) {
	select {
	case eos.signalIn <- eosMsg:
		fmt.Printf("\033[35m[EOS TRANSMITTER] Received EOS signal from worker - QueryType: %d, ClientID: %s, Step: %d\033[0m\n",
			eosMsg.QueryType, eosMsg.ClientID, eosMsg.Step)
	default:
		fmt.Printf("\033[35m[EOS TRANSMITTER] WARNING - Could not send EOS signal to transmitter\033[0m\n")
	}
}

// Stop stops the EOS transmitter
func (eos *EOSTransmitter) Stop() {
	eos.mu.Lock()
	eos.running = false
	eos.mu.Unlock()
}
