package supervisor_election

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/protocol/election"
)

type BullyElection struct {
	myID               int
	peers              map[int]string
	isLeader           bool
	leaderID           int
	electionPort       string
	listener           net.Listener
	onBecomeLeader     func()
	onLeaderChange     func(int)
	stopChan           chan bool
	mutex              sync.RWMutex
	wg                 sync.WaitGroup
	electionInProgress bool
}

func NewBullyElection(
	myID int,
	peers map[int]string,
	electionPort string,
	onBecomeLeader func(),
	onLeaderChange func(int),
) (*BullyElection, error) {
	return &BullyElection{
		myID:               myID,
		peers:              peers,
		isLeader:           false,
		leaderID:           -1,
		electionPort:       electionPort,
		onBecomeLeader:     onBecomeLeader,
		onLeaderChange:     onLeaderChange,
		stopChan:           make(chan bool),
		electionInProgress: false,
	}, nil
}

func (be *BullyElection) Start() error {
	listener, err := net.Listen("tcp", ":"+be.electionPort)
	if err != nil {
		return fmt.Errorf("failed to start election listener on port %s: %w", be.electionPort, err)
	}

	be.listener = listener
	log.Printf("[BullyElection] Node %d: Listening for election messages on port %s", be.myID, be.electionPort)

	be.wg.Add(1)              // we use the wait group to wait for the goroutines to finish to handle graceful shutdown
	go be.acceptConnections() // listens for incoming TCP connections from other supervisors

	be.wg.Add(1)
	go be.periodicLeaderCheck() // periodically checks if the leader is still alive

	return nil
}

func (be *BullyElection) acceptConnections() {
	defer be.wg.Done()

	for {
		select {
		case <-be.stopChan:
			return
		default:
			be.listener.(*net.TCPListener).SetDeadline(time.Now().Add(1 * time.Second))
			conn, err := be.listener.Accept()
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					continue
				}
				select {
				case <-be.stopChan:
					return
				default:
					continue
				}
			}

			be.wg.Add(1)
			go be.handleConnection(conn) // one goroutine for each connection with another supervisor
		}
	}
}

func (be *BullyElection) handleConnection(conn net.Conn) {
	defer be.wg.Done()
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}

	rawMsg, err := deserializer.Deserialize(buf[:n])
	if err != nil {
		log.Printf("[BullyElection] Node %d: Failed to deserialize message: %v", be.myID, err)
		return
	}

	msg, ok := rawMsg.(*election.ElectionMessage)
	if !ok {
		log.Printf("[BullyElection] Node %d: Received non-election message type: %T", be.myID, rawMsg)
		return
	}

	switch msg.EventType {
	case election.ElectionStart:
		be.handleElectionStart(msg)
	case election.ElectionOk:
		be.handleElectionOk(msg)
	case election.ElectionLeader:
		be.handleElectionLeader(msg)
	default:
		log.Printf("[BullyElection] Node %d: Unknown election event type: %d", be.myID, msg.EventType)
	}
}

func (be *BullyElection) handleElectionStart(msg *election.ElectionMessage) {
	log.Printf("[BullyElection] Node %d: Received ELECTION from %d", be.myID, msg.SupervisorID)

	if msg.SupervisorID < be.myID {
		be.sendOkMessage(msg.SupervisorID)
		go be.StartElection()
	}
}

func (be *BullyElection) handleElectionOk(msg *election.ElectionMessage) {
	log.Printf("[BullyElection] Node %d: Received OK from %d", be.myID, msg.SupervisorID)
}

func (be *BullyElection) handleElectionLeader(msg *election.ElectionMessage) {
	log.Printf("[BullyElection] Node %d: Received LEADER announcement from %d", be.myID, msg.SupervisorID)

	be.mutex.Lock()
	wasLeader := be.isLeader
	be.isLeader = false
	be.leaderID = msg.SupervisorID
	be.electionInProgress = false
	be.mutex.Unlock()

	if wasLeader && be.onLeaderChange != nil {
		be.onLeaderChange(msg.SupervisorID)
	}
}

func (be *BullyElection) StartElection() {
	be.mutex.Lock()
	if be.electionInProgress {
		be.mutex.Unlock()
		return
	}
	be.electionInProgress = true
	be.mutex.Unlock()

	log.Printf("[BullyElection] Node %d: Starting election", be.myID)

	higherNodes := be.getHigherNodes()

	if len(higherNodes) == 0 {
		be.becomeLeader()
		return
	}

	receivedOK := false
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, nodeID := range higherNodes {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if be.sendElectionMessage(id) {
				mu.Lock()
				receivedOK = true
				mu.Unlock()
			}
		}(nodeID)
	}

	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}

	mu.Lock()
	gotOK := receivedOK
	mu.Unlock()

	if !gotOK {
		be.becomeLeader()
	} else {
		time.Sleep(5 * time.Second)

		be.mutex.RLock()
		stillNoLeader := be.leaderID == -1 || be.leaderID == be.myID
		be.mutex.RUnlock()

		if stillNoLeader {
			be.becomeLeader()
		}
	}
}

func (be *BullyElection) becomeLeader() {
	be.mutex.Lock()
	wasLeader := be.isLeader
	be.isLeader = true
	be.leaderID = be.myID
	be.electionInProgress = false
	be.mutex.Unlock()

	log.Printf("[BullyElection] Node %d: I am now the LEADER", be.myID)

	be.broadcastLeader()

	if !wasLeader && be.onBecomeLeader != nil {
		be.onBecomeLeader()
	}
}

func (be *BullyElection) broadcastLeader() {
	log.Printf("[BullyElection] Node %d: Broadcasting LEADER announcement", be.myID)

	for nodeID := range be.peers {
		if nodeID != be.myID {
			go be.sendLeaderMessage(nodeID)
		}
	}
}

func (be *BullyElection) sendElectionMessage(targetID int) bool {
	addr, exists := be.peers[targetID]
	if !exists {
		return false
	}

	timestamp := time.Now().Unix()
	msg := election.NewElectionMessage(election.ElectionStart, be.myID, timestamp)

	data, err := election.SerializeElectionMessage(msg)
	if err != nil {
		return false
	}

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Write(data)
	if err != nil {
		return false
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err == nil && n > 0 {
		rawMsg, err := deserializer.Deserialize(buf[:n])
		if err == nil {
			if respMsg, ok := rawMsg.(*election.ElectionMessage); ok && respMsg.IsOk() {
				return true
			}
		}
	}

	return false
}

func (be *BullyElection) sendOkMessage(targetID int) {
	addr, exists := be.peers[targetID]
	if !exists {
		return
	}

	timestamp := time.Now().Unix()
	msg := election.NewElectionMessage(election.ElectionOk, be.myID, timestamp)

	data, err := election.SerializeElectionMessage(msg)
	if err != nil {
		return
	}

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	conn.Write(data)
}

func (be *BullyElection) sendLeaderMessage(targetID int) {
	addr, exists := be.peers[targetID]
	if !exists {
		return
	}

	timestamp := time.Now().Unix()
	msg := election.NewElectionMessage(election.ElectionLeader, be.myID, timestamp)

	data, err := election.SerializeElectionMessage(msg)
	if err != nil {
		return
	}

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	conn.Write(data)
}

func (be *BullyElection) getHigherNodes() []int {
	var higher []int
	for nodeID := range be.peers {
		if nodeID > be.myID {
			higher = append(higher, nodeID)
		}
	}
	return higher
}

func (be *BullyElection) periodicLeaderCheck() {
	defer be.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			be.mutex.RLock()
			isLeader := be.isLeader
			leaderID := be.leaderID
			be.mutex.RUnlock()

			if isLeader {
				be.broadcastLeader()
			} else if leaderID == -1 {
				log.Printf("[BullyElection] Node %d: No leader detected, starting election", be.myID)
				go be.StartElection()
			}

		case <-be.stopChan:
			return
		}
	}
}

func (be *BullyElection) IsLeader() bool {
	be.mutex.RLock()
	defer be.mutex.RUnlock()
	return be.isLeader
}

func (be *BullyElection) GetLeaderID() int {
	be.mutex.RLock()
	defer be.mutex.RUnlock()
	return be.leaderID
}

func (be *BullyElection) Stop() {
	close(be.stopChan)

	if be.listener != nil {
		be.listener.Close()
	}

	be.wg.Wait()
	log.Printf("[BullyElection] Node %d: Stopped", be.myID)
}
