package health_server

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/tp-distribuidos-2c2025/shared/netio"
)

type HealthServer struct {
	port     string
	listener net.Listener
	stopChan chan bool
	wg       sync.WaitGroup
}

func NewHealthServer(port string) *HealthServer {
	return &HealthServer{
		port:     port,
		stopChan: make(chan bool),
	}
}

func (hs *HealthServer) Start() error {
	listener, err := net.Listen("tcp", ":"+hs.port)
	if err != nil {
		return fmt.Errorf("failed to start health server on port %s: %w", hs.port, err)
	}

	hs.listener = listener
	log.Printf("[HealthServer] Listening on port %s", hs.port)

	go hs.acceptConnections()

	return nil
}

func (hs *HealthServer) acceptConnections() {
	for {
		select {
		case <-hs.stopChan:
			return
		default:
			hs.listener.(*net.TCPListener).SetDeadline(time.Now().Add(1 * time.Second))
			conn, err := hs.listener.Accept()
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					continue
				}
				select {
				case <-hs.stopChan:
					return
				default:
					log.Printf("[HealthServer] Error accepting connection: %v", err)
					continue
				}
			}

			hs.wg.Add(1)
			go hs.handleConnection(conn)
		}
	}
}

func (hs *HealthServer) handleConnection(conn net.Conn) {
	defer hs.wg.Done()
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	buf := make([]byte, 4)
	err := netio.ReadFull(conn, buf)
	if err != nil {
		return
	}

	if string(buf) == "PING" {
		conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if writeErr := netio.WriteAll(conn, []byte("PONG")); writeErr != nil {
			log.Printf("[HealthServer] Failed to send PONG: %v", writeErr)
		}
	}
}

func (hs *HealthServer) Stop() {
	close(hs.stopChan)

	if hs.listener != nil {
		hs.listener.Close()
	}

	hs.wg.Wait()
	log.Printf("[HealthServer] Stopped")
}
