package network

import (
	"log"
	"net"
	"sync"
)

// ConnectionManager manages client connections in a thread-safe manner
type ConnectionManager struct {
	connections     map[string]net.Conn // clientID -> connection
	clientIDsByConn map[net.Conn]string // connection -> clientID (for efficient reverse lookup)
	mutex           sync.RWMutex
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		connections:     make(map[string]net.Conn),
		clientIDsByConn: make(map[net.Conn]string),
	}
}

// Store stores a connection for a given client ID
func (cm *ConnectionManager) Store(clientID string, conn net.Conn) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	cm.connections[clientID] = conn
	cm.clientIDsByConn[conn] = clientID

	log.Printf("Connection Manager: Stored connection for client %s", clientID)
}

// Get retrieves a connection for a given client ID
func (cm *ConnectionManager) Get(clientID string) (net.Conn, bool) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	conn, exists := cm.connections[clientID]
	return conn, exists
}

// Remove removes a connection (finds client ID from connection)
func (cm *ConnectionManager) Remove(conn net.Conn) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	clientID, exists := cm.clientIDsByConn[conn]
	if exists {
		delete(cm.connections, clientID)
		delete(cm.clientIDsByConn, conn)
		log.Printf("Connection Manager: Removed connection for client %s", clientID)
	}
}

// RemoveByID removes a connection by client ID
func (cm *ConnectionManager) RemoveByID(clientID string) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	conn, exists := cm.connections[clientID]
	if exists {
		delete(cm.connections, clientID)
		delete(cm.clientIDsByConn, conn)
		log.Printf("Connection Manager: Removed connection for client %s", clientID)
	}
}

// Close closes a connection and removes it from the manager
func (cm *ConnectionManager) Close(clientID string) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	conn, exists := cm.connections[clientID]
	if exists {
		conn.Close()
		delete(cm.connections, clientID)
		delete(cm.clientIDsByConn, conn)
		log.Printf("Connection Manager: Closed and removed connection for client %s", clientID)
	}
}

