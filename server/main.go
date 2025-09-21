package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
)

func writeAll(c net.Conn, p []byte) error {
	for len(p) > 0 {
		n, err := c.Write(p)
		if err != nil {
			// On error, we consider the response failed; return the error.
			return err
		}
		p = p[n:]
	}
	return nil
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	fmt.Printf("Client connected: %s\n", conn.RemoteAddr())

	sc := bufio.NewScanner(conn)

	for sc.Scan() {
		message := sc.Text()
		fmt.Printf("Received: %s\n", message)

		response := "Echo: " + message + "\n"
		if err := writeAll(conn, []byte(response)); err != nil {
			log.Printf("Write error to %s: %v", conn.RemoteAddr(), err)
			return // close conn; client gets a broken pipe rather than a truncated line
		}

		if strings.EqualFold(strings.TrimSpace(message), "exit") {
			fmt.Printf("Client %s disconnected\n", conn.RemoteAddr())
			return
		}

	}
	if err := sc.Err(); err != nil && err != io.EOF {
		log.Printf("Error reading from %s: %v", conn.RemoteAddr(), err)
	}
}

func main() {
	port := "8080"
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	defer ln.Close()

	fmt.Printf("Echo server listening on port %s\n", port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go handleConnection(conn)
	}
}
