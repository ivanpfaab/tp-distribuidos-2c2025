package netio

import (
	"fmt"
	"io"
	"net"
)

// WriteAll writes all bytes from data to the connection, handling short writes.
// It loops until all bytes are written or an error occurs.
func WriteAll(conn net.Conn, data []byte) error {
	totalWritten := 0
	for totalWritten < len(data) {
		n, err := conn.Write(data[totalWritten:])
		if err != nil {
			return fmt.Errorf("write failed after %d/%d bytes: %w", totalWritten, len(data), err)
		}
		totalWritten += n
	}
	return nil
}

// ReadFull reads exactly len(buf) bytes from the connection, handling short reads.
// It loops until all bytes are read or an error occurs.
func ReadFull(conn net.Conn, buf []byte) error {
	totalRead := 0
	for totalRead < len(buf) {
		n, err := conn.Read(buf[totalRead:])
		if err != nil {
			if err == io.EOF && totalRead > 0 {
				return fmt.Errorf("unexpected EOF after %d/%d bytes", totalRead, len(buf))
			}
			return fmt.Errorf("read failed after %d/%d bytes: %w", totalRead, len(buf), err)
		}
		totalRead += n
	}
	return nil
}
