// Copyright 2026 Ekaterina Godulyan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transport

import (
	"fmt"
	"syscall"
	"testing"
	"time"
)

func TestNewRawSocket(t *testing.T) {
	fd, err := NewRawSocket("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRawSocket failed: %v", err)
	}
	if fd <= 0 {
		t.Errorf("expected fd > 0, got %d", fd)
	}
	_ = CloseSocket(fd)
}

func TestNewRawSocketInvalidAddr(t *testing.T) {
	_, err := NewRawSocket("invalid")
	if err == nil {
		t.Error("expected error for invalid address")
	}
}

func TestSendAndReceive(t *testing.T) {
	// Create two sockets on random ports
	fd1, err := NewRawSocket("127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create socket 1: %v", err)
	}
	defer func() { _ = CloseSocket(fd1) }()

	fd2, err := NewRawSocket("127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create socket 2: %v", err)
	}
	defer func() { _ = CloseSocket(fd2) }()

	// Get address of fd2
	addr2, err := getSockAddr(fd2)
	if err != nil {
		t.Fatalf("failed to get socket address: %v", err)
	}

	// Send "hello" from fd1 to fd2
	msg := []byte("hello")
	err = SendTo(fd1, msg, addr2)
	if err != nil {
		t.Fatalf("SendTo failed: %v", err)
	}

	// Receive with timeout
	recvCh := make(chan []byte)
	errCh := make(chan error)
	go func() {
		data, _, err := RecvFrom(fd2)
		if err != nil {
			errCh <- err
			return
		}
		recvCh <- data
	}()

	select {
	case data := <-recvCh:
		if string(data) != "hello" {
			t.Errorf("expected 'hello', got %q", string(data))
		}
	case err := <-errCh:
		t.Fatalf("RecvFrom failed: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for receive")
	}
}

func TestSendToClosedSocket(t *testing.T) {
	fd, err := NewRawSocket("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRawSocket failed: %v", err)
	}
	_ = CloseSocket(fd)

	// Try to send to closed socket
	addr := &syscall.SockaddrInet4{Port: 1234, Addr: [4]byte{127, 0, 0, 1}}
	err = SendTo(fd, []byte("test"), addr)
	if err == nil {
		t.Error("expected error when sending to closed socket")
	}
}

func TestRecvFromWithCancel(t *testing.T) {
	fd, err := NewRawSocket("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRawSocket failed: %v", err)
	}
	defer func() { _ = CloseSocket(fd) }()

	// Get socket address
	addr, err := getSockAddr(fd)
	if err != nil {
		t.Fatalf("failed to get socket address: %v", err)
	}

	// Start recv in goroutine
	dataCh := make(chan []byte)
	errCh := make(chan error)
	go func() {
		data, _, err := RecvFrom(fd)
		if err != nil {
			errCh <- err
			return
		}
		dataCh <- data
	}()

	// Send a packet to ourselves
	senderFd, err := NewRawSocket("127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create sender socket: %v", err)
	}
	defer func() { _ = CloseSocket(senderFd) }()

	msg := []byte("ping")
	err = SendTo(senderFd, msg, addr)
	if err != nil {
		t.Fatalf("SendTo failed: %v", err)
	}

	// Wait for receive
	select {
	case data := <-dataCh:
		if string(data) != "ping" {
			t.Errorf("expected 'ping', got %q", string(data))
		}
	case err := <-errCh:
		t.Fatalf("RecvFrom failed: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for RecvFrom")
	}
}

func TestConcurrentSendReceive(t *testing.T) {
	fd1, err := NewRawSocket("127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create socket 1: %v", err)
	}
	defer func() { _ = CloseSocket(fd1) }()

	fd2, err := NewRawSocket("127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create socket 2: %v", err)
	}
	defer func() { _ = CloseSocket(fd2) }()

	addr2, err := getSockAddr(fd2)
	if err != nil {
		t.Fatalf("failed to get socket address: %v", err)
	}

	const workers = 10
	done := make(chan bool, workers)
	for i := 0; i < workers; i++ {
		go func(id int, addr syscall.SockaddrInet4) {
			msg := []byte{byte(id)}
			err := SendTo(fd1, msg, &addr)
			if err != nil {
				t.Errorf("worker %d SendTo failed: %v", id, err)
			}
			done <- true
		}(i, *addr2)
	}

	// Receive all messages
	received := make(map[byte]bool)
	for i := 0; i < workers; i++ {
		data, _, err := RecvFrom(fd2)
		if err != nil {
			t.Fatalf("RecvFrom failed: %v", err)
		}
		received[data[0]] = true
	}

	for i := 0; i < workers; i++ {
		<-done
	}

	if len(received) != workers {
		t.Errorf("expected %d unique messages, got %d", workers, len(received))
	}
}

// Helper to get socket address (not exported)
func getSockAddr(fd int) (*syscall.SockaddrInet4, error) {
	addr, err := syscall.Getsockname(fd)
	if err != nil {
		return nil, err
	}
	inet4, ok := addr.(*syscall.SockaddrInet4)
	if !ok {
		return nil, fmt.Errorf("unexpected socket address type: %T", addr)
	}
	return inet4, nil
}
