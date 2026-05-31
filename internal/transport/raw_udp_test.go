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
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestNewRawSocketValidPort(t *testing.T) {
	fd, err := NewRawSocket("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRawSocket failed: %v", err)
	}
	if fd <= 0 {
		t.Errorf("expected fd > 0, got %d", fd)
	}
	defer func() { _ = CloseSocket(fd) }()

	addr, err := getSockAddr(fd)
	if err != nil {
		t.Fatalf("failed to get socket address: %v", err)
	}
	if addr.Port == 0 {
		t.Error("expected non-zero port for :0 binding")
	}
}

func TestNewRawSocketSpecificPort(t *testing.T) {
	fd, err := NewRawSocket("127.0.0.1:18001")
	if err != nil {
		t.Fatalf("NewRawSocket failed: %v", err)
	}
	defer func() { _ = CloseSocket(fd) }()

	addr, err := getSockAddr(fd)
	if err != nil {
		t.Fatalf("failed to get socket address: %v", err)
	}
	if addr.Port != 18001 {
		t.Errorf("expected port 18001, got %d", addr.Port)
	}
}

func TestNewRawSocketInvalidAddr(t *testing.T) {
	invalidAddrs := []string{
		"",
		"invalid",
		"127.0.0.1",
		"127.0.0.1:99999",
		"999.999.999.999:8000",
	}

	for _, addr := range invalidAddrs {
		t.Run(addr, func(t *testing.T) {
			fd, err := NewRawSocket(addr)
			if err == nil {
				_ = CloseSocket(fd)
				t.Errorf("expected error for addr %q", addr)
			}
		})
	}
}

func TestSendAndReceiveBasic(t *testing.T) {
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

	testCases := []string{"hello", "world", "single", "test message"}

	for _, expected := range testCases {
		t.Run(expected, func(t *testing.T) {
			err = SendTo(fd1, []byte(expected), addr2)
			if err != nil {
				t.Fatalf("SendTo failed: %v", err)
			}

			received, _, err := recvWithTimeout(fd2, 1*time.Second)
			if err != nil {
				t.Fatalf("RecvFrom failed: %v", err)
			}

			if string(received) != expected {
				t.Errorf("expected %q, got %q", expected, string(received))
			}
		})
	}
}

func TestSendToClosedSocket(t *testing.T) {
	fd, err := NewRawSocket("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRawSocket failed: %v", err)
	}

	addr := &syscall.SockaddrInet4{Port: 1234, Addr: [4]byte{127, 0, 0, 1}}

	if err := CloseSocket(fd); err != nil {
		t.Fatalf("CloseSocket failed: %v", err)
	}

	err = SendTo(fd, []byte("test"), addr)
	if err == nil {
		t.Error("expected error when sending to closed socket")
	}
}

func TestRecvFromClosedSocket(t *testing.T) {
	fd, err := NewRawSocket("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRawSocket failed: %v", err)
	}

	if err := CloseSocket(fd); err != nil {
		t.Fatalf("CloseSocket failed: %v", err)
	}

	_, _, err = RecvFrom(fd)
	if err == nil {
		t.Error("expected error when receiving from closed socket")
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

	const numWorkers = 10
	var wg sync.WaitGroup
	received := make(chan []byte, numWorkers)

	go func() {
		for i := 0; i < numWorkers; i++ {
			data, _, err := recvWithTimeout(fd2, 2*time.Second)
			if err != nil {
				t.Errorf("RecvFrom failed: %v", err)
				return
			}
			received <- data
		}
		close(received)
	}()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			msg := []byte(fmt.Sprintf("msg-%d", id))
			addrCopy := *addr2
			err := SendTo(fd1, msg, &addrCopy)
			if err != nil {
				t.Errorf("SendTo failed for worker %d: %v", id, err)
			}
		}(i)
	}

	wg.Wait()

	count := 0
	for range received {
		count++
	}

	if count != numWorkers {
		t.Errorf("expected %d messages, got %d", numWorkers, count)
	}
}

func TestMultipleSendReceive(t *testing.T) {
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

	const numMessages = 20
	for i := 0; i < numMessages; i++ {
		msg := []byte(fmt.Sprintf("msg-%d", i))

		err = SendTo(fd1, msg, addr2)
		if err != nil {
			t.Fatalf("SendTo failed at iteration %d: %v", i, err)
		}

		received, _, err := recvWithTimeout(fd2, 1*time.Second)
		if err != nil {
			t.Fatalf("RecvFrom failed at iteration %d: %v", i, err)
		}

		if string(received) != string(msg) {
			t.Errorf("iteration %d: expected %q, got %q", i, msg, received)
		}
	}
}

func TestZeroPortBinding(t *testing.T) {
	fd, err := NewRawSocket("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRawSocket failed: %v", err)
	}
	defer func() { _ = CloseSocket(fd) }()

	addr, err := getSockAddr(fd)
	if err != nil {
		t.Fatalf("failed to get socket address: %v", err)
	}

	if addr.Port == 0 {
		t.Error("expected non-zero port when binding to :0")
	}
}

func TestNewRawUDP(t *testing.T) {
	udp, err := NewRawUDP("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRawUDP failed: %v", err)
	}
	defer func() { _ = udp.Close() }()

	if udp.GetFD() <= 0 {
		t.Error("expected valid file descriptor")
	}
}

func recvWithTimeout(fd int, timeout time.Duration) ([]byte, *syscall.SockaddrInet4, error) {
	type result struct {
		data []byte
		addr *syscall.SockaddrInet4
		err  error
	}

	resultCh := make(chan result, 1)
	go func() {
		data, addr, err := RecvFrom(fd)
		resultCh <- result{data, addr, err}
	}()

	select {
	case res := <-resultCh:
		return res.data, res.addr, res.err
	case <-time.After(timeout):
		return nil, nil, fmt.Errorf("recv timeout after %v", timeout)
	}
}

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

// Добавить в конец файла:

func TestRawUDPSend(t *testing.T) {
	udp, err := NewRawUDP("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRawUDP failed: %v", err)
	}
	defer udp.Close()

	addr := &syscall.SockaddrInet4{Port: 12345, Addr: [4]byte{127, 0, 0, 1}}
	err = udp.Send([]byte("test"), addr)
	if err != nil {
		t.Errorf("Send failed: %v", err)
	}
}

func TestRawUDPReceive(t *testing.T) {
	udp1, err := NewRawUDP("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRawUDP failed: %v", err)
	}
	defer udp1.Close()

	udp2, err := NewRawUDP("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRawUDP failed: %v", err)
	}
	defer udp2.Close()

	addr2, err := getSockAddr(udp2.GetFD())
	if err != nil {
		t.Fatalf("failed to get address: %v", err)
	}

	err = udp1.Send([]byte("ping"), addr2)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	data, _, err := udp2.Receive()
	if err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	if string(data) != "ping" {
		t.Errorf("expected 'ping', got %s", data)
	}
}
