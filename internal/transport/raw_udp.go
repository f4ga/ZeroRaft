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
	"errors"
	"fmt"
	"net"
	"strconv"
	"syscall"
)

// Constants for buffer size and length prefix
const (
	udpBufferSize = 65535 // maximum UDP datagram size
)

// RawUDP представляет сырой UDP-сокет для Raft-коммуникаций.
type RawUDP struct{}

// NewRawUDP создаёт новый экземпляр RawUDP.
func NewRawUDP() *RawUDP {
	return &RawUDP{}
}

// Send отправляет данные по указанному адресу.
func (r *RawUDP) Send(addr string, data []byte) error {
	return errors.New("not implemented")
}

// Receive принимает данные из сокета.
func (r *RawUDP) Receive() ([]byte, string, error) {
	return nil, "", errors.New("not implemented")
}

// NewRawSocket создаёт UDP сокет на указанном адресе (формат "127.0.0.1:8001").
// Возвращает файловый дескриптор (int) и ошибку.
// Запрещено использовать net.ResolveUDPAddr, net.ListenPacket. Разрешен net.SplitHostPort и net.ParseIP.
func NewRawSocket(addr string) (int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return -1, fmt.Errorf("invalid address %q: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return -1, fmt.Errorf("invalid port %q: %w", portStr, err)
	}
	ip := net.ParseIP(host).To4()
	if ip == nil {
		return -1, fmt.Errorf("invalid IPv4 address %q", host)
	}

	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_UDP)
	if err != nil {
		return -1, fmt.Errorf("socket creation failed: %w", err)
	}

	// Allow reuse address to avoid "address already in use" in tests
	err = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	if err != nil {
		_ = syscall.Close(fd)
		return -1, fmt.Errorf("setsockopt SO_REUSEADDR failed: %w", err)
	}

	var sockaddr [4]byte
	copy(sockaddr[:], ip)
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Port: port, Addr: sockaddr}); err != nil {
		_ = syscall.Close(fd)
		return -1, fmt.Errorf("bind failed: %w", err)
	}
	return fd, nil
}

// RecvFrom читает данные из сокета. Возвращает байты и адрес отправителя (syscall.SockaddrInet4).
// Если таймаут не нужен, просто блокируется до получения данных.
func RecvFrom(fd int) ([]byte, *syscall.SockaddrInet4, error) {
	buf := make([]byte, udpBufferSize)
	for {
		n, from, err := syscall.Recvfrom(fd, buf, 0)
		if err == nil {
			fromInet4, ok := from.(*syscall.SockaddrInet4)
			if !ok {
				return nil, nil, fmt.Errorf("unexpected socket address type: %T", from)
			}
			return buf[:n], fromInet4, nil
		}

		// Retry on temporary errors: EAGAIN (resource temporarily unavailable)
		// and EINTR (system call interrupted by signal)
		if err == syscall.EAGAIN || err == syscall.EINTR {
			continue
		}
		return nil, nil, fmt.Errorf("recvfrom failed: %w", err)
	}
}

// SendTo отправляет данные на указанный адрес.
func SendTo(fd int, data []byte, addr *syscall.SockaddrInet4) error {
	// Retry once on temporary errors: EAGAIN (resource temporarily unavailable)
	// and EINTR (system call interrupted by signal)
	err := syscall.Sendto(fd, data, 0, addr)
	if err == nil {
		return nil
	}
	if err == syscall.EAGAIN || err == syscall.EINTR {
		err = syscall.Sendto(fd, data, 0, addr)
		if err != nil {
			return fmt.Errorf("sendto failed after retry: %w", err)
		}
		return nil
	}
	return fmt.Errorf("sendto failed: %w", err)
}

// CloseSocket закрывает сокет.
func CloseSocket(fd int) error {
	return syscall.Close(fd)
}
