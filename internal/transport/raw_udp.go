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
	"net"
	"strconv"
	"syscall"
)

func NewRawSocket(addr string) (int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return -1, fmt.Errorf("invalid address format: %v", err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return -1, fmt.Errorf("invalid port: %s", portStr)
	}

	if port < 0 || port > 65535 {
		return -1, fmt.Errorf("port out of range: %d", port)
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return -1, fmt.Errorf("invalid IP address: %s", host)
	}

	ip4 := ip.To4()
	if ip4 == nil {
		return -1, fmt.Errorf("non-IPv4 address: %s", host)
	}

	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_UDP)
	if err != nil {
		return -1, fmt.Errorf("socket creation failed: %v", err)
	}

	var addr4 [4]byte
	copy(addr4[:], ip4)

	sockaddr := &syscall.SockaddrInet4{
		Port: port,
		Addr: addr4,
	}

	if err := syscall.Bind(fd, sockaddr); err != nil {
		_ = syscall.Close(fd)
		return -1, fmt.Errorf("bind failed: %v", err)
	}

	return fd, nil
}

func RecvFrom(fd int) ([]byte, *syscall.SockaddrInet4, error) {
	buf := make([]byte, 65536)

	n, from, err := syscall.Recvfrom(fd, buf, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("recvfrom failed: %v", err)
	}

	inet4, ok := from.(*syscall.SockaddrInet4)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected address type: %T", from)
	}

	return buf[:n], inet4, nil
}

func SendTo(fd int, data []byte, addr *syscall.SockaddrInet4) error {
	if fd <= 0 {
		return fmt.Errorf("invalid file descriptor: %d", fd)
	}

	err := syscall.Sendto(fd, data, 0, addr)
	if err != nil {
		return fmt.Errorf("sendto failed: %v", err)
	}

	return nil
}

func CloseSocket(fd int) error {
	if fd <= 0 {
		return nil
	}
	return syscall.Close(fd)
}

type RawUDP struct {
	fd   int
	addr string
}

func NewRawUDP(addr string) (*RawUDP, error) {
	fd, err := NewRawSocket(addr)
	if err != nil {
		return nil, err
	}
	return &RawUDP{fd: fd, addr: addr}, nil
}

func (r *RawUDP) Send(data []byte, addr *syscall.SockaddrInet4) error {
	return SendTo(r.fd, data, addr)
}

func (r *RawUDP) Receive() ([]byte, *syscall.SockaddrInet4, error) {
	return RecvFrom(r.fd)
}

func (r *RawUDP) Close() error {
	return CloseSocket(r.fd)
}

func (r *RawUDP) GetFD() int {
	return r.fd
}
