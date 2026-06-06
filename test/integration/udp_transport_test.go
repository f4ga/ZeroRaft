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

package integration

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"

	"zeroraft/internal/codec"
	"zeroraft/internal/raft"
	"zeroraft/internal/transport"
)

// TestRealUDPTransport verifies that three nodes using real UDP sockets
// can elect a leader and replicate a command.
func TestRealUDPTransport(t *testing.T) {
	// Use unique ports to avoid conflicts with parallel tests
	basePort := 9200
	peers := map[int]string{
		1: fmt.Sprintf("127.0.0.1:%d", basePort),
		2: fmt.Sprintf("127.0.0.1:%d", basePort+1),
		3: fmt.Sprintf("127.0.0.1:%d", basePort+2),
	}

	// Create data directories
	dataDirs := make(map[int]string)
	for id := 1; id <= 3; id++ {
		dir, err := os.MkdirTemp("", fmt.Sprintf("zeroraft-udp-%d-*", id))
		if err != nil {
			t.Fatalf("failed to create temp dir for node %d: %v", id, err)
		}
		dataDirs[id] = dir
		t.Cleanup(func() { os.RemoveAll(dir) })
	}

	// Create sockets and nodes
	type nodeInfo struct {
		fd   int
		node *raft.RaftNode
	}
	nodes := make(map[int]*nodeInfo)

	for id := 1; id <= 3; id++ {
		addr := peers[id]
		fd, err := transport.NewRawSocket(addr)
		if err != nil {
			t.Fatalf("failed to create socket for node %d: %v", id, err)
		}
		t.Cleanup(func() { transport.CloseSocket(fd) })

		// resolveAddr helper using net package (allowed in tests)
		resolveAddr := func(addrStr string) (*syscall.SockaddrInet4, error) {
			host, portStr, err := net.SplitHostPort(addrStr)
			if err != nil {
				return nil, err
			}
			port, err := strconv.Atoi(portStr)
			if err != nil {
				return nil, err
			}
			ip := net.ParseIP(host)
			if ip == nil {
				return nil, fmt.Errorf("invalid IP: %s", host)
			}
			ip4 := ip.To4()
			if ip4 == nil {
				return nil, fmt.Errorf("non-IPv4 address: %s", host)
			}
			var addr4 [4]byte
			copy(addr4[:], ip4)
			return &syscall.SockaddrInet4{Port: port, Addr: addr4}, nil
		}

		sendFunc := func(peerAddr string, msg interface{}) error {
			raddr, err := resolveAddr(peerAddr)
			if err != nil {
				return err
			}
			data, err := codec.Encode(msg)
			if err != nil {
				return err
			}
			return transport.SendTo(fd, data, raddr)
		}

		node := raft.NewRaftNode(id, peers, dataDirs[id], sendFunc)
		node.Start()
		t.Cleanup(func() { node.Stop() })

		nodes[id] = &nodeInfo{fd: fd, node: node}
	}

	// Start recv loops for all nodes
	for id, info := range nodes {
		info := info
		go func() {
			for {
				data, from, err := transport.RecvFrom(info.fd)
				if err != nil {
					continue
				}
				fromAddr := fmt.Sprintf("%d.%d.%d.%d:%d", from.Addr[0], from.Addr[1], from.Addr[2], from.Addr[3], from.Port)

				msg, err := codec.Decode(data)
				if err != nil {
					continue
				}

				switch m := msg.(type) {
				case codec.RequestVote:
					resp := info.node.HandleRequestVote(m)
					_ = sendToFd(info.fd, fromAddr, resp)
				case codec.RequestVoteResponse:
					peerID := findPeerIDByAddr(peers, fromAddr)
					info.node.HandleRequestVoteResponse(peerID, m)
				case codec.AppendEntries:
					resp := info.node.HandleAppendEntries(m)
					_ = sendToFd(info.fd, fromAddr, resp)
				case codec.AppendEntriesResponse:
					peerID := findPeerIDByAddr(peers, fromAddr)
					info.node.HandleAppendEntriesResponse(peerID, m)
				}
			}
		}()
		// Suppress unused variable warning
		_ = id
	}

	// Wait for leader election (should happen within 1-2 seconds)
	t.Log("Waiting for leader election...")
	var leaderID int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for id, info := range nodes {
			if info.node.GetState() == raft.Leader {
				leaderID = id
				break
			}
		}
		if leaderID != 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if leaderID == 0 {
		t.Fatal("no leader elected within 5 seconds")
	}
	t.Logf("Leader elected: node %d", leaderID)

	// Submit a command to the leader
	_, err := nodes[leaderID].node.Submit("set foo bar")
	if err != nil {
		t.Fatalf("leader failed to submit command: %v", err)
	}

	// Wait for replication
	time.Sleep(1 * time.Second)

	// Verify all nodes have the value
	for id := 1; id <= 3; id++ {
		val, ok := nodes[id].node.GetStateMachineValue("foo")
		if !ok {
			t.Errorf("node %d: key 'foo' not found in state machine", id)
		} else if val != "bar" {
			t.Errorf("node %d: expected 'bar', got '%s'", id, val)
		}
	}

	t.Log("All nodes have replicated the command successfully")
}

// sendToFd encodes and sends a message to a peer address.
func sendToFd(fd int, peerAddr string, msg interface{}) error {
	raddr, err := resolveAddrSimple(peerAddr)
	if err != nil {
		return err
	}
	data, err := codec.Encode(msg)
	if err != nil {
		return err
	}
	return transport.SendTo(fd, data, raddr)
}

// resolveAddrSimple converts "ip:port" string to *syscall.SockaddrInet4.
func resolveAddrSimple(addrStr string) (*syscall.SockaddrInet4, error) {
	host, portStr, err := net.SplitHostPort(addrStr)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return nil, fmt.Errorf("invalid IP: %s", host)
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return nil, fmt.Errorf("non-IPv4 address: %s", host)
	}
	var addr4 [4]byte
	copy(addr4[:], ip4)
	return &syscall.SockaddrInet4{Port: port, Addr: addr4}, nil
}

// findPeerIDByAddr looks up a peer ID by its address string.
func findPeerIDByAddr(peers map[int]string, addr string) int {
	for id, peerAddr := range peers {
		if peerAddr == addr {
			return id
		}
	}
	return -1
}
