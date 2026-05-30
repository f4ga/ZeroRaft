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
	"sync"
	"testing"
	"time"

	"zeroraft/internal/raft"
	"zeroraft/internal/transport"
)

type message struct {
	fromID int
	toAddr string
	msg    interface{}
}

// Router simulates network delivery between nodes.
type Router struct {
	mu        sync.Mutex
	nodes     map[int]*raft.RaftNode
	addresses map[int]string
	inbox     chan message
	stopCh    chan struct{}
}

// NewRouter creates a new router with async message delivery.
func NewRouter() *Router {
	r := &Router{
		nodes:     make(map[int]*raft.RaftNode),
		addresses: map[int]string{1: "node1", 2: "node2", 3: "node3"},
		inbox:     make(chan message, 100),
		stopCh:    make(chan struct{}),
	}
	go r.deliveryLoop()
	return r
}

// Register adds a node to the router.
func (r *Router) Register(id int, node *raft.RaftNode) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes[id] = node
}

// Send delivers a message to the target address (called by RaftNode).
func (r *Router) Send(addr string, msg interface{}) error {
	// Find sender ID (ugly but works for test)
	var fromID int
	r.mu.Lock()
	for id := range r.nodes {
		fromID = id
		break
	}
	r.mu.Unlock()

	r.inbox <- message{fromID: fromID, toAddr: addr, msg: msg}
	return nil
}

func (r *Router) deliveryLoop() {
	for {
		select {
		case msg := <-r.inbox:
			r.deliver(msg)
		case <-r.stopCh:
			return
		}
	}
}

func (r *Router) deliver(msg message) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var targetID int
	switch msg.toAddr {
	case "node1":
		targetID = 1
	case "node2":
		targetID = 2
	case "node3":
		targetID = 3
	default:
		return
	}

	target, ok := r.nodes[targetID]
	if !ok {
		return
	}

	switch m := msg.msg.(type) {
	case transport.RequestVote:
		resp := target.HandleRequestVote(m)
		senderAddr := r.addresses[msg.fromID]
		go func() { _ = r.Send(senderAddr, resp) }()
	case transport.RequestVoteResponse:
		target.HandleRequestVoteResponse(msg.fromID, m)
	case transport.AppendEntries:
		resp := target.HandleAppendEntries(m)
		senderAddr := r.addresses[msg.fromID]
		go func() { _ = r.Send(senderAddr, resp) }()
	case transport.AppendEntriesResponse:
		target.HandleAppendEntriesResponse(msg.fromID, m)
	}
}

// Stop shuts down the router.
func (r *Router) Stop() {
	close(r.stopCh)
}

// TestClusterElectionAndReplication tests a 3-node cluster.
func TestClusterElectionAndReplication(t *testing.T) {
	router := NewRouter()
	defer router.Stop()

	peers := map[int]string{
		1: "node1",
		2: "node2",
		3: "node3",
	}

	node1 := raft.NewRaftNode(1, peers, t.TempDir(), router.Send)
	node2 := raft.NewRaftNode(2, peers, t.TempDir(), router.Send)
	node3 := raft.NewRaftNode(3, peers, t.TempDir(), router.Send)

	router.Register(1, node1)
	router.Register(2, node2)
	router.Register(3, node3)

	node1.Start()
	node2.Start()
	node3.Start()
	defer node1.Stop()
	defer node2.Stop()
	defer node3.Stop()

	// Wait for leader election
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	var leaderID int
	for {
		select {
		case <-timeout:
			t.Logf("Node1: state=%s, term=%d, leader=%d", node1.GetState(), node1.GetCurrentTerm(), node1.GetLeaderID())
			t.Logf("Node2: state=%s, term=%d, leader=%d", node2.GetState(), node2.GetCurrentTerm(), node2.GetLeaderID())
			t.Logf("Node3: state=%s, term=%d, leader=%d", node3.GetState(), node3.GetCurrentTerm(), node3.GetLeaderID())
			t.Fatal("no leader elected")
		case <-ticker.C:
			switch {
			case node1.GetState() == raft.Leader:
				leaderID = 1
			case node2.GetState() == raft.Leader:
				leaderID = 2
			case node3.GetState() == raft.Leader:
				leaderID = 3
			}
			if leaderID != 0 {
				t.Logf("✓ Leader elected: node %d", leaderID)
				goto leaderElected
			}
		}
	}
leaderElected:

	var leaderNode *raft.RaftNode
	switch leaderID {
	case 1:
		leaderNode = node1
	case 2:
		leaderNode = node2
	case 3:
		leaderNode = node3
	}

	// Submit a command
	idx, err := leaderNode.Submit("set foo bar")
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	t.Logf("Command submitted at index %d", idx)

	// Wait for replication and commit
	time.Sleep(2 * time.Second)

	// Verify all nodes have the value
	allSuccess := true
	for id, node := range map[int]*raft.RaftNode{1: node1, 2: node2, 3: node3} {
		val, ok := node.GetStateMachineValue("foo")
		if !ok || val != "bar" {
			t.Errorf("Node %d: expected 'bar', got '%s' (ok=%v)", id, val, ok)
			allSuccess = false
		} else {
			t.Logf("Node %d has foo=bar ✓", id)
		}
	}
	if !allSuccess {
		t.Fatal("replication failed")
	}
}
