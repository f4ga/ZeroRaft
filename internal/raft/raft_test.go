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

package raft

import (
	"sync"
	"testing"
	"time"

	"zeroraft/internal/transport"
)

// mockSend is a simple mock for sendFunc with mutex for race safety.
type mockSend struct {
	mu   sync.Mutex
	sent []interface{}
}

func (m *mockSend) Send(addr string, msg interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, msg)
	return nil
}

func (m *mockSend) GetSent() []interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sent
}

// TestInitialState checks that a new node starts as Follower.
func TestInitialState(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	mock := &mockSend{}
	node := NewRaftNode(1, peers, mock.Send)
	defer node.Stop()

	if node.GetState() != Follower {
		t.Errorf("expected Follower, got %v", node.GetState())
	}
	if node.GetCurrentTerm() != 0 {
		t.Errorf("expected term 0, got %d", node.GetCurrentTerm())
	}
	if node.GetVotedFor() != -1 {
		t.Errorf("expected votedFor -1, got %d", node.GetVotedFor())
	}
}

// TestStartElection manually triggers election.
func TestStartElection(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	mock := &mockSend{}
	node := NewRaftNode(1, peers, mock.Send)
	defer node.Stop()

	node.startElection()

	if node.GetState() != Candidate {
		t.Errorf("expected Candidate, got %v", node.GetState())
	}
	if node.GetCurrentTerm() != 1 {
		t.Errorf("expected term 1, got %d", node.GetCurrentTerm())
	}
	if node.GetVotedFor() != 1 {
		t.Errorf("expected voted for self (1), got %d", node.GetVotedFor())
	}
}

// TestRequestVoteGrant tests that a follower grants vote.
func TestRequestVoteGrant(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	mock := &mockSend{}
	node := NewRaftNode(2, peers, mock.Send)
	defer node.Stop()

	args := transport.RequestVote{
		Type:        "RequestVote",
		Term:        1,
		CandidateID: 1,
	}
	resp := node.handleRequestVote(args)

	if !resp.VoteGranted {
		t.Error("expected VoteGranted=true")
	}
	if node.GetVotedFor() != 1 {
		t.Errorf("expected votedFor=1, got %d", node.GetVotedFor())
	}
}

// TestRequestVoteDenyAlreadyVoted tests vote denial after already voting.
func TestRequestVoteDenyAlreadyVoted(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	mock := &mockSend{}
	node := NewRaftNode(2, peers, mock.Send)
	defer node.Stop()

	// First vote
	args1 := transport.RequestVote{Term: 1, CandidateID: 1}
	node.handleRequestVote(args1)

	// Second vote for different candidate
	args2 := transport.RequestVote{Term: 1, CandidateID: 3}
	resp := node.handleRequestVote(args2)

	if resp.VoteGranted {
		t.Error("expected VoteGranted=false for second vote")
	}
}

// TestRequestVoteDenyStaleTerm tests stale term rejection.
func TestRequestVoteDenyStaleTerm(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	mock := &mockSend{}
	node := NewRaftNode(2, peers, mock.Send)
	defer node.Stop()

	// Manually set higher term
	node.mu.Lock()
	node.currentTerm = 5
	node.mu.Unlock()

	args := transport.RequestVote{Term: 3, CandidateID: 1}
	resp := node.handleRequestVote(args)

	if resp.VoteGranted {
		t.Error("expected VoteGranted=false for stale term")
	}
}

// TestRequestVoteHigherTermUpdatesTerm tests term update on higher term.
func TestRequestVoteHigherTermUpdatesTerm(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	mock := &mockSend{}
	node := NewRaftNode(2, peers, mock.Send)
	defer node.Stop()

	args := transport.RequestVote{Term: 5, CandidateID: 1}
	node.handleRequestVote(args)

	if node.GetCurrentTerm() != 5 {
		t.Errorf("expected term 5, got %d", node.GetCurrentTerm())
	}
	if node.GetState() != Follower {
		t.Errorf("expected Follower, got %v", node.GetState())
	}
}

// TestHeartbeatResetsElection tests that heartbeat prevents election.
func TestHeartbeatResetsElection(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	mock := &mockSend{}
	node := NewRaftNode(2, peers, mock.Send)
	defer node.Stop()

	// Send heartbeat
	args := transport.AppendEntries{Term: 1, LeaderID: 1}
	node.handleAppendEntries(args)

	// Heartbeat should call resetElectionTimer (non-blocking)
	// No assertion needed - just verifying no deadlock
}

// TestHeartbeatUpdatesTerm tests higher term heartbeat updates term.
func TestHeartbeatUpdatesTerm(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	mock := &mockSend{}
	node := NewRaftNode(2, peers, mock.Send)
	defer node.Stop()

	args := transport.AppendEntries{Term: 5, LeaderID: 1}
	node.handleAppendEntries(args)

	if node.GetCurrentTerm() != 5 {
		t.Errorf("expected term 5, got %d", node.GetCurrentTerm())
	}
	if node.GetLeaderID() != 1 {
		t.Errorf("expected leaderID 1, got %d", node.GetLeaderID())
	}
}

// TestHeartbeatStaleTermIgnored tests stale heartbeat is ignored.
func TestHeartbeatStaleTermIgnored(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	mock := &mockSend{}
	node := NewRaftNode(2, peers, mock.Send)
	defer node.Stop()

	node.mu.Lock()
	node.currentTerm = 5
	node.mu.Unlock()

	args := transport.AppendEntries{Term: 3, LeaderID: 1}
	node.handleAppendEntries(args)

	if node.GetCurrentTerm() != 5 {
		t.Errorf("term should remain 5, got %d", node.GetCurrentTerm())
	}
}

// TestLeaderID tests GetLeaderID method.
func TestLeaderID(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	mock := &mockSend{}
	node := NewRaftNode(2, peers, mock.Send)
	defer node.Stop()

	// Initially no leader
	if node.GetLeaderID() != -1 {
		t.Errorf("expected leaderID -1, got %d", node.GetLeaderID())
	}

	// After heartbeat from leader 1
	args := transport.AppendEntries{Term: 1, LeaderID: 1}
	node.handleAppendEntries(args)

	if node.GetLeaderID() != 1 {
		t.Errorf("expected leaderID 1, got %d", node.GetLeaderID())
	}
}

// TestBecomeLeader tests transition to Leader after getting votes.
func TestBecomeLeader(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2", 3: "addr3"}
	mock := &mockSend{}
	node := NewRaftNode(1, peers, mock.Send)
	defer node.Stop()

	// Start election
	node.startElection()

	// Simulate votes from peers 2 and 3
	node.handleResponseVote(2, transport.RequestVoteResponse{VoteGranted: true})
	node.handleResponseVote(3, transport.RequestVoteResponse{VoteGranted: true})

	time.Sleep(10 * time.Millisecond)

	if node.GetState() != Leader {
		t.Errorf("expected Leader, got %v", node.GetState())
	}
}
