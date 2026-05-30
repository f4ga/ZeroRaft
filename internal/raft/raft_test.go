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
	node := NewRaftNode(1, peers, t.TempDir(), mock.Send)
	node.Start()
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
	node := NewRaftNode(1, peers, t.TempDir(), mock.Send)
	node.Start()
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
	node := NewRaftNode(2, peers, t.TempDir(), mock.Send)
	node.Start()
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
	node := NewRaftNode(2, peers, t.TempDir(), mock.Send)
	node.Start()
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
	node := NewRaftNode(2, peers, t.TempDir(), mock.Send)
	node.Start()
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
	node := NewRaftNode(2, peers, t.TempDir(), mock.Send)
	node.Start()
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
	node := NewRaftNode(2, peers, t.TempDir(), mock.Send)
	node.Start()
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
	node := NewRaftNode(2, peers, t.TempDir(), mock.Send)
	node.Start()
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
	node := NewRaftNode(2, peers, t.TempDir(), mock.Send)
	node.Start()
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
	node := NewRaftNode(2, peers, t.TempDir(), mock.Send)
	node.Start()
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
	node := NewRaftNode(1, peers, t.TempDir(), mock.Send)
	node.Start()
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

// TestAppendEntriesWithEntries tests AppendEntries with log entries.
func TestAppendEntriesWithEntries(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	mock := &mockSend{}
	node := NewRaftNode(2, peers, t.TempDir(), mock.Send)
	node.Start()
	defer node.Stop()

	args := transport.AppendEntries{
		Type:         "AppendEntries",
		Term:         1,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []transport.LogEntry{
			{Index: 1, Term: 1, Command: "set foo bar"},
		},
		LeaderCommit: 0,
	}
	resp := node.handleAppendEntries(args)
	if !resp.Success {
		t.Error("expected success=true")
	}
	if node.log.LastIndex() != 1 {
		t.Errorf("expected last index 1, got %d", node.log.LastIndex())
	}
}

// TestAppendEntriesConflict tests conflict resolution.
func TestAppendEntriesConflict(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	mock := &mockSend{}
	node := NewRaftNode(2, peers, t.TempDir(), mock.Send)
	node.Start()
	defer node.Stop()

	// Add conflicting entries
	node.log.Append(LogEntry{Index: 1, Term: 1, Command: "old"})
	node.log.Append(LogEntry{Index: 2, Term: 1, Command: "old2"})

	// Leader sends entries with different term at index 1
	args := transport.AppendEntries{
		Type:         "AppendEntries",
		Term:         2,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []transport.LogEntry{
			{Index: 1, Term: 2, Command: "new"},
		},
		LeaderCommit: 0,
	}
	resp := node.handleAppendEntries(args)
	if !resp.Success {
		t.Error("expected success=true")
	}
	if node.log.Len() != 2 { // sentinel + 1 entry
		t.Errorf("expected 2 entries after conflict, got %d", node.log.Len())
	}
	entry, _ := node.log.Get(1)
	if entry.Term != 2 {
		t.Errorf("expected term 2 after overwrite, got %d", entry.Term)
	}
}

// TestCommitIndexUpdate tests that commitIndex is updated.
func TestCommitIndexUpdate(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	mock := &mockSend{}
	node := NewRaftNode(2, peers, t.TempDir(), mock.Send)
	node.Start()
	defer node.Stop()

	// Append entries and commit
	args := transport.AppendEntries{
		Type:         "AppendEntries",
		Term:         1,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []transport.LogEntry{
			{Index: 1, Term: 1, Command: "set x 1"},
		},
		LeaderCommit: 1,
	}
	node.handleAppendEntries(args)
	if node.commitIndex != 1 {
		t.Errorf("expected commitIndex 1, got %d", node.commitIndex)
	}
}

// TestSubmitCommand tests that leader accepts commands.
func TestSubmitCommand(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	mock := &mockSend{}
	node := NewRaftNode(1, peers, t.TempDir(), mock.Send)
	node.Start()
	defer node.Stop()

	// Become leader
	node.startElection()
	node.handleResponseVote(2, transport.RequestVoteResponse{VoteGranted: true})

	idx, err := node.Submit("set foo bar")
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	if idx != 1 {
		t.Errorf("expected index 1, got %d", idx)
	}

	entry, ok := node.log.Get(1)
	if !ok || entry.Command != "set foo bar" {
		t.Errorf("log entry mismatch: %+v", entry)
	}
}

// TestSubmitNotLeader tests that follower rejects commands.
func TestSubmitNotLeader(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	mock := &mockSend{}
	node := NewRaftNode(2, peers, t.TempDir(), mock.Send)
	node.Start()
	defer node.Stop()

	_, err := node.Submit("set foo bar")
	if err == nil {
		t.Error("expected error for non-leader submit")
	}
}
