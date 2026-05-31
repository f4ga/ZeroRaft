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

func TestNewRaftNode(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)

	if node == nil {
		t.Fatal("expected node, got nil")
	}
	if node.id != 1 {
		t.Errorf("expected id 1, got %d", node.id)
	}
	if node.state != Follower {
		t.Errorf("expected Follower, got %v", node.state)
	}
	if node.currentTerm != 0 {
		t.Errorf("expected term 0, got %d", node.currentTerm)
	}
	if node.votedFor != -1 {
		t.Errorf("expected votedFor -1, got %d", node.votedFor)
	}
}

func TestStartStop(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.Start()
	time.Sleep(10 * time.Millisecond)
	node.Stop()
}

func TestGetState(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)

	state := node.GetState()
	if state != Follower {
		t.Errorf("expected Follower, got %v", state)
	}
}

func TestGetCurrentTerm(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)

	term := node.GetCurrentTerm()
	if term != 0 {
		t.Errorf("expected term 0, got %d", term)
	}
}

func TestGetVotedFor(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)

	votedFor := node.GetVotedFor()
	if votedFor != -1 {
		t.Errorf("expected votedFor -1, got %d", votedFor)
	}
}

func TestGetLeaderID(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)

	leaderID := node.GetLeaderID()
	if leaderID != -1 {
		t.Errorf("expected leaderID -1, got %d", leaderID)
	}
}

func TestGetPeerAddr(t *testing.T) {
	peers := map[int]string{
		1: "127.0.0.1:8001",
		2: "127.0.0.1:8002",
		3: "127.0.0.1:8003",
	}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)

	addr := node.GetPeerAddr(2)
	if addr != "127.0.0.1:8002" {
		t.Errorf("expected 127.0.0.1:8002, got %s", addr)
	}

	addr = node.GetPeerAddr(999)
	if addr != "" {
		t.Errorf("expected empty string for unknown peer, got %s", addr)
	}
}

func TestGetCommitIndex(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)

	idx := node.GetCommitIndex()
	if idx != 0 {
		t.Errorf("expected commit index 0, got %d", idx)
	}
}

func TestGetStateMachineValue(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)

	val, ok := node.GetStateMachineValue("nonexistent")
	if ok {
		t.Error("expected false for nonexistent key")
	}
	if val != "" {
		t.Errorf("expected empty string, got %s", val)
	}
}

func TestHandleRequestVote(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)

	args := transport.RequestVote{
		Type:         "RequestVote",
		Term:         1,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp := node.handleRequestVote(args)

	if !resp.VoteGranted {
		t.Error("expected vote granted for first request")
	}
	if resp.Term != 1 {
		t.Errorf("expected term 1, got %d", resp.Term)
	}

	args2 := transport.RequestVote{
		Type:        "RequestVote",
		Term:        1,
		CandidateID: 3,
	}

	resp2 := node.handleRequestVote(args2)
	if resp2.VoteGranted {
		t.Error("expected vote denied after already voting")
	}
}

func TestHandleRequestVoteWithHigherTerm(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.currentTerm = 5

	args := transport.RequestVote{
		Type:         "RequestVote",
		Term:         3,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp := node.handleRequestVote(args)

	if resp.VoteGranted {
		t.Error("expected vote denied for stale term")
	}
}

func TestHandleAppendEntries(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)

	args := transport.AppendEntries{
		Type:     "AppendEntries",
		Term:     1,
		LeaderID: 2,
	}

	resp := node.handleAppendEntries(args)

	if !resp.Success {
		t.Error("expected success for heartbeat")
	}
	if resp.Term != 1 {
		t.Errorf("expected term 1, got %d", resp.Term)
	}

	args.Term = 0
	resp = node.handleAppendEntries(args)
	if resp.Success {
		t.Error("expected failure for stale term")
	}
}

func TestHandleAppendEntriesWithLogEntries(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)

	// Log starts with sentinel (index 0), so length is 1 initially
	initialLen := node.log.Len() // should be 1 (sentinel only)

	args := transport.AppendEntries{
		Type:         "AppendEntries",
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []transport.LogEntry{
			{Index: 1, Term: 1, Command: "test"},
		},
		LeaderCommit: 0,
	}

	resp := node.handleAppendEntries(args)

	if !resp.Success {
		t.Error("expected success")
	}

	// After adding 1 entry: sentinel (index 0) + new entry (index 1) = 2
	expectedLen := initialLen + 1 // 1 + 1 = 2
	if node.log.Len() != expectedLen {
		t.Errorf("expected log length %d, got %d", expectedLen, node.log.Len())
	}
}
func TestHandleAppendEntriesWithConflict(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)

	entry := LogEntry{Index: 1, Term: 1, Command: "old"}
	node.log.Append(entry)

	args := transport.AppendEntries{
		Type:         "AppendEntries",
		Term:         2,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []transport.LogEntry{
			{Index: 1, Term: 2, Command: "new"},
		},
		LeaderCommit: 0,
	}

	resp := node.handleAppendEntries(args)

	if !resp.Success {
		t.Error("expected success")
	}

	last := node.log.Last()
	if last.Index == 0 {
		t.Fatal("no entries in log")
	}
	if last.Command != "new" {
		t.Errorf("expected command 'new', got %s", last.Command)
	}
}

func TestHandleAppendEntriesWithPrevLogMismatch(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)

	entry := LogEntry{Index: 1, Term: 1, Command: "test"}
	node.log.Append(entry)

	args := transport.AppendEntries{
		Type:         "AppendEntries",
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 2,
		PrevLogTerm:  0,
		Entries: []transport.LogEntry{
			{Index: 3, Term: 1, Command: "new"},
		},
		LeaderCommit: 0,
	}

	resp := node.handleAppendEntries(args)

	if resp.Success {
		t.Error("expected failure for prevLog mismatch")
	}
}

func TestStartElection(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)

	node.startElection()

	if node.state != Candidate {
		t.Errorf("expected Candidate, got %v", node.state)
	}
	if node.currentTerm != 1 {
		t.Errorf("expected term 1, got %d", node.currentTerm)
	}
	if node.votedFor != 1 {
		t.Errorf("expected votedFor 1, got %d", node.votedFor)
	}
}

func TestSubmit(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)

	_, err := node.Submit("set foo bar")
	if err == nil {
		t.Error("expected error when submitting as follower")
	}

	node.state = Leader

	idx, err := node.Submit("set foo bar")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if idx != 1 {
		t.Errorf("expected index 1, got %d", idx)
	}
}

func TestApplyCommittedEntries(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)

	entry := LogEntry{Index: 1, Term: 1, Command: "set foo bar"}
	node.log.Append(entry)
	node.commitIndex = 1

	node.applyCommittedEntries()

	val, ok := node.stateMachine.Get("foo")
	if !ok {
		t.Error("expected foo to be set")
	}
	if val != "bar" {
		t.Errorf("expected bar, got %s", val)
	}

	if node.lastApplied != 1 {
		t.Errorf("expected lastApplied 1, got %d", node.lastApplied)
	}
}

func TestPersistStateLocked(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	dir := t.TempDir()
	node := NewRaftNode(1, peers, dir, sendFunc)

	node.persistStateLocked()

	state, err := LoadState(dir)
	if err != nil {
		t.Fatalf("LoadState failed: %v", err)
	}

	if state.CurrentTerm != node.currentTerm {
		t.Errorf("expected term %d, got %d", node.currentTerm, state.CurrentTerm)
	}
}

func TestRandomElectionTimeout(t *testing.T) {
	for i := 0; i < 50; i++ {
		timeout := randomElectionTimeout()
		if timeout < 150*time.Millisecond || timeout > 300*time.Millisecond {
			t.Errorf("timeout %v out of range [150ms, 300ms]", timeout)
		}
	}
}

func TestResetElectionTimer(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.resetElectionTimer()
}

func TestConvertToTransportEntries(t *testing.T) {
	entries := []LogEntry{
		{Index: 1, Term: 1, Command: "cmd1"},
		{Index: 2, Term: 1, Command: "cmd2"},
	}

	transportEntries := convertToTransportEntries(entries)

	if len(transportEntries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(transportEntries))
	}

	if transportEntries[0].Index != 1 {
		t.Errorf("expected index 1, got %d", transportEntries[0].Index)
	}

	if transportEntries[0].Command != "cmd1" {
		t.Errorf("expected cmd1, got %s", transportEntries[0].Command)
	}
}
func TestHandleResponseVote(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.startElection()

	resp := transport.RequestVoteResponse{
		Type:        "RequestVoteResponse",
		Term:        1,
		VoteGranted: true,
	}

	node.handleResponseVote(2, resp)
}

func TestHandleAppendEntriesResponse(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)

	resp := transport.AppendEntriesResponse{
		Type:    "AppendEntriesResponse",
		Term:    1,
		Success: true,
	}

	node.handleAppendEntriesResponse(2, resp)
}

func TestHandleRequestVoteResponseHigherTerm(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.startElection() // makes node Candidate with term=1

	resp := transport.RequestVoteResponse{
		Type:        "RequestVoteResponse",
		Term:        10,
		VoteGranted: false,
	}

	node.handleResponseVote(2, resp)

	if node.currentTerm != 10 {
		t.Errorf("expected term 10, got %d", node.currentTerm)
	}
	if node.state != Follower {
		t.Errorf("expected Follower, got %v", node.state)
	}
}
func TestRunLoop(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)

	done := make(chan bool)
	go func() {
		node.run()
		done <- true
	}()

	time.Sleep(50 * time.Millisecond)
	node.Stop()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Error("run loop didn't stop")
	}
}

func TestSendHeartbeats(t *testing.T) {
	var mu sync.Mutex
	var lastSent interface{}
	sendFunc := func(addr string, msg interface{}) error {
		mu.Lock()
		defer mu.Unlock()
		lastSent = msg
		return nil
	}

	peers := map[int]string{1: "addr1", 2: "addr2"}
	node := NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.state = Leader

	// Initialize nextIndex for followers
	for peerID := range peers {
		node.nextIndex[peerID] = 1
		node.matchIndex[peerID] = 0
	}

	// Call sendHeartbeats directly (it's synchronous when called directly)
	node.sendHeartbeats()

	// Give a little time for async goroutines to complete
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	sent := lastSent
	mu.Unlock()

	if sent == nil {
		t.Error("expected heartbeat to be sent")
		return
	}

	ae, ok := sent.(transport.AppendEntries)
	if !ok {
		t.Fatalf("expected AppendEntries, got %T", sent)
	}

	if len(ae.Entries) != 0 {
		t.Error("expected empty entries for heartbeat")
	}
}
