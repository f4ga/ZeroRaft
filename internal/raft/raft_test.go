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
