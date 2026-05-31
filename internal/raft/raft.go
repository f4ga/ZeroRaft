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
	"fmt"
	"math/rand"
	"sync"
	"time"

	"zeroraft/internal/transport"
)

type State string

const (
	Follower  State = "Follower"
	Candidate State = "Candidate"
	Leader    State = "Leader"
)

type RaftNode struct {
	mu          sync.RWMutex
	id          int
	peers       map[int]string
	currentTerm uint64
	votedFor    int
	state       State
	leaderId    int
	dataDir     string

	// Log and state machine
	log          *RaftLog
	stateMachine *StateMachine
	commitIndex  uint64
	lastApplied  uint64

	// Leader state (reinitialized after election)
	nextIndex  map[int]uint64
	matchIndex map[int]uint64

	votesReceived map[int]bool

	// Channels for timer management (no locks needed!)
	resetTimerCh chan struct{}
	stopCh       chan struct{}
	stopDone     chan struct{}

	sendFunc func(peerAddr string, msg interface{}) error
}

func NewRaftNode(id int, peers map[int]string, dataDir string, sendFunc func(string, interface{}) error) *RaftNode {
	// Load persisted state
	persisted, err := LoadState(dataDir)
	if err != nil {
		// Log error but continue with defaults
		persisted = PersistentState{CurrentTerm: 0, VotedFor: -1}
	}

	rn := &RaftNode{
		id:           id,
		peers:        peers,
		sendFunc:     sendFunc,
		dataDir:      dataDir,
		currentTerm:  persisted.CurrentTerm,
		votedFor:     persisted.VotedFor,
		state:        Follower,
		leaderId:     -1,
		log:          NewRaftLog(),
		stateMachine: NewStateMachine(),
		commitIndex:  0,
		lastApplied:  0,
		nextIndex:    make(map[int]uint64),
		matchIndex:   make(map[int]uint64),
		resetTimerCh: make(chan struct{}, 1),
		stopCh:       make(chan struct{}),
		stopDone:     make(chan struct{}),
	}

	// Initialize votesReceived map
	rn.votesReceived = make(map[int]bool)

	return rn
}

// Start launches the background event loop.
func (rn *RaftNode) Start() {
	rn.resetElectionTimer()
	go rn.run()
}

// resetElectionTimer sends a signal to reset the timer (non-blocking).
// No locks needed!
func (rn *RaftNode) resetElectionTimer() {
	select {
	case rn.resetTimerCh <- struct{}{}:
	default:
		// Channel already has a signal, that's enough
	}
}

func (rn *RaftNode) randomElectionTimeout() time.Duration {
	min := 150 * time.Millisecond
	max := 300 * time.Millisecond
	delta := rand.Int63n(int64(max - min))
	return min + time.Duration(delta)
}

// persistStateLocked persists state to disk. MUST be called with rn.mu held.
func (rn *RaftNode) persistStateLocked() {
	state := PersistentState{
		CurrentTerm: rn.currentTerm,
		VotedFor:    rn.votedFor,
	}
	if err := SaveState(rn.dataDir, state); err != nil {
		// In production, log this error
		_ = err
	}
}

func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	rn.votesReceived = map[int]bool{rn.id: true}
	term := rn.currentTerm

	// Initialize leader state
	for peerID := range rn.peers {
		rn.nextIndex[peerID] = rn.log.LastIndex() + 1
		rn.matchIndex[peerID] = 0
	}

	rn.persistStateLocked()
	rn.mu.Unlock()

	args := transport.RequestVote{
		Type:         "RequestVote",
		Term:         term,
		CandidateID:  rn.id,
		LastLogIndex: rn.log.LastIndex(),
		LastLogTerm:  rn.log.LastTerm(),
	}

	for _, addr := range rn.peers {
		go func(addr string, args transport.RequestVote) {
			if err := rn.sendFunc(addr, args); err != nil {
				_ = err
			}
		}(addr, args)
	}
}

func (rn *RaftNode) handleRequestVote(args transport.RequestVote) transport.RequestVoteResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if args.Term < rn.currentTerm {
		return transport.RequestVoteResponse{Type: "RequestVoteResponse", Term: rn.currentTerm, VoteGranted: false}
	}
	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.state = Follower
		rn.votedFor = -1
		rn.persistStateLocked()
	}
	if rn.votedFor != -1 && rn.votedFor != args.CandidateID {
		return transport.RequestVoteResponse{Type: "RequestVoteResponse", Term: rn.currentTerm, VoteGranted: false}
	}

	// Check if candidate's log is at least as up-to-date
	lastLogTerm := rn.log.LastTerm()
	lastLogIndex := rn.log.LastIndex()
	if args.LastLogTerm < lastLogTerm {
		return transport.RequestVoteResponse{Type: "RequestVoteResponse", Term: rn.currentTerm, VoteGranted: false}
	}
	if args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
		return transport.RequestVoteResponse{Type: "RequestVoteResponse", Term: rn.currentTerm, VoteGranted: false}
	}

	rn.votedFor = args.CandidateID
	rn.persistStateLocked()
	rn.resetElectionTimer()
	return transport.RequestVoteResponse{Type: "RequestVoteResponse", Term: rn.currentTerm, VoteGranted: true}
}

func (rn *RaftNode) handleAppendEntries(args transport.AppendEntries) transport.AppendEntriesResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if args.Term < rn.currentTerm {
		return transport.AppendEntriesResponse{Type: "AppendEntriesResponse", Term: rn.currentTerm, Success: false}
	}
	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.state = Follower
		rn.votedFor = -1
		rn.persistStateLocked()
	}
	rn.leaderId = args.LeaderID
	rn.resetElectionTimer()

	// Check if we have matching prevLog entry
	if !rn.log.HasEntry(args.PrevLogIndex, args.PrevLogTerm) {
		return transport.AppendEntriesResponse{Type: "AppendEntriesResponse", Term: rn.currentTerm, Success: false}
	}

	// Process entries
	for _, entry := range args.Entries {
		existing, exists := rn.log.Get(entry.Index)
		if exists && existing.Term != entry.Term {
			// Conflict: truncate from this index
			rn.log.TruncateFrom(entry.Index)
		}
		if !exists || existing.Term != entry.Term {
			rn.log.Append(LogEntry{
				Index:   entry.Index,
				Term:    entry.Term,
				Command: entry.Command,
			})
		}
	}

	// Update commit index
	if args.LeaderCommit > rn.commitIndex {
		lastIdx := rn.log.LastIndex()
		if args.LeaderCommit < lastIdx {
			rn.commitIndex = args.LeaderCommit
		} else {
			rn.commitIndex = lastIdx
		}
		rn.applyCommittedEntries()
	}

	return transport.AppendEntriesResponse{Type: "AppendEntriesResponse", Term: rn.currentTerm, Success: true}
}

func (rn *RaftNode) handleResponseVote(peerID int, resp transport.RequestVoteResponse) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Candidate {
		return
	}
	if resp.Term > rn.currentTerm {
		rn.currentTerm = resp.Term
		rn.state = Follower
		rn.votedFor = -1
		rn.persistStateLocked()
		rn.resetElectionTimer()
		return
	}
	if resp.VoteGranted {
		if rn.votesReceived == nil {
			rn.votesReceived = make(map[int]bool)
		}
		rn.votesReceived[peerID] = true

		majority := len(rn.peers)/2 + 1
		if len(rn.votesReceived) >= majority {
			rn.state = Leader
			rn.leaderId = rn.id
		}
	}
}

// handleAppendEntriesResponse processes responses from followers.
func (rn *RaftNode) handleAppendEntriesResponse(peerID int, resp transport.AppendEntriesResponse) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Leader {
		return
	}
	if resp.Term > rn.currentTerm {
		rn.currentTerm = resp.Term
		rn.state = Follower
		rn.votedFor = -1
		rn.persistStateLocked()
		rn.resetElectionTimer()
		return
	}
	if !resp.Success {
		// Conflict: decrement nextIndex and retry
		if rn.nextIndex[peerID] > 1 {
			rn.nextIndex[peerID]--
		}
		go rn.replicateToFollower(peerID)
		return
	}

	// Success: update matchIndex and nextIndex
	rn.matchIndex[peerID] = rn.nextIndex[peerID] - 1
	rn.nextIndex[peerID] = rn.log.LastIndex() + 1

	// Check if we can commit new entries
	for i := rn.commitIndex + 1; i <= rn.log.LastIndex(); i++ {
		majority := 0
		for _, match := range rn.matchIndex {
			if match >= i {
				majority++
			}
		}
		// Add self
		majority++
		if majority > len(rn.peers)/2 {
			rn.commitIndex = i
		}
	}
	rn.applyCommittedEntries()
}

func (rn *RaftNode) run() {
	timeout := rn.randomElectionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		rn.mu.RLock()
		state := rn.state
		rn.mu.RUnlock()

		if state == Leader {
			// Send heartbeats to all peers
			rn.sendHeartbeats()

			select {
			case <-rn.resetTimerCh:
			case <-rn.stopCh:
				close(rn.stopDone)
				return
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}

		select {
		case <-rn.resetTimerCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(rn.randomElectionTimeout())

		case <-timer.C:
			rn.startElection()
			timer.Reset(rn.randomElectionTimeout())

		case <-rn.stopCh:
			close(rn.stopDone)
			return
		}
	}
}

// sendHeartbeats sends empty AppendEntries to all peers.
func (rn *RaftNode) sendHeartbeats() {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if rn.state != Leader {
		return
	}

	for peerID := range rn.peers {
		prevLogIndex := rn.nextIndex[peerID] - 1
		prevLogEntry, _ := rn.log.Get(prevLogIndex)
		args := transport.AppendEntries{
			Type:         "AppendEntries",
			Term:         rn.currentTerm,
			LeaderID:     rn.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogEntry.Term,
			Entries:      nil, // empty = heartbeat
			LeaderCommit: rn.commitIndex,
		}
		go func(addr string) {
			_ = rn.sendFunc(addr, args)
		}(rn.peers[peerID])
	}
}

// Submit adds a command to the log. Only the leader can accept commands.
func (rn *RaftNode) Submit(command string) (uint64, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Leader {
		return 0, fmt.Errorf("not leader, current leader is node %d", rn.leaderId)
	}

	entry := LogEntry{
		Index:   rn.log.LastIndex() + 1,
		Term:    rn.currentTerm,
		Command: command,
	}
	rn.log.Append(entry)
	rn.matchIndex[rn.id] = entry.Index

	// Replicate to all peers
	for peerID := range rn.peers {
		go rn.replicateToFollower(peerID)
	}

	return entry.Index, nil
}

// replicateToFollower sends AppendEntries to a specific follower.
func (rn *RaftNode) replicateToFollower(followerID int) {
	rn.mu.RLock()
	nextIdx := rn.nextIndex[followerID]
	prevLogIndex := nextIdx - 1
	prevLogEntry, _ := rn.log.Get(prevLogIndex)
	entries := rn.log.SliceFrom(nextIdx)
	args := transport.AppendEntries{
		Type:         "AppendEntries",
		Term:         rn.currentTerm,
		LeaderID:     rn.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogEntry.Term,
		Entries:      convertToTransportEntries(entries),
		LeaderCommit: rn.commitIndex,
	}
	peerAddr := rn.peers[followerID]
	rn.mu.RUnlock()

	_ = rn.sendFunc(peerAddr, args)
}

func convertToTransportEntries(entries []LogEntry) []transport.LogEntry {
	result := make([]transport.LogEntry, len(entries))
	for i, e := range entries {
		result[i] = transport.LogEntry{
			Index:   e.Index,
			Term:    e.Term,
			Command: e.Command,
		}
	}
	return result
}

// applyCommittedEntries applies all committed but not yet applied entries.
func (rn *RaftNode) applyCommittedEntries() {
	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		entry, ok := rn.log.Get(rn.lastApplied)
		if !ok {
			continue
		}
		if err := rn.stateMachine.Apply(entry.Command); err != nil {
			_ = err // log in production
		}
	}
}

// HandleRequestVote is the public wrapper for testing.
func (rn *RaftNode) HandleRequestVote(args transport.RequestVote) transport.RequestVoteResponse {
	return rn.handleRequestVote(args)
}

// HandleAppendEntries is the public wrapper for testing.
func (rn *RaftNode) HandleAppendEntries(args transport.AppendEntries) transport.AppendEntriesResponse {
	return rn.handleAppendEntries(args)
}

// HandleRequestVoteResponse is the public wrapper for testing.
func (rn *RaftNode) HandleRequestVoteResponse(peerID int, resp transport.RequestVoteResponse) {
	rn.handleResponseVote(peerID, resp)
}

// HandleAppendEntriesResponse is the public wrapper for testing.
func (rn *RaftNode) HandleAppendEntriesResponse(peerID int, resp transport.AppendEntriesResponse) {
	rn.handleAppendEntriesResponse(peerID, resp)
}

// GetStateMachineValue reads a value from the local state machine.
func (rn *RaftNode) GetStateMachineValue(key string) (string, bool) {
	return rn.stateMachine.Get(key)
}

func (rn *RaftNode) Stop() {
	close(rn.stopCh)
	<-rn.stopDone
}

func (rn *RaftNode) GetState() State {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.state
}

func (rn *RaftNode) GetCurrentTerm() uint64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.currentTerm
}

func (rn *RaftNode) GetVotedFor() int {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.votedFor
}

func (rn *RaftNode) GetLeaderID() int {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.leaderId
}

// GetCommitIndex returns the current commit index.
func (rn *RaftNode) GetCommitIndex() uint64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.commitIndex
}

// GetPeerAddr returns the address of a peer by ID.
func (rn *RaftNode) GetPeerAddr(id int) string {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.peers[id]
}

// randomElectionTimeout returns a random duration between 150ms and 300ms.
func randomElectionTimeout() time.Duration {
	min := 150 * time.Millisecond
	max := 300 * time.Millisecond
	return min + time.Duration(rand.Int63n(int64(max-min)))
}
