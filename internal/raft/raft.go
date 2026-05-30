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

	lastLogIndex uint64
	lastLogTerm  uint64

	votesReceived map[int]bool

	// Channels for timer management (no locks needed!)
	resetTimerCh chan struct{}
	stopCh       chan struct{}
	stopDone     chan struct{}

	sendFunc func(peerAddr string, msg interface{}) error
}

func NewRaftNode(id int, peers map[int]string, sendFunc func(string, interface{}) error) *RaftNode {
	rn := &RaftNode{
		id:            id,
		peers:         peers,
		sendFunc:      sendFunc,
		state:         Follower,
		currentTerm:   0,
		votedFor:      -1,
		leaderId:      -1,
		votesReceived: make(map[int]bool),
		resetTimerCh:  make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
		stopDone:      make(chan struct{}),
	}

	go rn.run()
	return rn
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

func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	rn.votesReceived = map[int]bool{rn.id: true}
	term := rn.currentTerm
	rn.mu.Unlock()

	args := transport.RequestVote{
		Type:         "RequestVote",
		Term:         term,
		CandidateID:  rn.id,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	for _, addr := range rn.peers {
		go rn.sendFunc(addr, args)
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
	}
	if rn.votedFor != -1 && rn.votedFor != args.CandidateID {
		return transport.RequestVoteResponse{Type: "RequestVoteResponse", Term: rn.currentTerm, VoteGranted: false}
	}
	rn.votedFor = args.CandidateID
	rn.resetElectionTimer()
	return transport.RequestVoteResponse{Type: "RequestVoteResponse", Term: rn.currentTerm, VoteGranted: true}
}

func (rn *RaftNode) handleAppendEntries(args transport.AppendEntries) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if args.Term < rn.currentTerm {
		return
	}
	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.state = Follower
		rn.votedFor = -1
	}
	rn.leaderId = args.LeaderID
	rn.resetElectionTimer()
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

func (rn *RaftNode) run() {
	timeout := rn.randomElectionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		// Check state without blocking (read-only, quick)
		rn.mu.RLock()
		state := rn.state
		rn.mu.RUnlock()

		if state == Leader {
			// Leader doesn't need election timer
			select {
			case <-rn.resetTimerCh:
				// Just consume to keep channel empty
			case <-rn.stopCh:
				close(rn.stopDone)
				return
			}
			time.Sleep(50 * time.Millisecond)
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
