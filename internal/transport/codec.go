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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
)

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	Command string `json:"command"`
}

// RequestVote RPC structure.
type RequestVote struct {
	Type         string `json:"type"` // always "RequestVote"
	Term         uint64 `json:"term"`
	CandidateID  int    `json:"candidateId"`
	LastLogIndex uint64 `json:"lastLogIndex"`
	LastLogTerm  uint64 `json:"lastLogTerm"`
}

// RequestVoteResponse RPC structure.
type RequestVoteResponse struct {
	Type        string `json:"type"` // "RequestVoteResponse"
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"voteGranted"`
}

// AppendEntries RPC structure.
type AppendEntries struct {
	Type         string     `json:"type"` // "AppendEntries"
	Term         uint64     `json:"term"`
	LeaderID     int        `json:"leaderId"`
	PrevLogIndex uint64     `json:"prevLogIndex"`
	PrevLogTerm  uint64     `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leaderCommit"`
}

// AppendEntriesResponse RPC structure.
type AppendEntriesResponse struct {
	Type    string `json:"type"` // "AppendEntriesResponse"
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

// Encode serializes msg into JSON, prepends 4-byte length prefix (big-endian).
func Encode(msg interface{}) ([]byte, error) {
	jsonData, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("json marshal: %w", err)
	}

	// 4-byte length prefix (big-endian)
	length := uint32(len(jsonData))
	buf := make([]byte, 4+len(jsonData))
	binary.BigEndian.PutUint32(buf[:4], length)
	copy(buf[4:], jsonData)

	return buf, nil
}

// Decode takes raw data (received from UDP), extracts length prefix, parses JSON.
// Returns the appropriate message type based on the "type" field.
func Decode(data []byte) (interface{}, error) {
	if len(data) < 4 {
		return nil, errors.New("data too short for length prefix")
	}

	length := binary.BigEndian.Uint32(data[:4])
	if int(length) > len(data)-4 {
		return nil, errors.New("length prefix exceeds data size")
	}

	jsonData := data[4 : 4+length]

	// First, parse just the "type" field to know which struct to unmarshal into.
	var typeOnly struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(jsonData, &typeOnly); err != nil {
		return nil, fmt.Errorf("unmarshal type field: %w", err)
	}

	// Now unmarshal into the appropriate struct.
	switch typeOnly.Type {
	case "RequestVote":
		var msg RequestVote
		if err := json.Unmarshal(jsonData, &msg); err != nil {
			return nil, fmt.Errorf("unmarshal RequestVote: %w", err)
		}
		return msg, nil
	case "RequestVoteResponse":
		var msg RequestVoteResponse
		if err := json.Unmarshal(jsonData, &msg); err != nil {
			return nil, fmt.Errorf("unmarshal RequestVoteResponse: %w", err)
		}
		return msg, nil
	case "AppendEntries":
		var msg AppendEntries
		if err := json.Unmarshal(jsonData, &msg); err != nil {
			return nil, fmt.Errorf("unmarshal AppendEntries: %w", err)
		}
		return msg, nil
	case "AppendEntriesResponse":
		var msg AppendEntriesResponse
		if err := json.Unmarshal(jsonData, &msg); err != nil {
			return nil, fmt.Errorf("unmarshal AppendEntriesResponse: %w", err)
		}
		return msg, nil
	default:
		return nil, fmt.Errorf("unknown message type: %q", typeOnly.Type)
	}
}

// GetMessageType quickly determines the message type without full parsing.
func GetMessageType(data []byte) (string, error) {
	if len(data) < 4 {
		return "", errors.New("data too short for length prefix")
	}

	length := binary.BigEndian.Uint32(data[:4])
	if int(length) > len(data)-4 {
		return "", errors.New("length prefix exceeds data size")
	}

	jsonData := data[4 : 4+length]

	var typeOnly struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(jsonData, &typeOnly); err != nil {
		return "", fmt.Errorf("unmarshal type field: %w", err)
	}

	if typeOnly.Type == "" {
		return "", errors.New("missing type field")
	}

	return typeOnly.Type, nil
}
