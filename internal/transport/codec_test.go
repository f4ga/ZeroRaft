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
	"math/rand"
	"testing"
)

// No need to call rand.Seed in Go 1.20+

func TestEncodeDecodeRequestVote(t *testing.T) {
	msg := RequestVote{
		Type:         "RequestVote",
		Term:         5,
		CandidateID:  3,
		LastLogIndex: 100,
		LastLogTerm:  4,
	}

	encoded, err := Encode(msg)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	decodedMsg, ok := decoded.(RequestVote)
	if !ok {
		t.Fatalf("Decoded message is not RequestVote, got %T", decoded)
	}

	if decodedMsg.Term != msg.Term {
		t.Errorf("Term mismatch: got %d, want %d", decodedMsg.Term, msg.Term)
	}
	if decodedMsg.CandidateID != msg.CandidateID {
		t.Errorf("CandidateID mismatch: got %d, want %d", decodedMsg.CandidateID, msg.CandidateID)
	}
	if decodedMsg.LastLogIndex != msg.LastLogIndex {
		t.Errorf("LastLogIndex mismatch: got %d, want %d", decodedMsg.LastLogIndex, msg.LastLogIndex)
	}
	if decodedMsg.LastLogTerm != msg.LastLogTerm {
		t.Errorf("LastLogTerm mismatch: got %d, want %d", decodedMsg.LastLogTerm, msg.LastLogTerm)
	}
}

func TestEncodeDecodeRequestVoteResponse(t *testing.T) {
	msg := RequestVoteResponse{
		Type:        "RequestVoteResponse",
		Term:        5,
		VoteGranted: true,
	}

	encoded, err := Encode(msg)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	decodedMsg, ok := decoded.(RequestVoteResponse)
	if !ok {
		t.Fatalf("Decoded message is not RequestVoteResponse, got %T", decoded)
	}

	if decodedMsg.Term != msg.Term {
		t.Errorf("Term mismatch: got %d, want %d", decodedMsg.Term, msg.Term)
	}
	if decodedMsg.VoteGranted != msg.VoteGranted {
		t.Errorf("VoteGranted mismatch: got %v, want %v", decodedMsg.VoteGranted, msg.VoteGranted)
	}
}

func TestEncodeDecodeAppendEntries(t *testing.T) {
	msg := AppendEntries{
		Type:         "AppendEntries",
		Term:         7,
		LeaderID:     2,
		PrevLogIndex: 150,
		PrevLogTerm:  6,
		Entries: []LogEntry{
			{Index: 151, Term: 7, Command: "set x 1"},
			{Index: 152, Term: 7, Command: "set y 2"},
		},
		LeaderCommit: 140,
	}

	encoded, err := Encode(msg)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	decodedMsg, ok := decoded.(AppendEntries)
	if !ok {
		t.Fatalf("Decoded message is not AppendEntries, got %T", decoded)
	}

	if decodedMsg.Term != msg.Term {
		t.Errorf("Term mismatch: got %d, want %d", decodedMsg.Term, msg.Term)
	}
	if decodedMsg.LeaderID != msg.LeaderID {
		t.Errorf("LeaderID mismatch: got %d, want %d", decodedMsg.LeaderID, msg.LeaderID)
	}
	if decodedMsg.PrevLogIndex != msg.PrevLogIndex {
		t.Errorf("PrevLogIndex mismatch: got %d, want %d", decodedMsg.PrevLogIndex, msg.PrevLogIndex)
	}
	if decodedMsg.PrevLogTerm != msg.PrevLogTerm {
		t.Errorf("PrevLogTerm mismatch: got %d, want %d", decodedMsg.PrevLogTerm, msg.PrevLogTerm)
	}
	if decodedMsg.LeaderCommit != msg.LeaderCommit {
		t.Errorf("LeaderCommit mismatch: got %d, want %d", decodedMsg.LeaderCommit, msg.LeaderCommit)
	}
	if len(decodedMsg.Entries) != len(msg.Entries) {
		t.Fatalf("Entries length mismatch: got %d, want %d", len(decodedMsg.Entries), len(msg.Entries))
	}
	for i := range msg.Entries {
		if decodedMsg.Entries[i].Index != msg.Entries[i].Index {
			t.Errorf("Entries[%d].Index mismatch: got %d, want %d", i, decodedMsg.Entries[i].Index, msg.Entries[i].Index)
		}
		if decodedMsg.Entries[i].Term != msg.Entries[i].Term {
			t.Errorf("Entries[%d].Term mismatch: got %d, want %d", i, decodedMsg.Entries[i].Term, msg.Entries[i].Term)
		}
		if decodedMsg.Entries[i].Command != msg.Entries[i].Command {
			t.Errorf("Entries[%d].Command mismatch: got %q, want %q", i, decodedMsg.Entries[i].Command, msg.Entries[i].Command)
		}
	}
}

func TestEncodeDecodeAppendEntriesResponse(t *testing.T) {
	msg := AppendEntriesResponse{
		Type:    "AppendEntriesResponse",
		Term:    7,
		Success: false,
	}

	encoded, err := Encode(msg)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	decodedMsg, ok := decoded.(AppendEntriesResponse)
	if !ok {
		t.Fatalf("Decoded message is not AppendEntriesResponse, got %T", decoded)
	}

	if decodedMsg.Term != msg.Term {
		t.Errorf("Term mismatch: got %d, want %d", decodedMsg.Term, msg.Term)
	}
	if decodedMsg.Success != msg.Success {
		t.Errorf("Success mismatch: got %v, want %v", decodedMsg.Success, msg.Success)
	}
}

func TestEncodeWithEmptyEntries(t *testing.T) {
	msg := AppendEntries{
		Type:         "AppendEntries",
		Term:         1,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{}, // empty slice
		LeaderCommit: 0,
	}

	encoded, err := Encode(msg)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	decodedMsg, ok := decoded.(AppendEntries)
	if !ok {
		t.Fatalf("Decoded message is not AppendEntries, got %T", decoded)
	}

	if len(decodedMsg.Entries) != 0 {
		t.Errorf("Expected empty Entries, got %d entries", len(decodedMsg.Entries))
	}
}

func TestDecodeIncompleteData(t *testing.T) {
	// Data shorter than 4 bytes
	data := []byte{0, 0, 1}
	_, err := Decode(data)
	if err == nil {
		t.Error("Expected error for incomplete data, got nil")
	}
}

func TestDecodeTruncatedJSON(t *testing.T) {
	// Length prefix says 100 bytes, but actual data is shorter
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data[:4], 100) // length = 100
	// data[4:] is missing

	_, err := Decode(data)
	if err == nil {
		t.Error("Expected error for truncated JSON, got nil")
	}
}

func TestEncodeDecodeRandomMessages(t *testing.T) {
	// Generate 100 random messages (fuzzing)
	for i := 0; i < 100; i++ {
		var msg interface{}
		switch rand.Intn(4) {
		case 0:
			msg = RequestVote{
				Type:         "RequestVote",
				Term:         rand.Uint64(),
				CandidateID:  rand.Int(),
				LastLogIndex: rand.Uint64(),
				LastLogTerm:  rand.Uint64(),
			}
		case 1:
			msg = RequestVoteResponse{
				Type:        "RequestVoteResponse",
				Term:        rand.Uint64(),
				VoteGranted: rand.Intn(2) == 1,
			}
		case 2:
			entries := make([]LogEntry, rand.Intn(5))
			for j := range entries {
				entries[j] = LogEntry{
					Index:   rand.Uint64(),
					Term:    rand.Uint64(),
					Command: randomString(10),
				}
			}
			msg = AppendEntries{
				Type:         "AppendEntries",
				Term:         rand.Uint64(),
				LeaderID:     rand.Int(),
				PrevLogIndex: rand.Uint64(),
				PrevLogTerm:  rand.Uint64(),
				Entries:      entries,
				LeaderCommit: rand.Uint64(),
			}
		case 3:
			msg = AppendEntriesResponse{
				Type:    "AppendEntriesResponse",
				Term:    rand.Uint64(),
				Success: rand.Intn(2) == 1,
			}
		}

		encoded, err := Encode(msg)
		if err != nil {
			t.Fatalf("Encode failed on iteration %d: %v", i, err)
		}

		decoded, err := Decode(encoded)
		if err != nil {
			t.Fatalf("Decode failed on iteration %d: %v", i, err)
		}

		// Compare by re-encoding the decoded message and comparing bytes
		reencoded, err := Encode(decoded)
		if err != nil {
			t.Fatalf("Re-encode failed on iteration %d: %v", i, err)
		}

		if string(encoded) != string(reencoded) {
			t.Errorf("Round-trip mismatch on iteration %d", i)
		}
	}
}

func TestGetMessageType(t *testing.T) {
	tests := []struct {
		name     string
		msg      interface{}
		wantType string
	}{
		{
			name: "RequestVote",
			msg: RequestVote{
				Type:         "RequestVote",
				Term:         1,
				CandidateID:  1,
				LastLogIndex: 1,
				LastLogTerm:  1,
			},
			wantType: "RequestVote",
		},
		{
			name: "RequestVoteResponse",
			msg: RequestVoteResponse{
				Type:        "RequestVoteResponse",
				Term:        1,
				VoteGranted: true,
			},
			wantType: "RequestVoteResponse",
		},
		{
			name: "AppendEntries",
			msg: AppendEntries{
				Type:         "AppendEntries",
				Term:         1,
				LeaderID:     1,
				PrevLogIndex: 1,
				PrevLogTerm:  1,
				Entries:      []LogEntry{},
				LeaderCommit: 1,
			},
			wantType: "AppendEntries",
		},
		{
			name: "AppendEntriesResponse",
			msg: AppendEntriesResponse{
				Type:    "AppendEntriesResponse",
				Term:    1,
				Success: true,
			},
			wantType: "AppendEntriesResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := Encode(tt.msg)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			gotType, err := GetMessageType(encoded)
			if err != nil {
				t.Fatalf("GetMessageType failed: %v", err)
			}

			if gotType != tt.wantType {
				t.Errorf("GetMessageType() = %q, want %q", gotType, tt.wantType)
			}
		})
	}
}

func TestGetMessageTypeInvalid(t *testing.T) {
	// Too short data
	_, err := GetMessageType([]byte{0, 0, 1})
	if err == nil {
		t.Error("Expected error for short data, got nil")
	}

	// Length exceeds data
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data[:4], 100)
	_, err = GetMessageType(data)
	if err == nil {
		t.Error("Expected error for length exceeding data, got nil")
	}

	// Invalid JSON
	data = make([]byte, 8)
	binary.BigEndian.PutUint32(data[:4], 4)
	copy(data[4:], "{invalid")
	_, err = GetMessageType(data)
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}

	// JSON without type field
	data = make([]byte, 8)
	binary.BigEndian.PutUint32(data[:4], 2)
	copy(data[4:], "{}")
	_, err = GetMessageType(data)
	if err == nil {
		t.Error("Expected error for missing type field, got nil")
	}
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
