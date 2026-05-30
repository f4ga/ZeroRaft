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
	"fmt"
	raft "zeroraft/internal/api"

	"google.golang.org/protobuf/proto"
)

// ProtobufCodec implements binary encoding using Protocol Buffers.
type ProtobufCodec struct{}

// Encode serializes msg to protobuf and adds 4-byte length prefix.
func (c *ProtobufCodec) Encode(msg interface{}) ([]byte, error) {
	var protoMsg proto.Message

	switch m := msg.(type) {
	case RequestVote:
		protoMsg = &raft.RequestVote{
			Type:         m.Type,
			Term:         m.Term,
			CandidateId:  int32(m.CandidateID),
			LastLogIndex: m.LastLogIndex,
			LastLogTerm:  m.LastLogTerm,
		}
	case RequestVoteResponse:
		protoMsg = &raft.RequestVoteResponse{
			Type:        m.Type,
			Term:        m.Term,
			VoteGranted: m.VoteGranted,
		}
	case AppendEntries:
		entries := make([]*raft.LogEntry, len(m.Entries))
		for i, e := range m.Entries {
			entries[i] = &raft.LogEntry{
				Index:   e.Index,
				Term:    e.Term,
				Command: e.Command,
			}
		}
		protoMsg = &raft.AppendEntries{
			Type:         m.Type,
			Term:         m.Term,
			LeaderId:     int32(m.LeaderID),
			PrevLogIndex: m.PrevLogIndex,
			PrevLogTerm:  m.PrevLogTerm,
			Entries:      entries,
			LeaderCommit: m.LeaderCommit,
		}
	case AppendEntriesResponse:
		protoMsg = &raft.AppendEntriesResponse{
			Type:    m.Type,
			Term:    m.Term,
			Success: m.Success,
		}
	default:
		return nil, fmt.Errorf("unsupported message type: %T", msg)
	}

	// Marshal to protobuf
	pbData, err := proto.Marshal(protoMsg)
	if err != nil {
		return nil, fmt.Errorf("protobuf marshal: %w", err)
	}

	// Add length prefix (using lengthPrefixSize constant)
	buf := make([]byte, lengthPrefixSize+len(pbData))
	binary.BigEndian.PutUint32(buf[:lengthPrefixSize], uint32(len(pbData)))
	copy(buf[lengthPrefixSize:], pbData)

	return buf, nil
}

// Decode extracts length prefix and unmarshals protobuf message.
func (c *ProtobufCodec) Decode(data []byte) (interface{}, error) {
	if len(data) < lengthPrefixSize {
		return nil, ErrInsufficientData
	}

	length := binary.BigEndian.Uint32(data[:lengthPrefixSize])
	if int(length) > len(data)-lengthPrefixSize {
		return nil, fmt.Errorf("%w: declared %d bytes, only %d available", ErrLengthMismatch, length, len(data)-lengthPrefixSize)
	}

	pbData := data[lengthPrefixSize : lengthPrefixSize+length]

	// Try RequestVote
	var reqVote raft.RequestVote
	if err := proto.Unmarshal(pbData, &reqVote); err == nil && reqVote.Type == "RequestVote" {
		return RequestVote{
			Type:         reqVote.Type,
			Term:         reqVote.Term,
			CandidateID:  int(reqVote.CandidateId),
			LastLogIndex: reqVote.LastLogIndex,
			LastLogTerm:  reqVote.LastLogTerm,
		}, nil
	}

	// Try RequestVoteResponse
	var reqVoteResp raft.RequestVoteResponse
	if err := proto.Unmarshal(pbData, &reqVoteResp); err == nil && reqVoteResp.Type == "RequestVoteResponse" {
		return RequestVoteResponse{
			Type:        reqVoteResp.Type,
			Term:        reqVoteResp.Term,
			VoteGranted: reqVoteResp.VoteGranted,
		}, nil
	}

	// Try AppendEntries
	var appendEntries raft.AppendEntries
	if err := proto.Unmarshal(pbData, &appendEntries); err == nil && appendEntries.Type == "AppendEntries" {
		entries := make([]LogEntry, len(appendEntries.Entries))
		for i, e := range appendEntries.Entries {
			entries[i] = LogEntry{
				Index:   e.Index,
				Term:    e.Term,
				Command: e.Command,
			}
		}
		return AppendEntries{
			Type:         appendEntries.Type,
			Term:         appendEntries.Term,
			LeaderID:     int(appendEntries.LeaderId),
			PrevLogIndex: appendEntries.PrevLogIndex,
			PrevLogTerm:  appendEntries.PrevLogTerm,
			Entries:      entries,
			LeaderCommit: appendEntries.LeaderCommit,
		}, nil
	}

	// Try AppendEntriesResponse
	var appendEntriesResp raft.AppendEntriesResponse
	if err := proto.Unmarshal(pbData, &appendEntriesResp); err == nil && appendEntriesResp.Type == "AppendEntriesResponse" {
		return AppendEntriesResponse{
			Type:    appendEntriesResp.Type,
			Term:    appendEntriesResp.Term,
			Success: appendEntriesResp.Success,
		}, nil
	}

	return nil, fmt.Errorf("unknown protobuf message type")
}
