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

import "testing"

func TestProtobufCodecEncodeDecode(t *testing.T) {
	c := &ProtobufCodec{}
	data, err := c.Encode(RequestVote{Type: "RequestVote", Term: 1, CandidateID: 1})
	if err != nil {
		t.Fatalf("protobuf encode error: %v", err)
	}
	if data == nil {
		t.Fatal("expected non-nil data")
	}
	decoded, err := c.Decode(data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	rv, ok := decoded.(RequestVote)
	if !ok {
		t.Fatalf("wrong type: %T", decoded)
	}
	if rv.Term != 1 {
		t.Errorf("term mismatch: got %d, want 1", rv.Term)
	}
}

func TestProtobufCodecEncodeDecodeAllTypes(t *testing.T) {
	c := &ProtobufCodec{}

	tests := []struct {
		name string
		msg  interface{}
	}{
		{
			name: "RequestVote",
			msg:  RequestVote{Type: "RequestVote", Term: 1, CandidateID: 2, LastLogIndex: 10, LastLogTerm: 1},
		},
		{
			name: "RequestVoteResponse",
			msg:  RequestVoteResponse{Type: "RequestVoteResponse", Term: 1, VoteGranted: true},
		},
		{
			name: "AppendEntries",
			msg: AppendEntries{
				Type:         "AppendEntries",
				Term:         1,
				LeaderID:     1,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries: []LogEntry{
					{Index: 1, Term: 1, Command: "set x 1"},
				},
				LeaderCommit: 0,
			},
		},
		{
			name: "AppendEntriesResponse",
			msg:  AppendEntriesResponse{Type: "AppendEntriesResponse", Term: 1, Success: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := c.Encode(tt.msg)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}
			if data == nil {
				t.Fatal("expected non-nil data")
			}
			decoded, err := c.Decode(data)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if decoded == nil {
				t.Fatal("expected non-nil decoded message")
			}
		})
	}
}

func TestProtobufCodecDecodeInvalid(t *testing.T) {
	c := &ProtobufCodec{}
	_, err := c.Decode([]byte{0, 0, 0, 0})
	if err == nil {
		t.Error("expected error for invalid data")
	}
}

func TestProtobufCodecDecodeShortData(t *testing.T) {
	c := &ProtobufCodec{}
	_, err := c.Decode([]byte{0, 0, 1})
	if err == nil {
		t.Error("expected error for short data")
	}
}

func TestProtobufCodecDecodeLengthMismatch(t *testing.T) {
	c := &ProtobufCodec{}
	// Length prefix says 100 bytes, but only 4 bytes total
	data := []byte{0, 0, 0, 100}
	_, err := c.Decode(data)
	if err == nil {
		t.Error("expected error for length mismatch")
	}
}

func TestProtobufCodecUnsupportedType(t *testing.T) {
	c := &ProtobufCodec{}
	_, err := c.Encode("unsupported string type")
	if err == nil {
		t.Error("expected error for unsupported type")
	}
}
