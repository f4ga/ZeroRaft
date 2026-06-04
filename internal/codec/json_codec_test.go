// Copyright 2026 Ekaterina Godulyan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package codec

import "testing"

func TestJSONCodecEncodeDecode(t *testing.T) {
	c := &JSONCodec{}
	orig := RequestVote{Type: "RequestVote", Term: 5, CandidateID: 1}
	data, err := c.Encode(orig)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	decoded, err := c.Decode(data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	rv, ok := decoded.(RequestVote)
	if !ok {
		t.Fatalf("wrong type: %T", decoded)
	}
	if rv.Term != 5 {
		t.Errorf("term mismatch: got %d, want 5", rv.Term)
	}
}
func TestJSONCodecDecodeInvalid(t *testing.T) {
	c := &JSONCodec{}
	_, err := c.Decode([]byte{0, 0, 0, 0})
	if err == nil {
		t.Error("expected error for invalid data")
	}
}
func TestJSONCodecDecodeShortData(t *testing.T) {
	c := &JSONCodec{}
	_, err := c.Decode([]byte{0, 0, 1})
	if err == nil {
		t.Error("expected error for short data")
	}
}
func TestJSONCodecDecodeLengthMismatch(t *testing.T) {
	c := &JSONCodec{}
	// Length prefix says 100 bytes, but only 4 bytes total
	data := []byte{0, 0, 0, 100}
	_, err := c.Decode(data)
	if err == nil {
		t.Error("expected error for length mismatch")
	}
}
func TestJSONCodecEncodeDecodeAllTypes(t *testing.T) {
	c := &JSONCodec{}
	tests := []struct {
		name string
		msg  interface{}
	}{
		{
			name: "RequestVote",
			msg:  RequestVote{Type: "RequestVote", Term: 1, CandidateID: 2},
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
				Entries:      []LogEntry{},
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
			decoded, err := c.Decode(data)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			// Re-encode and compare bytes
			reEncoded, err := c.Encode(decoded)
			if err != nil {
				t.Fatalf("re-encode error: %v", err)
			}
			if string(data) != string(reEncoded) {
				t.Errorf("round-trip byte mismatch")
			}
		})
	}
}
func TestJSONCodecDecodeUnknownType(t *testing.T) {
	c := &JSONCodec{}
	// Encode a struct with unknown type field
	data, err := c.Encode(struct {
		Type string `json:"type"`
		Data string `json:"data"`
	}{Type: "UnknownType", Data: "test"})
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	_, err = c.Decode(data)
	if err == nil {
		t.Error("expected error for unknown message type")
	}
}
func TestJSONCodecDecodeMissingType(t *testing.T) {
	c := &JSONCodec{}
	// Encode a struct without a type field
	data, err := c.Encode(struct {
		Data string `json:"data"`
	}{Data: "test"})
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	_, err = c.Decode(data)
	if err == nil {
		t.Error("expected error for missing type field")
	}
}
