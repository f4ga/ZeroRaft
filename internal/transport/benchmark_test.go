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
	"testing"
)

var (
	jsonCodec     = &JSONCodec{}
	protobufCodec = &ProtobufCodec{}

	testRequestVote = RequestVote{
		Type:         "RequestVote",
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 100,
		LastLogTerm:  4,
	}

	testAppendEntries = AppendEntries{
		Type:         "AppendEntries",
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 99,
		PrevLogTerm:  4,
		Entries: []LogEntry{
			{Index: 100, Term: 5, Command: "set foo bar"},
			{Index: 101, Term: 5, Command: "set baz qux"},
		},
		LeaderCommit: 99,
	}
)

// BenchmarkJSONEncode measures JSON encoding performance.
func BenchmarkJSONEncode(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = jsonCodec.Encode(testRequestVote)
	}
}

// BenchmarkProtobufEncode measures Protobuf encoding performance.
func BenchmarkProtobufEncode(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = protobufCodec.Encode(testRequestVote)
	}
}

// BenchmarkJSONDecode measures JSON decoding performance.
func BenchmarkJSONDecode(b *testing.B) {
	encoded, _ := jsonCodec.Encode(testAppendEntries)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = jsonCodec.Decode(encoded)
	}
}

// BenchmarkProtobufDecode measures Protobuf decoding performance.
func BenchmarkProtobufDecode(b *testing.B) {
	encoded, _ := protobufCodec.Encode(testAppendEntries)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = protobufCodec.Decode(encoded)
	}
}

// BenchmarkJSONSize measures JSON message size.
func BenchmarkJSONSize(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, _ := jsonCodec.Encode(testAppendEntries)
		_ = len(data)
	}
}

// BenchmarkProtobufSize measures Protobuf message size.
func BenchmarkProtobufSize(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, _ := protobufCodec.Encode(testAppendEntries)
		_ = len(data)
	}
}
