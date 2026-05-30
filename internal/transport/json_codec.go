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
	"fmt"
)

// JSONCodec implements JSON encoding with length prefix.
type JSONCodec struct{}

// Encode serializes msg to JSON and adds 4-byte length prefix.
func (c *JSONCodec) Encode(msg interface{}) ([]byte, error) {
	jsonData, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("json marshal: %w", err)
	}

	buf := make([]byte, lengthPrefixSize+len(jsonData))
	binary.BigEndian.PutUint32(buf[:lengthPrefixSize], uint32(len(jsonData)))
	copy(buf[lengthPrefixSize:], jsonData)

	return buf, nil
}

// Decode extracts length prefix and parses JSON.
func (c *JSONCodec) Decode(data []byte) (interface{}, error) {
	if len(data) < lengthPrefixSize {
		return nil, fmt.Errorf("%w: expected at least %d bytes, got %d", ErrInsufficientData, lengthPrefixSize, len(data))
	}

	declaredLen := binary.BigEndian.Uint32(data[:lengthPrefixSize])
	if int(declaredLen) > len(data)-lengthPrefixSize {
		return nil, fmt.Errorf("%w: declared %d bytes, but only %d available after prefix", ErrLengthMismatch, declaredLen, len(data)-lengthPrefixSize)
	}

	jsonData := data[lengthPrefixSize : lengthPrefixSize+declaredLen]

	var typeOnly struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(jsonData, &typeOnly); err != nil {
		return nil, fmt.Errorf("unmarshal type field: %w", err)
	}

	if typeOnly.Type == "" {
		return nil, fmt.Errorf("%w: message has missing 'type' field", ErrLengthMismatch)
	}

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
