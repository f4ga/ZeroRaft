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

// Codec defines the interface for encoding/decoding Raft messages.
type Codec interface {
	Encode(msg interface{}) ([]byte, error)
	Decode(data []byte) (interface{}, error)
}

// CodecType specifies which encoding to use.
type CodecType int

const (
	CodecTypeJSON CodecType = iota
	CodecTypeProtobuf
)

// NewCodec creates a new codec of the specified type.
func NewCodec(codecType CodecType) Codec {
	switch codecType {
	case CodecTypeProtobuf:
		return &ProtobufCodec{}
	default:
		return &JSONCodec{}
	}
}
