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

func TestNewCodecDefault(t *testing.T) {
	c := NewCodec(CodecTypeJSON)
	if c == nil {
		t.Fatal("expected non-nil")
	}
	if _, ok := c.(*JSONCodec); !ok {
		t.Errorf("expected JSONCodec, got %T", c)
	}
}

func TestNewCodecJSON(t *testing.T) {
	c := NewCodec(CodecTypeJSON)
	if _, ok := c.(*JSONCodec); !ok {
		t.Errorf("expected JSONCodec, got %T", c)
	}
}

func TestNewCodecProtobuf(t *testing.T) {
	c := NewCodec(CodecTypeProtobuf)
	if _, ok := c.(*ProtobufCodec); !ok {
		t.Errorf("expected ProtobufCodec, got %T", c)
	}
}

func TestNewCodecUnknown(t *testing.T) {
	// Any unknown CodecType should fall back to JSONCodec
	c := NewCodec(CodecType(999))
	if _, ok := c.(*JSONCodec); !ok {
		t.Errorf("expected JSONCodec fallback, got %T", c)
	}
}
