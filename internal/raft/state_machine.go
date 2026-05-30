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
	"fmt"
	"strings"
	"sync"
)

// StateMachine is a simple in-memory key-value store.
type StateMachine struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewStateMachine creates a new state machine.
func NewStateMachine() *StateMachine {
	return &StateMachine{
		data: make(map[string]string),
	}
}

// Set sets a key-value pair.
func (sm *StateMachine) Set(key, value string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.data[key] = value
}

// Get returns the value for a key.
func (sm *StateMachine) Get(key string) (string, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	v, ok := sm.data[key]
	return v, ok
}

// Apply parses and executes a command. Supported commands:
//   - "set key value"
//   - "get key" (no-op for apply, handled by Get)
func (sm *StateMachine) Apply(command string) error {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return fmt.Errorf("empty command")
	}

	switch parts[0] {
	case "set":
		if len(parts) < 3 {
			return fmt.Errorf("set requires key and value")
		}
		sm.Set(parts[1], strings.Join(parts[2:], " "))
		return nil
	case "get":
		return nil // no state change
	default:
		return fmt.Errorf("unknown command: %s", parts[0])
	}
}
