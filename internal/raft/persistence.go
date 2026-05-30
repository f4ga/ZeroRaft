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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// PersistentState represents the data that must be persisted to disk.
type PersistentState struct {
	CurrentTerm uint64 `json:"currentTerm"`
	VotedFor    int    `json:"votedFor"` // -1 if not voted
}

// SaveState atomically saves state to dataDir/raft-state.json.
// Uses temporary file + rename for atomicity.
func SaveState(dataDir string, state PersistentState) error {
	// Ensure directory exists (idempotent, safe for concurrent calls)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Marshal state to JSON
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	// Write to temporary file
	tmpPath := filepath.Join(dataDir, "raft-state.json.tmp")
	finalPath := filepath.Join(dataDir, "raft-state.json")

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	// Atomic rename (on POSIX systems)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		// If rename fails, try to clean up temp file
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// LoadState loads state from disk. Returns default (0, -1) if file not exists.
func LoadState(dataDir string) (PersistentState, error) {
	path := filepath.Join(dataDir, "raft-state.json")

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// No existing state, return defaults
			return PersistentState{CurrentTerm: 0, VotedFor: -1}, nil
		}
		return PersistentState{}, fmt.Errorf("failed to read state file: %w", err)
	}

	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return PersistentState{}, fmt.Errorf("failed to unmarshal state: %w", err)
	}

	return state, nil
}
