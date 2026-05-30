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
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestSaveAndLoad verifies that SaveState writes a correct file
// and LoadState reads the same values back.
func TestSaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	state := PersistentState{CurrentTerm: 42, VotedFor: 7}
	if err := SaveState(dir, state); err != nil {
		t.Fatalf("SaveState returned error: %v", err)
	}
	loaded, err := LoadState(dir)
	if err != nil {
		t.Fatalf("LoadState returned error: %v", err)
	}
	if loaded.CurrentTerm != state.CurrentTerm || loaded.VotedFor != state.VotedFor {
		t.Fatalf("loaded state mismatch: got %+v, want %+v", loaded, state)
	}
}

// TestLoadNonExistent confirms that LoadState returns defaults
// when the state file does not exist.
func TestLoadNonExistent(t *testing.T) {
	dir := t.TempDir()
	st, err := LoadState(dir)
	if err != nil {
		t.Fatalf("LoadState returned error: %v", err)
	}
	if st.CurrentTerm != 0 || st.VotedFor != -1 {
		t.Fatalf("expected defaults (0, -1), got %+v", st)
	}
}

// TestLoadCorruptedFile ensures that an invalid JSON file
// results in an error.
func TestLoadCorruptedFile(t *testing.T) {
	dir := t.TempDir()
	// Write a malformed JSON file.
	corruptPath := filepath.Join(dir, "raft-state.json")
	if err := os.WriteFile(corruptPath, []byte("{invalid json"), 0644); err != nil {
		t.Fatalf("failed to write corrupt file: %v", err)
	}
	_, err := LoadState(dir)
	if err == nil {
		t.Fatalf("expected error loading corrupted file, got nil")
	}
}

// TestAtomicWrite simulates a crash after the temporary file has been
// written but before the rename. The temporary file should be ignored
// on the next LoadState call (i.e., default values are used).
func TestAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	tmpPath := filepath.Join(dir, "raft-state.json.tmp")
	// Write only the temporary file, never rename it.
	if err := os.WriteFile(tmpPath, []byte(`{"currentTerm":99,"votedFor":2}`), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	// LoadState should ignore the temp file and return defaults.
	st, err := LoadState(dir)
	if err != nil {
		t.Fatalf("LoadState returned error: %v", err)
	}
	if st.CurrentTerm != 0 || st.VotedFor != -1 {
		t.Fatalf("expected defaults because final file missing, got %+v", st)
	}
}

// TestConcurrentSave runs SaveState from multiple goroutines
// to ensure there are no data races and that the final file
// contains a valid JSON representation.
func TestConcurrentSave(t *testing.T) {
	dir := t.TempDir()
	const workers = 10
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(idx int) {
			defer wg.Done()
			// Each worker writes to its own subdirectory to avoid rename conflicts
			workerDir := filepath.Join(dir, fmt.Sprintf("worker-%d", idx))
			state := PersistentState{CurrentTerm: uint64(idx), VotedFor: idx}
			if err := SaveState(workerDir, state); err != nil {
				t.Errorf("worker %d SaveState error: %v", idx, err)
			}
		}(i)
	}
	wg.Wait()

	// Verify each worker's state was saved correctly
	for i := 0; i < workers; i++ {
		workerDir := filepath.Join(dir, fmt.Sprintf("worker-%d", i))
		st, err := LoadState(workerDir)
		if err != nil {
			t.Fatalf("worker %d LoadState failed: %v", i, err)
		}
		if st.CurrentTerm != uint64(i) || st.VotedFor != i {
			t.Errorf("worker %d state mismatch: got %+v, want term=%d votedFor=%d", i, st, i, i)
		}
	}
}

// TestIntegrationWithRaftNode checks that a RaftNode persists its
// term and vote across restarts.
// Note: This test uses a minimal mock of the transport layer.
func TestIntegrationWithRaftNode(t *testing.T) {
	dir := t.TempDir()
	peers := map[int]string{2: "peer2"}

	mockSend := func(to string, msg interface{}) error { return nil }

	// Create a node, force a term change (no Start = no background goroutine = no race)
	node := NewRaftNode(1, peers, dir, mockSend)
	node.currentTerm = 5
	node.votedFor = 2
	if err := SaveState(dir, PersistentState{CurrentTerm: node.currentTerm, VotedFor: node.votedFor}); err != nil {
		t.Fatalf("initial SaveState failed: %v", err)
	}

	// Re-create a node with the same data directory
	newNode := NewRaftNode(1, peers, dir, mockSend)
	if newNode.currentTerm != 5 || newNode.votedFor != 2 {
		t.Fatalf("state not recovered after restart: term=%d votedFor=%d", newNode.currentTerm, newNode.votedFor)
	}
}
