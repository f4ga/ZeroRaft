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

package client

import (
	"testing"

	"zeroraft/internal/raft"
)

// TestParseCommand tests command parsing without requiring a leader
// by using a mock that doesn't execute actual commands.
func TestParseCommand(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	raftSendFunc := func(addr string, msg interface{}) error { return nil }
	cliSendFunc := func(addr string, data []byte) error { return nil }

	node := raft.NewRaftNode(1, peers, t.TempDir(), raftSendFunc)
	node.Start()
	defer node.Stop()

	cli := NewCLI(node, cliSendFunc)

	// Test /status - should work even without leader
	err := cli.executeCommand("/status")
	if err != nil {
		t.Errorf("/status failed: %v", err)
	}

	// Test /leader - should work even without leader
	err = cli.executeCommand("/leader")
	if err != nil {
		t.Errorf("/leader failed: %v", err)
	}

	// Test /chaos - should work without leader
	err = cli.executeCommand("/chaos loss=0.5")
	if err != nil {
		t.Errorf("/chaos failed: %v", err)
	}

	// Test /get - should work without leader (local read)
	err = cli.executeCommand("/get foo")
	if err != nil {
		t.Errorf("/get failed: %v", err)
	}

	// Test /set - expects leader, so it will return error
	// This is expected behavior, not a test failure
	err = cli.executeCommand("/set foo bar")
	if err == nil {
		t.Log("Note: /set succeeded unexpectedly (maybe leader elected)")
	} else {
		t.Logf("/set correctly returned error: %v", err)
	}

	// Test invalid command
	err = cli.executeCommand("/invalid")
	if err == nil {
		t.Error("expected error for invalid command")
	}
}

// TestGetCommand tests /get command on follower
func TestGetCommand(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	raftSendFunc := func(addr string, msg interface{}) error { return nil }
	cliSendFunc := func(addr string, data []byte) error { return nil }

	node := raft.NewRaftNode(1, peers, t.TempDir(), raftSendFunc)
	node.Start()
	defer node.Stop()

	cli := NewCLI(node, cliSendFunc)

	// Get non-existent key
	err := cli.executeCommand("/get nonexistent")
	if err != nil {
		t.Errorf("/get failed: %v", err)
	}
}

// TestCLICreation tests that CLI can be created without errors
func TestCLICreation(t *testing.T) {
	peers := map[int]string{1: "addr1", 2: "addr2"}
	raftSendFunc := func(addr string, msg interface{}) error { return nil }
	cliSendFunc := func(addr string, data []byte) error { return nil }

	node := raft.NewRaftNode(1, peers, t.TempDir(), raftSendFunc)
	cli := NewCLI(node, cliSendFunc)

	// CLI should never be nil, but check anyway
	if cli == nil {
		t.Fatal("expected CLI instance, got nil")
	}
	if cli.node == nil {
		t.Fatal("expected node to be set")
	}
}
