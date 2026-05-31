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

// TestCLICreation tests CLI initialization with real RaftNode
func TestCLICreation(t *testing.T) {
	peers := map[int]string{
		1: "127.0.0.1:18001",
		2: "127.0.0.1:18002",
	}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := raft.NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.Start()
	defer node.Stop()

	sendBinary := func(addr string, data []byte) error { return nil }

	cli := NewCLI(node, sendBinary)

	if cli == nil {
		t.Fatal("expected CLI instance, got nil")
	}
	if cli.node == nil {
		t.Fatal("expected node to be set")
	}
	if cli.sendBinary == nil {
		t.Fatal("expected sendBinary to be set")
	}
}

// TestCmdStatus tests status command
func TestCmdStatus(t *testing.T) {
	peers := map[int]string{
		1: "127.0.0.1:18001",
		2: "127.0.0.1:18002",
	}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := raft.NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.Start()
	defer node.Stop()

	cli := &CLI{node: node}

	// Should not return error
	err := cli.cmdStatus()
	if err != nil {
		t.Errorf("cmdStatus returned error: %v", err)
	}
}

// TestCmdLeader tests leader command
func TestCmdLeader(t *testing.T) {
	peers := map[int]string{
		1: "127.0.0.1:18001",
		2: "127.0.0.1:18002",
	}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := raft.NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.Start()
	defer node.Stop()

	cli := &CLI{node: node}

	err := cli.cmdLeader()
	if err != nil {
		t.Errorf("cmdLeader returned error: %v", err)
	}
}

// TestCmdGet tests get command
func TestCmdGet(t *testing.T) {
	peers := map[int]string{
		1: "127.0.0.1:18001",
		2: "127.0.0.1:18002",
	}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	node := raft.NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.Start()
	defer node.Stop()

	cli := &CLI{node: node}

	// Get non-existent key
	err := cli.cmdGet("nonexistent")
	if err != nil {
		t.Errorf("cmdGet returned error: %v", err)
	}
}

// TestCmdChaos tests chaos command parsing
func TestCmdChaos(t *testing.T) {
	tests := []struct {
		name        string
		arg         string
		expectError bool
	}{
		{"valid 0.0", "loss=0.0", false},
		{"valid 0.3", "loss=0.3", false},
		{"valid 1.0", "loss=1.0", false},
		{"valid 0.75", "loss=0.75", false},
		{"invalid format", "loss", true},
		{"missing loss", "0.5", true},
		{"out of range high", "loss=1.5", true},
		{"out of range low", "loss=-0.1", true},
		{"invalid number", "loss=abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cli := &CLI{}
			err := cli.cmdChaos(tt.arg)
			if tt.expectError && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestExecuteCommand tests command routing
func TestExecuteCommand(t *testing.T) {
	peers := map[int]string{
		1: "127.0.0.1:18001",
		2: "127.0.0.1:18002",
	}
	sendFunc := func(addr string, msg interface{}) error { return nil }
	sendBinary := func(addr string, data []byte) error { return nil }

	node := raft.NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.Start()
	defer node.Stop()

	cli := NewCLI(node, sendBinary)

	tests := []struct {
		name    string
		line    string
		wantErr bool
	}{
		{"status", "/status", false},
		{"get", "/get foo", false},
		{"leader", "/leader", false},
		{"chaos", "/chaos loss=0.5", false},
		{"empty", "", false},
		{"unknown", "/unknown", true},
		{"get missing args", "/get", true},
		{"chaos missing args", "/chaos", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cli.executeCommand(tt.line)
			if tt.wantErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestSetCommandOnFollower tests set command behavior
// TestSetCommandOnFollower tests set command behavior
func TestSetCommandOnFollower(t *testing.T) {
	peers := map[int]string{
		1: "127.0.0.1:18001",
		2: "127.0.0.1:18002",
	}
	sendFunc := func(addr string, msg interface{}) error { return nil }

	sendBinary := func(addr string, data []byte) error {
		// Just verify it doesn't panic
		// lastSentAddr = addr  // Remove or uncomment if needed
		return nil
	}

	node := raft.NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.Start()
	defer node.Stop()

	cli := NewCLI(node, sendBinary)

	// Try to set a value (should try to redirect or return error)
	err := cli.cmdSet("testkey", "testvalue")

	// It's OK if it returns error (no leader yet) or succeeds
	// Just verify it doesn't panic
	if err != nil {
		t.Logf("set command returned: %v (expected if no leader)", err)
	}
}

// TestCLIWithRealRaftCluster tests CLI with realistic scenario
func TestCLIWithRealRaftCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	peers := map[int]string{
		1: "127.0.0.1:19001",
		2: "127.0.0.1:19002",
		3: "127.0.0.1:19003",
	}

	sendFunc := func(addr string, msg interface{}) error { return nil }
	sendBinary := func(addr string, data []byte) error { return nil }

	// Create multiple nodes (but only test one)
	node := raft.NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.Start()
	defer node.Stop()

	cli := NewCLI(node, sendBinary)

	// Test all commands
	commands := []string{
		"/status",
		"/leader",
		"/get test",
		"/chaos loss=0.3",
	}

	for _, cmd := range commands {
		t.Run(cmd, func(t *testing.T) {
			err := cli.executeCommand(cmd)
			if err != nil {
				t.Errorf("command %q failed: %v", cmd, err)
			}
		})
	}
}

// BenchmarkCLIExecuteCommand benchmarks command execution
func BenchmarkCLIExecuteCommand(b *testing.B) {
	peers := map[int]string{
		1: "127.0.0.1:18001",
		2: "127.0.0.1:18002",
	}
	sendFunc := func(addr string, msg interface{}) error { return nil }
	sendBinary := func(addr string, data []byte) error { return nil }

	node := raft.NewRaftNode(1, peers, b.TempDir(), sendFunc)
	node.Start()
	defer node.Stop()

	cli := NewCLI(node, sendBinary)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cli.executeCommand("/status")
	}
}
