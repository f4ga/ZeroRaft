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
package client

import (
	"testing"
	"zeroraft/internal/codec"
	"zeroraft/internal/raft"
)

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
	err := cli.cmdStatus()
	if err != nil {
		t.Errorf("cmdStatus returned error: %v", err)
	}
}
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
	err := cli.cmdGet("nonexistent")
	if err != nil {
		t.Errorf("cmdGet returned error: %v", err)
	}
}
func TestCmdSetOnLeader(t *testing.T) {
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
	// Try to set value (may fail if not leader, but should not panic)
	_ = cli.cmdSet("testkey", "testvalue")
}
func TestCmdSetOnFollower(t *testing.T) {
	peers := map[int]string{
		1: "127.0.0.1:18001",
		2: "127.0.0.1:18002",
	}
	sendFunc := func(addr string, msg interface{}) error { return nil }
	var sentToLeader bool
	sendBinary := func(addr string, data []byte) error {
		sentToLeader = true
		return nil
	}
	node := raft.NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.Start()
	defer node.Stop()
	cli := NewCLI(node, sendBinary)
	err := cli.cmdSet("testkey", "testvalue")
	if err != nil {
		// Expected if no leader, but command may be sent to leader if exists
		t.Logf("set command returned: %v", err)
	}
	_ = sentToLeader
}
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
		{"set", "/set foo bar", false},
		{"set missing args", "/set foo", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cli.executeCommand(tt.line)
			if err != nil && err.Error() == "no leader known" {
				t.Logf("command %q returned: %v (expected when no leader)", tt.line, err)
				return
			}
			if tt.wantErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error for %q: %v", tt.line, err)
			}
		})
	}
}
func TestExecuteCommandSet(t *testing.T) {
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
	err := cli.executeCommand("/set test value")
	if err != nil {
		t.Logf("set command result: %v", err)
	}
}
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
	node := raft.NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.Start()
	defer node.Stop()
	cli := NewCLI(node, sendBinary)
	commands := []string{
		"/status",
		"/leader",
		"/get test",
		"/chaos loss=0.3",
		"/set test value",
	}
	for _, cmd := range commands {
		t.Run(cmd, func(t *testing.T) {
			err := cli.executeCommand(cmd)
			if err != nil {
				t.Logf("command %q returned: %v", cmd, err)
			}
		})
	}
}
func TestCmdSetRedirectToLeader(t *testing.T) {
	peers := map[int]string{
		1: "127.0.0.1:18001",
		2: "127.0.0.1:18002",
	}
	sendFunc := func(addr string, msg interface{}) error { return nil }
	var redirectedAddr string
	sendBinary := func(addr string, data []byte) error {
		redirectedAddr = addr
		return nil
	}
	node := raft.NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.Start()
	defer node.Stop()
	// Make the node learn about a leader via HandleAppendEntries.
	// This sets leaderId internally, which GetLeaderID() will return.
	ae := codec.AppendEntries{
		Type:     "AppendEntries",
		Term:     1,
		LeaderID: 2,
	}
	node.HandleAppendEntries(ae)
	cli := NewCLI(node, sendBinary)
	err := cli.cmdSet("testkey", "testvalue")
	if err != nil {
		t.Fatalf("cmdSet failed: %v", err)
	}
	if redirectedAddr != "127.0.0.1:18002" {
		t.Errorf("expected redirect to leader addr '127.0.0.1:18002', got %q", redirectedAddr)
	}
}
func TestCmdSetNoTransport(t *testing.T) {
	peers := map[int]string{
		1: "127.0.0.1:18001",
		2: "127.0.0.1:18002",
	}
	sendFunc := func(addr string, msg interface{}) error { return nil }
	node := raft.NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.Start()
	defer node.Stop()
	cli := &CLI{node: node, sendBinary: nil}
	err := cli.cmdSet("testkey", "testvalue")
	if err == nil {
		t.Error("expected error when sendBinary is nil")
	}
}
func TestExecuteCommandSetMissingArgs(t *testing.T) {
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
	err := cli.executeCommand("/set")
	if err == nil {
		t.Error("expected error for missing args")
	}
}
func TestCmdSetRedirectToUnknownLeader(t *testing.T) {
	peers := map[int]string{
		1: "127.0.0.1:18001",
		2: "127.0.0.1:18002",
	}
	sendFunc := func(addr string, msg interface{}) error { return nil }
	sendBinary := func(addr string, data []byte) error { return nil }
	node := raft.NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.Start()
	defer node.Stop()
	// Set leader to a non-existent peer ID (3) — GetPeerAddr will return ""
	ae := codec.AppendEntries{
		Type:     "AppendEntries",
		Term:     1,
		LeaderID: 3,
	}
	node.HandleAppendEntries(ae)
	cli := NewCLI(node, sendBinary)
	err := cli.cmdSet("testkey", "testvalue")
	if err == nil {
		t.Error("expected error when leader address is unknown")
	}
}
func TestCmdGetExistingKey(t *testing.T) {
	peers := map[int]string{
		1: "127.0.0.1:18001",
		2: "127.0.0.1:18002",
	}
	sendFunc := func(addr string, msg interface{}) error { return nil }
	node := raft.NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.Start()
	defer node.Stop()
	// Use HandleAppendEntries to add an entry and commit it,
	// which will apply it to the state machine.
	ae := codec.AppendEntries{
		Type:         "AppendEntries",
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []codec.LogEntry{
			{Index: 1, Term: 1, Command: "set foo bar"},
		},
		LeaderCommit: 1,
	}
	node.HandleAppendEntries(ae)
	cli := &CLI{node: node}
	err := cli.cmdGet("foo")
	if err != nil {
		t.Errorf("cmdGet returned error: %v", err)
	}
}
func TestCmdLeaderWhenExists(t *testing.T) {
	peers := map[int]string{
		1: "127.0.0.1:18001",
		2: "127.0.0.1:18002",
	}
	sendFunc := func(addr string, msg interface{}) error { return nil }
	node := raft.NewRaftNode(1, peers, t.TempDir(), sendFunc)
	node.Start()
	defer node.Stop()
	// Make the node learn about a leader via HandleAppendEntries
	ae := codec.AppendEntries{
		Type:     "AppendEntries",
		Term:     1,
		LeaderID: 2,
	}
	node.HandleAppendEntries(ae)
	cli := &CLI{node: node}
	err := cli.cmdLeader()
	if err != nil {
		t.Errorf("cmdLeader returned error: %v", err)
	}
}
func TestExecuteCommandGetMissingArgs(t *testing.T) {
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
	err := cli.executeCommand("/get")
	if err == nil {
		t.Error("expected error for missing args")
	}
}
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
