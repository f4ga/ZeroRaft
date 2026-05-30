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

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"zeroraft/internal/client"
	"zeroraft/internal/raft"
)

func main() {
	id := flag.Int("id", 0, "node id (1-3)")
	addr := flag.String("addr", "", "listen address (e.g., 127.0.0.1:8001)")
	peersStr := flag.String("peers", "", "comma-separated list of peers (format: id=addr,id=addr)")
	dataDir := flag.String("data-dir", "/tmp/zeroraft", "data directory for persistence")
	flag.Parse()

	if *id == 0 {
		log.Fatal("--id is required")
	}
	if *addr == "" {
		log.Fatal("--addr is required")
	}
	if *peersStr == "" {
		log.Fatal("--peers is required")
	}

	// Parse peers
	peers, err := parsePeers(*peersStr)
	if err != nil {
		log.Fatalf("failed to parse peers: %v", err)
	}
	if _, ok := peers[*id]; !ok {
		log.Fatalf("node %d not found in peers list", *id)
	}

	// Create data directory
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("failed to create data directory: %v", err)
	}

	// Create send function for raft (interface{} type)
	raftSendFunc := func(addr string, msg interface{}) error {
		return fmt.Errorf("raft transport not implemented yet (would send to %s: %+v)", addr, msg)
	}

	// Create Raft node
	node := raft.NewRaftNode(*id, peers, *dataDir, raftSendFunc)
	node.Start()

	// Create send function for CLI ([]byte type)
	cliSendFunc := func(addr string, data []byte) error {
		return fmt.Errorf("cli transport not implemented yet (would send to %s: %s)", addr, string(data))
	}

	// Create CLI
	cli := client.NewCLI(node, cliSendFunc)

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nShutting down...")
		node.Stop()
		os.Exit(0)
	}()

	// Run CLI
	if err := cli.Run(); err != nil {
		log.Fatalf("CLI error: %v", err)
	}
}

func parsePeers(peersStr string) (map[int]string, error) {
	peers := make(map[int]string)
	parts := strings.Split(peersStr, ",")
	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid peer format: %s", part)
		}
		id, err := strconv.Atoi(kv[0])
		if err != nil {
			return nil, fmt.Errorf("invalid peer id: %s", kv[0])
		}
		peers[id] = kv[1]
	}
	return peers, nil
}
