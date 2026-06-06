// Copyright 2026 Ekaterina Godulyan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
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
	"net/http"
	_ "net/http/pprof" // enable pprof endpoints
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"zeroraft/internal/client"
	"zeroraft/internal/codec"
	"zeroraft/internal/raft"
	"zeroraft/internal/transport"
)

func main() {
	id := flag.Int("id", 0, "node id (1-3)")
	addr := flag.String("addr", "", "listen address (e.g., 127.0.0.1:8001)")
	peersStr := flag.String("peers", "", "comma-separated list of peers (format: id=addr,id=addr)")
	dataDir := flag.String("data-dir", "/tmp/zeroraft", "data directory for persistence")
	pcapFile := flag.String("pcap", "", "path to PCAP file for packet capture (optional)")
	pprofAddr := flag.String("pprof", ":6060", "pprof server address (e.g., :6060)")
	healthFlag := flag.Bool("health", false, "healthcheck mode: exit 0 if healthy, 1 otherwise")
	flag.Parse()

	if *healthFlag {
		// Healthcheck mode: create node, check health, exit.
		// We need peers and addr to create a node, but in healthcheck mode
		// we only check local state. Use defaults if not provided.
		if *addr == "" {
			*addr = "0.0.0.0:0"
		}
		if *peersStr == "" {
			*peersStr = ""
		}
		if *id == 0 {
			*id = 1
		}
		peers, err := parsePeers(*peersStr)
		if err != nil {
			peers = map[int]string{}
		}
		if err := os.MkdirAll(*dataDir, 0755); err != nil {
			os.Exit(1)
		}
		sendFunc := func(addr string, msg interface{}) error { return nil }
		node := raft.NewRaftNode(*id, peers, *dataDir, sendFunc)
		node.Start()
		// Give the node a moment to initialize
		time.Sleep(50 * time.Millisecond)
		if node.IsHealthy() {
			node.Stop()
			os.Exit(0)
		}
		node.Stop()
		os.Exit(1)
	}

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

	// Create data directory
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("failed to create data directory: %v", err)
	}

	// Start pprof server (RT-28)
	go func() {
		log.Printf("Starting pprof server on %s", *pprofAddr)
		if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
			log.Printf("pprof server error: %v", err)
		}
	}()

	// Initialize PCAP writer if requested (BS-04)
	var pcapWriter *transport.PcapWriter
	if *pcapFile != "" {
		pw, err := transport.NewPcapWriter(*pcapFile)
		if err != nil {
			log.Fatalf("failed to create PCAP file: %v", err)
		}
		pcapWriter = pw
		log.Printf("PCAP recording enabled, writing to %s", *pcapFile)
	}

	// Create raw UDP socket (RT-01, RT-02)
	fd, err := transport.NewRawSocket(*addr)
	if err != nil {
		log.Fatalf("failed to create UDP socket: %v", err)
	}
	defer func() {
		if err := transport.CloseSocket(fd); err != nil {
			log.Printf("error closing socket: %v", err)
		}
	}()
	log.Printf("UDP socket created on %s (fd=%d)", *addr, fd)

	// raftSendFunc sends an RPC message to a peer over UDP.
	raftSendFunc := func(peerAddr string, msg interface{}) error {
		raddr, err := transport.ResolveAddr(peerAddr)
		if err != nil {
			return fmt.Errorf("resolve addr %s: %v", peerAddr, err)
		}
		data, err := codec.Encode(msg)
		if err != nil {
			return fmt.Errorf("encode: %v", err)
		}
		if pcapWriter != nil {
			_ = pcapWriter.WritePacket(data)
		}
		return transport.SendTo(fd, data, raddr)
	}

	// Create Raft node
	node := raft.NewRaftNode(*id, peers, *dataDir, raftSendFunc)
	node.Start()

	// recvLoop receives UDP datagrams and dispatches them to Raft handlers.
	go func() {
		for {
			data, from, err := transport.RecvFrom(fd)
			if err != nil {
				// Temporary error, continue
				continue
			}
			if pcapWriter != nil {
				_ = pcapWriter.WritePacket(data)
			}

			// Build sender address string for responses
			fromAddr := fmt.Sprintf("%d.%d.%d.%d:%d", from.Addr[0], from.Addr[1], from.Addr[2], from.Addr[3], from.Port)

			// Try to decode as RPC message first
			msg, err := codec.Decode(data)
			if err != nil {
				// Not an RPC message — might be a CLI text command forwarded to leader.
				// Try to handle as raw text command (SET key value).
				text := strings.TrimSpace(string(data))
				if strings.HasPrefix(text, "SET ") {
					rest := strings.TrimPrefix(text, "SET ")
					parts := strings.SplitN(rest, " ", 2)
					if len(parts) == 2 {
						_, err := node.Submit(fmt.Sprintf("set %s %s", parts[0], parts[1]))
						if err != nil {
							log.Printf("error processing forwarded SET from %s: %v", fromAddr, err)
						}
					}
				}
				continue
			}

			// Dispatch RPC message by type
			switch m := msg.(type) {
			case codec.RequestVote:
				resp := node.HandleRequestVote(m)
				_ = raftSendFunc(fromAddr, resp)
			case codec.RequestVoteResponse:
				// Find peer ID by address for the response handler
				peerID := node.FindPeerIDByAddr(fromAddr)
				node.HandleRequestVoteResponse(peerID, m)
			case codec.AppendEntries:
				resp := node.HandleAppendEntries(m)
				_ = raftSendFunc(fromAddr, resp)
			case codec.AppendEntriesResponse:
				peerID := node.FindPeerIDByAddr(fromAddr)
				node.HandleAppendEntriesResponse(peerID, m)
			default:
				log.Printf("unknown message type from %s", fromAddr)
			}
		}
	}()

	// cliSendFunc sends raw text commands to a peer over UDP (for leader forwarding).
	cliSendFunc := func(addr string, data []byte) error {
		raddr, err := transport.ResolveAddr(addr)
		if err != nil {
			return fmt.Errorf("resolve addr %s: %v", addr, err)
		}
		if pcapWriter != nil {
			_ = pcapWriter.WritePacket(data)
		}
		return transport.SendTo(fd, data, raddr)
	}

	// Create CLI
	cli := client.NewCLI(node, cliSendFunc)

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nShutting down...")
		if pcapWriter != nil {
			_ = pcapWriter.Close()
		}
		node.Stop()
		os.Exit(0)
	}()

	// Run CLI
	if err := cli.Run(); err != nil {
		log.Printf("CLI error: %v", err)
		if pcapWriter != nil {
			_ = pcapWriter.Close()
		}
		node.Stop()
		_ = transport.CloseSocket(fd)
		//nolint:gocritic // resources already closed explicitly
		os.Exit(1)
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
