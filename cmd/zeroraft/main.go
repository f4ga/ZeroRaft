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
	"bufio"
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

	"golang.org/x/term"

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
	cliFlag := flag.Bool("cli", false, "client mode: read commands from stdin, send via UDP, exit after /exit")
	flag.Parse()

	if *healthFlag {
		// Healthcheck mode: create node, check health, exit.
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
		time.Sleep(500 * time.Millisecond)
		if node.IsHealthy() {
			node.Stop()
			os.Exit(0)
		}
		node.Stop()
		os.Exit(1)
	}

	if *cliFlag {
		// Client mode: read commands from stdin, send to target node via UDP,
		// wait for response, print it, and exit after /exit.
		if *addr == "" {
			log.Fatal("--addr is required in client mode (target node address)")
		}

		// Resolve target address
		targetAddr, err := transport.ResolveAddr(*addr)
		if err != nil {
			log.Fatalf("failed to resolve target address: %v", err)
		}

		// Create a temporary UDP socket bound to a random port
		clientFd, err := transport.NewRawSocket("0.0.0.0:0")
		if err != nil {
			log.Fatalf("failed to create client socket: %v", err)
		}
		defer func() {
			if err := transport.CloseSocket(clientFd); err != nil {
				log.Printf("error closing client socket: %v", err)
			}
		}()

		// Read all stdin commands
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			if line == "/exit" {
				break
			}

			// Convert CLI command to protocol command
			var payload string
			if strings.HasPrefix(line, "/set ") {
				parts := strings.SplitN(line[5:], " ", 2)
				if len(parts) != 2 {
					fmt.Fprintf(os.Stderr, "Error: invalid /set command\n")
					continue
				}
				payload = fmt.Sprintf("SET %s %s\n", parts[0], parts[1])
			} else if strings.HasPrefix(line, "/get ") {
				key := strings.TrimSpace(line[5:])
				payload = fmt.Sprintf("GET %s\n", key)
			} else if line == "/status" {
				payload = "STATUS\n"
			} else if line == "/leader" {
				payload = "LEADER\n"
			} else {
				fmt.Fprintf(os.Stderr, "Error: unsupported command in client mode: %s\n", line)
				continue
			}

			// Send UDP packet to target node
			if err := transport.SendTo(clientFd, []byte(payload), targetAddr); err != nil {
				fmt.Fprintf(os.Stderr, "Error sending: %v\n", err)
				continue
			}

			// Wait for response with timeout
			errCh := make(chan error, 1)
			dataCh := make(chan []byte, 1)
			go func() {
				data, _, err := transport.RecvFrom(clientFd)
				if err != nil {
					errCh <- err
					return
				}
				dataCh <- data
			}()
			select {
			case data := <-dataCh:
				fmt.Print(string(data))
			case err := <-errCh:
				fmt.Fprintf(os.Stderr, "Error receiving: %v\n", err)
			case <-time.After(2 * time.Second):
				fmt.Fprintf(os.Stderr, "Timeout waiting for response\n")
			}
		}
		return
	}

	// Normal server mode
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
	// Also handles plain text commands (SET, GET, STATUS, LEADER) with responses.
	go func() {
		for {
			data, from, err := transport.RecvFrom(fd)
			if err != nil {
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
				// Not an RPC message — try plain text command.
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
				} else if strings.HasPrefix(text, "GET ") {
					key := strings.TrimSpace(text[4:])
					val, ok := node.GetStateMachineValue(key)
					resp := fmt.Sprintf("value: %s\n", val)
					if !ok {
						resp = "not found\n"
					}
					go func(addr string, respData []byte) {
						raddr, _ := transport.ResolveAddr(addr)
						_ = transport.SendTo(fd, respData, raddr)
					}(fromAddr, []byte(resp))
				} else if text == "STATUS" {
					state := node.GetState()
					term := node.GetCurrentTerm()
					leaderID := node.GetLeaderID()
					commitIndex := node.GetCommitIndex()
					resp := fmt.Sprintf("State: %s\nTerm: %d\nLeader: node %d\nCommit Index: %d\n", state, term, leaderID, commitIndex)
					go func(addr string, respData []byte) {
						raddr, _ := transport.ResolveAddr(addr)
						_ = transport.SendTo(fd, respData, raddr)
					}(fromAddr, []byte(resp))
				} else if text == "LEADER" {
					leaderID := node.GetLeaderID()
					leaderAddr := node.GetPeerAddr(leaderID)
					resp := fmt.Sprintf("Leader: node %d (%s)\n", leaderID, leaderAddr)
					go func(addr string, respData []byte) {
						raddr, _ := transport.ResolveAddr(addr)
						_ = transport.SendTo(fd, respData, raddr)
					}(fromAddr, []byte(resp))
				}
				continue
			}

			// Dispatch RPC message by type
			switch m := msg.(type) {
			case codec.RequestVote:
				resp := node.HandleRequestVote(m)
				_ = raftSendFunc(fromAddr, resp)
			case codec.RequestVoteResponse:
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

	// Run CLI only in interactive terminal mode.
	// In non-interactive environments (e.g., Docker containers without a TTY),
	// cli.Run() would immediately receive EOF on stdin and exit, causing the
	// process to terminate and Docker to restart the container in a loop.
	if term.IsTerminal(int(os.Stdin.Fd())) {
		cli := client.NewCLI(node, cliSendFunc)
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
	} else {
		log.Println("Running in non-interactive mode (no CLI)")
		// Block indefinitely — the signal handler above will call os.Exit(0)
		// on SIGINT/SIGTERM, so this goroutine will never outlive the process.
		select {}
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
