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
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"zeroraft/internal/raft"
)

// CLI represents the command-line interface.
type CLI struct {
	node       *raft.RaftNode
	sendBinary func(addr string, data []byte) error
}

// NewCLI creates a new CLI instance.
func NewCLI(node *raft.RaftNode, sendBinary func(addr string, data []byte) error) *CLI {
	return &CLI{
		node:       node,
		sendBinary: sendBinary,
	}
}

// Run starts the interactive CLI loop.
func (c *CLI) Run() error {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("ZeroRaft CLI")
	fmt.Println("Available commands:")
	fmt.Println("  /status                    - show node status")
	fmt.Println("  /set <key> <value>         - set a key-value pair")
	fmt.Println("  /get <key>                 - get value for key")
	fmt.Println("  /leader                    - show current leader")
	fmt.Println("  /chaos loss=<0.0-1.0>      - set packet loss probability")
	fmt.Println("  /exit                      - exit CLI")
	fmt.Println()

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "/exit" {
			fmt.Println("Goodbye!")
			return nil
		}
		if err := c.executeCommand(line); err != nil {
			fmt.Printf("Error: %v\n", err)
		}
	}
	return scanner.Err()
}

// executeCommand parses and executes a command.
func (c *CLI) executeCommand(line string) error {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return nil
	}

	switch parts[0] {
	case "/status":
		return c.cmdStatus()
	case "/set":
		if len(parts) < 3 {
			return fmt.Errorf("usage: /set <key> <value>")
		}
		return c.cmdSet(parts[1], strings.Join(parts[2:], " "))
	case "/get":
		if len(parts) != 2 {
			return fmt.Errorf("usage: /get <key>")
		}
		return c.cmdGet(parts[1])
	case "/leader":
		return c.cmdLeader()
	case "/chaos":
		if len(parts) != 2 {
			return fmt.Errorf("usage: /chaos loss=<0.0-1.0>")
		}
		return c.cmdChaos(parts[1])
	default:
		return fmt.Errorf("unknown command: %s", parts[0])
	}
}

// cmdStatus displays node status.
func (c *CLI) cmdStatus() error {
	state := c.node.GetState()
	term := c.node.GetCurrentTerm()
	leaderID := c.node.GetLeaderID()
	commitIndex := c.node.GetCommitIndex()

	fmt.Printf("State: %s\n", state)
	fmt.Printf("Term: %d\n", term)
	fmt.Printf("Leader: node %d\n", leaderID)
	fmt.Printf("Commit Index: %d\n", commitIndex)
	return nil
}

// cmdSet sets a key-value pair.
func (c *CLI) cmdSet(key, value string) error {
	// Check if this node is leader
	if c.node.GetState() == raft.Leader {
		_, err := c.node.Submit(fmt.Sprintf("set %s %s", key, value))
		return err
	}

	// Redirect to leader
	leaderID := c.node.GetLeaderID()
	if leaderID == -1 {
		return fmt.Errorf("no leader known")
	}
	leaderAddr := c.node.GetPeerAddr(leaderID)
	if leaderAddr == "" {
		return fmt.Errorf("leader address unknown")
	}

	// Send command to leader via UDP
	cmd := fmt.Sprintf("SET %s %s\n", key, value)
	if c.sendBinary != nil {
		return c.sendBinary(leaderAddr, []byte(cmd))
	}
	return fmt.Errorf("transport not available")
}

// cmdGet gets a value by key.
func (c *CLI) cmdGet(key string) error {
	value, ok := c.node.GetStateMachineValue(key)
	if !ok {
		fmt.Printf("Key '%s' not found\n", key)
		return nil
	}
	fmt.Printf("%s\n", value)
	return nil
}

// cmdLeader shows current leader.
func (c *CLI) cmdLeader() error {
	leaderID := c.node.GetLeaderID()
	if leaderID == -1 {
		fmt.Println("No leader elected yet")
		return nil
	}
	leaderAddr := c.node.GetPeerAddr(leaderID)
	fmt.Printf("Leader: node %d (%s)\n", leaderID, leaderAddr)
	return nil
}

// cmdChaos sets packet loss probability.
func (c *CLI) cmdChaos(arg string) error {
	// Parse "loss=0.5"
	if !strings.HasPrefix(arg, "loss=") {
		return fmt.Errorf("invalid format, use loss=<value>")
	}
	valStr := strings.TrimPrefix(arg, "loss=")
	loss, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return fmt.Errorf("invalid loss value: %v", err)
	}
	if loss < 0 || loss > 1 {
		return fmt.Errorf("loss must be between 0.0 and 1.0")
	}
	// Note: SetDropProbability will be added in Phase 8
	fmt.Printf("Packet loss probability set to %.2f (will be implemented in Phase 8)\n", loss)
	return nil
}
