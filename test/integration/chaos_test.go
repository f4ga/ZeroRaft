// ... copyright header ...

package integration

import (
	"sync"
	"testing"
	"time"

	"zeroraft/internal/codec"
	"zeroraft/internal/raft"
	"zeroraft/internal/transport"
)

// Тип message уже определён в cluster_test.go – используем его.

type chaosRouter struct {
	mu        sync.Mutex
	nodes     map[int]*raft.RaftNode
	addresses map[int]string
	inbox     chan message
	stopCh    chan struct{}
}

func newChaosRouter() *chaosRouter {
	r := &chaosRouter{
		nodes:     make(map[int]*raft.RaftNode),
		addresses: map[int]string{1: "node1", 2: "node2", 3: "node3"},
		inbox:     make(chan message, 10000),
		stopCh:    make(chan struct{}),
	}
	go r.deliveryLoop()
	return r
}

func (r *chaosRouter) Register(id int, node *raft.RaftNode) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes[id] = node
}

// Send – отправляет сообщение в очередь.
func (r *chaosRouter) Send(addr string, msg interface{}) error {
	r.inbox <- message{fromID: 0, toAddr: addr, msg: msg}
	return nil
}

func (r *chaosRouter) deliveryLoop() {
	for {
		select {
		case msg := <-r.inbox:
			if transport.ShouldDrop() {
				continue
			}
			r.deliver(msg)
		case <-r.stopCh:
			return
		}
	}
}

func (r *chaosRouter) deliver(msg message) {
	r.mu.Lock()
	addrCopy := make(map[int]string, len(r.addresses))
	for k, v := range r.addresses {
		addrCopy[k] = v
	}
	nodeCopy := make(map[int]*raft.RaftNode, len(r.nodes))
	for k, v := range r.nodes {
		nodeCopy[k] = v
	}
	r.mu.Unlock()

	var targetID int
	switch msg.toAddr {
	case "node1":
		targetID = 1
	case "node2":
		targetID = 2
	case "node3":
		targetID = 3
	default:
		return
	}

	target, ok := nodeCopy[targetID]
	if !ok {
		return
	}

	switch m := msg.msg.(type) {
	case codec.RequestVote:
		resp := target.HandleRequestVote(m)
		if fromAddr, ok := addrCopy[m.CandidateID]; ok && fromAddr != "" {
			go func(addr string, resp interface{}) {
				_ = r.Send(addr, resp)
			}(fromAddr, resp)
		}
	case codec.RequestVoteResponse:
		target.HandleRequestVoteResponse(-1, m)
	case codec.AppendEntries:
		resp := target.HandleAppendEntries(m)
		if fromAddr, ok := addrCopy[m.LeaderID]; ok && fromAddr != "" {
			go func(addr string, resp interface{}) {
				_ = r.Send(addr, resp)
			}(fromAddr, resp)
		}
	case codec.AppendEntriesResponse:
		target.HandleAppendEntriesResponse(-1, m)
	}
}

func (r *chaosRouter) Stop() {
	close(r.stopCh)
}

// TestChaosClusterWithPacketLoss проверяет работу кластера при 30% потерь.
func TestChaosClusterWithPacketLoss(t *testing.T) {
	transport.SetDropProbability(0.3)
	defer transport.SetDropProbability(0.0)
	router := newChaosRouter()
	defer router.Stop()
	peers := map[int]string{1: "node1", 2: "node2", 3: "node3"}
	node1 := raft.NewRaftNode(1, peers, t.TempDir(), router.Send)
	node2 := raft.NewRaftNode(2, peers, t.TempDir(), router.Send)
	node3 := raft.NewRaftNode(3, peers, t.TempDir(), router.Send)
	router.Register(1, node1)
	router.Register(2, node2)
	router.Register(3, node3)
	node1.Start()
	node2.Start()
	node3.Start()
	defer node1.Stop()
	defer node2.Stop()
	defer node3.Stop()
	timeout := time.After(20 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var leaderNode *raft.RaftNode
leaderLoop:
	for {
		select {
		case <-timeout:
			t.Fatalf("no leader elected. Node states: 1=%s 2=%s 3=%s",
				node1.GetState(), node2.GetState(), node3.GetState())
		case <-ticker.C:
			switch {
			case node1.GetState() == raft.Leader:
				leaderNode = node1
			case node2.GetState() == raft.Leader:
				leaderNode = node2
			case node3.GetState() == raft.Leader:
				leaderNode = node3
			}
			if leaderNode != nil {
				t.Logf("Leader elected (node %d) with 30%% packet loss", leaderNode.GetLeaderID())
				break leaderLoop
			}
		}
	}
	const numCommands = 10
	for i := 0; i < numCommands; i++ {
		if _, err := leaderNode.Submit("set chaos_key chaos_value"); err != nil {
			t.Fatalf("Submit failed: %v", err)
		}
	}
	time.Sleep(5 * time.Second)
	for id, node := range map[int]*raft.RaftNode{1: node1, 2: node2, 3: node3} {
		val, ok := node.GetStateMachineValue("chaos_key")
		if !ok || val != "chaos_value" {
			t.Errorf("Node %d: expected 'chaos_value', got %q (ok=%v)", id, val, ok)
		}
	}
	t.Log("All nodes consistent under 30% loss")
}

// TestChaosClusterNoLoss — baseline без потерь.
func TestChaosClusterNoLoss(t *testing.T) {
	transport.SetDropProbability(0.0)
	router := newChaosRouter()
	defer router.Stop()
	peers := map[int]string{1: "node1", 2: "node2", 3: "node3"}
	node1 := raft.NewRaftNode(1, peers, t.TempDir(), router.Send)
	node2 := raft.NewRaftNode(2, peers, t.TempDir(), router.Send)
	node3 := raft.NewRaftNode(3, peers, t.TempDir(), router.Send)
	router.Register(1, node1)
	router.Register(2, node2)
	router.Register(3, node3)
	node1.Start()
	node2.Start()
	node3.Start()
	defer node1.Stop()
	defer node2.Stop()
	defer node3.Stop()
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	var leaderNode *raft.RaftNode
leaderLoop:
	for {
		select {
		case <-timeout:
			t.Fatal("no leader elected (0% loss)")
		case <-ticker.C:
			switch {
			case node1.GetState() == raft.Leader:
				leaderNode = node1
			case node2.GetState() == raft.Leader:
				leaderNode = node2
			case node3.GetState() == raft.Leader:
				leaderNode = node3
			}
			if leaderNode != nil {
				t.Logf("Leader elected (node %d) without loss", leaderNode.GetLeaderID())
				break leaderLoop
			}
		}
	}
	for i := 0; i < 10; i++ {
		if _, err := leaderNode.Submit("set no_loss_key value"); err != nil {
			t.Fatalf("Submit failed: %v", err)
		}
	}
	time.Sleep(3 * time.Second)
	for id, node := range map[int]*raft.RaftNode{1: node1, 2: node2, 3: node3} {
		val, ok := node.GetStateMachineValue("no_loss_key")
		if !ok || val != "value" {
			t.Errorf("Node %d: expected 'value', got %q", id, val)
		}
	}
}

// TestChaosConcurrentClients — конкурентная отправка при 30% потерь.
func TestChaosConcurrentClients(t *testing.T) {
	transport.SetDropProbability(0.3)
	defer transport.SetDropProbability(0.0)
	router := newChaosRouter()
	defer router.Stop()
	peers := map[int]string{1: "node1", 2: "node2", 3: "node3"}
	node1 := raft.NewRaftNode(1, peers, t.TempDir(), router.Send)
	node2 := raft.NewRaftNode(2, peers, t.TempDir(), router.Send)
	node3 := raft.NewRaftNode(3, peers, t.TempDir(), router.Send)
	router.Register(1, node1)
	router.Register(2, node2)
	router.Register(3, node3)
	node1.Start()
	node2.Start()
	node3.Start()
	defer node1.Stop()
	defer node2.Stop()
	defer node3.Stop()
	timeout := time.After(20 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var leaderNode *raft.RaftNode
leaderLoop:
	for {
		select {
		case <-timeout:
			t.Fatal("no leader elected for concurrent test")
		case <-ticker.C:
			switch {
			case node1.GetState() == raft.Leader:
				leaderNode = node1
			case node2.GetState() == raft.Leader:
				leaderNode = node2
			case node3.GetState() == raft.Leader:
				leaderNode = node3
			}
			if leaderNode != nil {
				break leaderLoop
			}
		}
	}
	t.Log("Leader elected, starting concurrent clients")
	const numClients = 5
	const commandsPerClient = 20
	var wg sync.WaitGroup
	errCh := make(chan error, numClients*commandsPerClient)
	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			for i := 0; i < commandsPerClient; i++ {
				cmd := "set concurrent_key concurrent_value"
				if _, err := leaderNode.Submit(cmd); err != nil {
					errCh <- err
					return
				}
			}
		}(c)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent submit failed: %v", err)
	}
	time.Sleep(5 * time.Second)
	for id, node := range map[int]*raft.RaftNode{1: node1, 2: node2, 3: node3} {
		val, ok := node.GetStateMachineValue("concurrent_key")
		if !ok || val != "concurrent_value" {
			t.Errorf("Node %d: expected 'concurrent_value', got %q (ok=%v)", id, val, ok)
		}
	}
	t.Log("Concurrent test passed under 30% loss")
}
