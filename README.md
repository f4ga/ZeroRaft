# ZeroRaft

<div align="center">
  
**Build Raft from scratch. No shortcuts. No magic.**

[![CI](https://github.com/f4ga/ZeroRaft/actions/workflows/ci.yml/badge.svg)](https://github.com/f4ga/ZeroRaft/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/f4ga/ZeroRaft)](https://goreportcard.com/report/github.com/f4ga/ZeroRaft)
[![Coverage](https://img.shields.io/badge/coverage-84%25-brightgreen)](https://github.com/f4ga/ZeroRaft)
[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

</div>

---

## The Hard Way

Most engineers use Raft every day — via etcd, Consul, or TiKV. Few understand how it actually works.

**ZeroRaft changes that.**

This is a complete, from‑scratch implementation of the Raft consensus protocol with one strict rule:

> **No `net` package. No Raft libraries. No ORM. No message brokers.**
> 
> Just Go, `syscall`, and manual control over every byte.

You'll see exactly how:
- UDP datagrams become Raft RPCs
- Election timers prevent split votes
- Log conflicts resolve through `nextIndex` backoff
- A cluster survives 30% packet loss

**This is not production software. This is deep knowledge.**

---

## What You'll Learn

By studying (or running) ZeroRaft, you'll understand:

| Concept | How ZeroRaft reveals it |
|---------|------------------------|
| **Raw sockets** | `syscall.Socket`, `bind`, `recvfrom`, `sendto` — no `net.Conn` abstractions |
| **RPC encoding** | 4‑byte length prefix + JSON — you see every wire byte |
| **Leader election** | Random timeouts (150–300ms), `RequestVote` with log comparison |
| **Log replication** | `AppendEntries`, `nextIndex`/`matchIndex`, conflict resolution |
| **Consensus safety** | A leader never commits entries from previous terms |
| **Persistence** | Atomic file writes via temp+rename, crash recovery |
| **Network chaos** | Packet loss injection, PCAP capture (Wireshark) |
| **Profiling** | Built‑in `pprof`, flame graphs, syscall tracing with `strace` |

---

## Architecture at a Glance

```
┌─────────────────────────────────────────────────────────────┐
│                     YOUR APPLICATION                         │
├─────────────────────────────────────────────────────────────┤
│   CLI (readline)    │    State Machine    │    Persistence  │
│   /set, /get,       │    map[string]string│    currentTerm  │
│   /status, /chaos   │    apply()          │    votedFor     │
├─────────────────────────────────────────────────────────────┤
│                     RAFT CORE MODULE                         │
│  • Follower / Candidate / Leader states                      │
│  • Election timers (random 150‑300ms)                        │
│  • Log: []LogEntry {Index, Term, Command}                    │
│  • commitIndex, lastApplied, nextIndex[], matchIndex[]       │
├─────────────────────────────────────────────────────────────┤
│                    TRANSPORT LAYER                            │
│  • Raw UDP: syscall.Socket, recvfrom, sendto                 │
│  • Codec: [4 bytes length (BE)] + JSON                       │
│  • Chaos: packet loss simulation (0..100%)                   │
│  • PCAP: Wireshark‑compatible capture                        │
├─────────────────────────────────────────────────────────────┤
│                    SYSTEM INTERFACE                           │
│  • pprof: CPU, memory, mutex, goroutine profiling            │
│  • Healthcheck: /zeroraft --health                           │
│  • Signals: SIGINT, SIGTERM graceful shutdown                │
└─────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Prerequisites
- Go 1.23 or higher
- Docker (optional, for cluster tests)

### 1. Clone & Build

```bash
git clone https://github.com/f4ga/ZeroRaft.git
cd ZeroRaft
make build
```

### 2. Run a Single Node

```bash
./bin/zeroraft \
  --id=1 \
  --addr=127.0.0.1:8001 \
  --peers=127.0.0.1:8002,127.0.0.1:8003 \
  --data-dir=/tmp/zeroraft/node1
```

### 3. Start a 3‑Node Cluster (Docker)

```bash
make docker-up

# Attach to node1's CLI
docker exec -it zeroraft-node1-1 /zeroraft --cli

# Or view logs
docker logs -f zeroraft-node1-1
```

### 4. Try CLI Commands

```
> /status
Node: 1, State: Leader, Term: 3, CommitIndex: 5, Leader: 1

> /set message "hello raft"
OK, committed at index 6

> /get message
hello raft

> /leader
Leader: 1 (address: 127.0.0.1:8001)

> /chaos loss=0.3
Packet loss probability set to 30%

> /status
[still works, just slower...]
```

---

## Command Reference

| Command | Example | Description |
|---------|---------|-------------|
| `/status` | `/status` | Show role, term, commit index, leader address |
| `/set` | `/set foo bar` | Write key-value to the cluster (auto‑forwarded to leader) |
| `/get` | `/get foo` | Read from local state machine |
| `/leader` | `/leader` | Display current leader ID and address |
| `/chaos loss` | `/chaos loss=0.3` | Set packet loss probability (0.0 to 1.0) |
| `/exit` | `/exit` | Leave interactive CLI |

---

## Testing

ZeroRaft follows strict TDD with race detection.

```bash
# Run all tests with race detector
make test

# Run with coverage
go test -cover ./...

# Run specific package
go test -race ./internal/raft

# Integration tests (3‑node cluster)
go test -race ./test/integration
```

**Coverage requirement:** >80% (currently 84%)

---

## Benchmarking (Raw vs net.UDPConn)

ZeroRaft includes a benchmark script that compares performance against a version using Go's standard `net` package.

```bash
./scripts/bench.sh raw    # test raw syscall implementation
./scripts/bench.sh net    # test net.UDPConn baseline
```

**Expected results** (preliminary):

| Metric | net.UDPConn | Raw syscall | Improvement |
|--------|-------------|-------------|-------------|
| Throughput (cmds/sec) | ~1,100 | ~1,250 | +13.6% |
| p99 latency (ms) | 95 | 85 | -10.5% |
| Syscalls per RPC | 8 | 2 | -75% |
| CPU time | 2.8s | 2.3s | -17.8% |

> Raw UDP is faster because it bypasses Go's netpoll and reduces allocations.

---

## Profiling

ZeroRaft embeds `pprof` on port `6060`:

```bash
# CPU profile (30 seconds)
curl http://localhost:6060/debug/pprof/profile?seconds=30 > cpu.prof
go tool pprof -http=:8080 cpu.prof

# Memory profile
curl http://localhost:6060/debug/pprof/heap > mem.prof
go tool pprof -http=:8080 mem.prof

# Trace (execution events)
curl http://localhost:6060/debug/pprof/trace?seconds=5 > trace.out
go tool trace trace.out
```

**Flame graph example:**
```
─────────────────────────────────────────────────
│  syscall.Sendto (35%)                           │
│    ├── raft.appendEntries (28%)                 │
│    └── codec.Encode (7%)                        │
│  raft.handleRequestVote (22%)                   │
│  runtime.selectgo (18%)                         │
│  syscall.Recvfrom (15%)                         │
│  encoding/json.Marshal (10%)                    │
─────────────────────────────────────────────────
```

---

## Packet Capture (PCAP)

Run with `--pcap` to capture all UDP traffic:

```bash
./zeroraft --id=1 --addr=127.0.0.1:8001 --pcap=cluster.pcap
```

Open `cluster.pcap` in Wireshark to see:
- Exact RPC payloads (length prefix + JSON)
- Retransmissions during packet loss
- Heartbeat intervals

---

## Project Structure

```
zeroraft/
├── cmd/zeroraft/
│   └── main.go                 # entry point, flags, signals, pprof
├── internal/
│   ├── transport/
│   │   ├── raw_udp.go          # syscall.Socket, recvfrom, sendto
│   │   ├── codec.go            # length+JSON encode/decode
│   │   ├── chaos.go            # packet loss simulation
│   │   └── pcap.go             # Wireshark capture writer
│   ├── raft/
│   │   ├── raft.go             # Raft FSM (states, elections, RPCs)
│   │   ├── log.go              # LogEntry, RaftLog (append/truncate)
│   │   ├── state_machine.go    # map[string]string, apply()
│   │   └── persistence.go      # atomic save/load for term+votedFor
│   └── client/
│       └── cli.go              # readline, commands, leader forwarding
├── test/
│   ├── integration/            # 3‑node cluster tests
│   └── benchmark/              # raw vs net performance
├── scripts/
│   ├── bench.sh                # benchmark runner
│   └── chaos.sh                # external tc netem (alternative)
├── .github/workflows/ci.yml    # GitHub Actions
├── Dockerfile                  # multi‑stage build
├── docker-compose.yml          # 3 nodes with volumes
├── Makefile                    # build, test, lint, docker-up
└── techdebt.md                 # technical debt tracker (transparency)
```

---

## How Raft Works — A ZeroRaft Perspective

### Leader Election

```
[Follower] --(no heartbeat for 150‑300ms)--> [Candidate]
[Candidate] --(wins majority)-------------> [Leader]
[Leader]    --(sends heartbeat every 50ms)-> [Follower] (resets timers)
```

**Key detail:** Election timeout is **random** per node to prevent split votes.

### Log Replication

1. Client sends `/set foo bar` to leader
2. Leader appends to its log (uncommitted)
3. Leader sends `AppendEntries` to all followers
4. Followers append to their logs, reply `success=true`
5. Leader receives majority → commits entry
6. Leader applies to state machine, replies to client
7. Next heartbeat includes `leaderCommit` → followers apply

### Conflict Resolution

When a follower's log diverges:

```
Leader:  [1,2,3,4,5]
Follower:[1,2,3,6,7]

Leader sends AppendEntries(prevLogIndex=3, prevLogTerm=2, entries=[4,5])
Follower: prevLogIndex=3 matches term=2? YES
          entry at index=4 exists? YES, term=6 ≠ 4 → conflict!
          → truncate from index=4, append [4,5]
Result:  [1,2,3,4,5] ✅
```

---

## Why No `net` Package?

Most Go network code relies on:

```go
conn, _ := net.ListenUDP("udp", addr)  // hides syscall details
```

Under the hood, Go's `net` uses `syscall` too — but adds `netFD`, `poll.FD`, and a selector loop.

**ZeroRaft removes those abstractions:**

- You see **every** `recvfrom` and `sendto`
- You trace with `strace -p PID` and watch raw syscalls
- You control buffers, timeouts, and error handling manually

This is **not** more efficient by default — but it's fully transparent.

---

## Docker Deployment

```bash
# Build and start cluster
make docker-up

# Check health status
docker ps
# CONTAINER ID   STATUS
# abc123         Up 2 minutes (healthy)
# def456         Up 2 minutes (healthy)
# ghi789         Up 2 minutes (healthy)

# Kill leader and watch re-election
docker stop $(docker ps -q --filter "name=node1")

# New leader appears in ~300ms
docker logs zeroraft-node2-1 | grep "became leader"
```

**Healthcheck definition:**
```dockerfile
HEALTHCHECK --interval=5s --timeout=2s --retries=3 \
  CMD /zeroraft --health
```

The `--health` flag exits 0 if the node is in `Leader` or `Follower` state (not stuck in election loop).

---

## Technical Debt — Full Transparency

ZeroRaft tracks all known simplifications and future improvements in [`techdebt.md`](techdebt.md).

**Examples of current debt:**

| Issue | Phase to fix | Description |
|-------|--------------|-------------|
| No log persistence | BS-01 (bonus) | Log stored in memory only → lose entries on crash |
| No snapshots | BS-02 (bonus) | Log grows indefinitely |
| No membership changes | BS-03 (bonus) | Can't add/remove nodes after cluster starts |
| JSON encoding | Optimization | Binary (protobuf) would be faster |
| No pipeline replication | Optimization | One RPC per heartbeat, not batched |

**Why document debt?**  
Because great engineers know what they're sacrificing for speed of delivery.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for:
- Code style (gofmt, golangci-lint)
- Testing with `-race` (amd64 only)
- PR process
- Adding tests for new features

**First time contributing to a distributed system?**  
Start with `internal/transport/raw_udp.go` — it's isolated and well-tested.

---

## Comparison with etcd

| Aspect | ZeroRaft | etcd |
|--------|----------|------|
| **Purpose** | Learning & transparency | Production consensus |
| **Network stack** | Raw `syscall` | `net` + gRPC |
| **Code size** | ~5,000 lines (self-contained) | ~50,000 lines (core only) |
| **Visibility** | 🔥 100% — every byte traced | Black box |
| **Performance** | Good (for learning) | Excellent (optimized) |
| **Production-ready** | ❌ | ✅ (CNCF graduated) |
| **Educational value** | 🚀 Maximum | Low |

ZeroRaft will never replace etcd — but it will teach you what etcd's documentation cannot.

---

## Roadmap (Post-MVP)

| Feature | Priority | Description |
|---------|----------|-------------|
| **Log persistence** | High | Save `raft-log.json`, recover after crash |
| **Snapshots** | Medium | Truncate log when >1000 entries |
| **Membership changes** | Medium | Joint consensus for adding/removing nodes |
| **TLS** | Low | Encrypt UDP traffic (DTLS) |
| **gRPC gateway** | Low | HTTP API for state machine |
| **Kubernetes operator** | Stretch | Auto‑healing clusters on K8s |

---

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.

---

## Acknowledgments

- **Raft paper:** [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) — Diego Ongaro, John Ousterhout
- **MIT 6.824:** [Distributed Systems course](https://pdos.csail.mit.edu/6.824/) — inspiration for testing approach
- **etcd:** Production Raft implementation — for validating our design

---

<div align="center">
  <sub>
    Built with ❤️ and 
    <code>syscall</code> · No shortcuts · No magic
  </sub>
  <br/><br/>
  <a href="https://github.com/f4ga/ZeroRaft">github.com/f4ga/ZeroRaft</a>
</div>
