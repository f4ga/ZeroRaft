<div align="center">
  <h1>ZeroRaft</h1>
  <p>Raft consensus protocol from scratch. No net package. No libraries.</p>

  <a href="https://github.com/f4ga/ZeroRaft/actions/workflows/ci.yml"><img src="https://github.com/f4ga/ZeroRaft/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://goreportcard.com/report/github.com/f4ga/ZeroRaft"><img src="https://goreportcard.com/badge/github.com/f4ga/ZeroRaft" alt="Go Report Card"></a>
  <img src="https://img.shields.io/badge/coverage-84%25-brightgreen" alt="Coverage">
  <a href="https://go.dev/"><img src="https://img.shields.io/badge/Go-1.23+-00ADD8?logo=go" alt="Go Version"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue" alt="License"></a>

</div>

---

## 📖 Table of Contents

- [What is ZeroRaft?](#what-is-zeroraft)
- [Why From Scratch?](#why-from-scratch)
- [Why No `net` Package?](#why-no-net-package)
- [What's Implemented](#whats-implemented)
- [What's Missing](#whats-missing)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Command Reference](#command-reference)
- [Testing](#testing)
- [Benchmarking](#benchmarking)
- [Profiling](#profiling)
- [Packet Capture (PCAP)](#packet-capture-pcap)
- [Project Structure](#project-structure)
- [How Raft Works](#how-raft-works)
- [Docker Deployment](#docker-deployment)
- [FAQ](#faq)
- [License](#license)

---

## What is ZeroRaft?

ZeroRaft is an implementation of the Raft consensus protocol written from scratch.

**The problem:** Most engineers use etcd, Consul, or TiKV daily, but few understand how Raft actually works.

**The solution:** Build Raft from scratch with one rule — no `net` package, no existing Raft frameworks. Only Go, syscalls, and manual control.

**This is not production software. This is for learning.**

---

## Why From Scratch?

Building from scratch answers questions that using etcd never will:

- Why does a cluster need odd number of nodes?
- What happens when network packets are lost?
- Why can't a leader commit entries from previous terms?
- How does `strace` reveal what's really happening?

**What you gain:**
- Deep understanding of consensus protocols
- Low-level network programming with syscalls
- Profiling and optimization with pprof
- A portfolio project for Senior level

---

## Why No `net` Package?

Go's standard `net` package hides important details:

```go
// What you normally write:
conn, _ := net.ListenUDP("udp", addr)  // hides syscall.Socket, bind
conn.ReadFrom(buf)                      // hides recvfrom
conn.WriteTo(data, addr)                // hides sendto
```

**ZeroRaft removes all abstractions:**

```go
// What ZeroRaft does:
fd, _ := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_UDP)
syscall.Bind(fd, sockaddr)
syscall.Recvfrom(fd, buf, 0)
syscall.Sendto(fd, data, 0, addr)
```

**Why it matters:**

| Aspect | With `net` package | With raw syscalls |
|:-------|:-------------------|:------------------|
| Visibility | Black box | See every syscall with `strace` |
| Control | Limited | Full control over buffers, timeouts |
| Learning | Low | Maximum — see exactly what the OS does |

Run `strace -p PID` on a running ZeroRaft node and you'll see every `recvfrom` and `sendto` with arguments.

---

## What's Implemented

| Component | Status |
|-----------|--------|
| Raw UDP transport (syscall.Socket, bind, recvfrom, sendto) | ✅ |
| Codec (4-byte length prefix + JSON) | ✅ |
| Leader election (Follower/Candidate/Leader, random timeouts 150-300ms) | ✅ |
| Persistence (currentTerm, votedFor saved to disk) | ✅ |
| Log replication (AppendEntries with conflict resolution) | ✅ |
| State machine (in-memory map with set/get) | ✅ |
| CLI (interactive commands, leader forwarding) | ✅ |
| Packet loss simulation (`/chaos loss=0.3`) | ✅ |
| PCAP capture (Wireshark-compatible) | ✅ |
| pprof profiling (CPU, memory, trace) | ✅ |
| Docker deployment (3 nodes with healthcheck) | ✅ |
| Log persistence to disk | ⏳ (in-memory only for now) |
| Snapshots for log compaction | ⏳ |
| Membership changes (add/remove nodes) | ⏳ |
| Prometheus metrics | ⏳ |
| TLS encryption | ⏳ |

⏳ = planned, not yet implemented

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│   CLI (readline)    │    State Machine    │    Persistence  │
├─────────────────────────────────────────────────────────────┤
│                     RAFT CORE MODULE                         │
│  • Follower / Candidate / Leader                             │
│  • Election timers (150-300ms random)                        │
│  • Log: []LogEntry {Index, Term, Command}                    │
│  • commitIndex, lastApplied, nextIndex[], matchIndex[]       │
├─────────────────────────────────────────────────────────────┤
│                    TRANSPORT LAYER                            │
│  • Raw UDP: syscall.Socket, recvfrom, sendto                 │
│  • Codec: [4 bytes length (BE)] + JSON                       │
│  • Chaos: packet loss (0-100%)                               │
├─────────────────────────────────────────────────────────────┤
│                    SYSTEM INTERFACE                           │
│  • pprof on :6060 (CPU, memory, mutex, goroutine)            │
│  • Healthcheck: /zeroraft --health                           │
│  • Signals: SIGINT, SIGTERM graceful shutdown                │
└─────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Prerequisites
- Go 1.23+
- Docker (optional)

### Clone & Build

```bash
git clone https://github.com/f4ga/ZeroRaft.git
cd ZeroRaft
make build
```

### Run a Single Node

```bash
./bin/zeroraft \
  --id=1 \
  --addr=127.0.0.1:8001 \
  --peers=127.0.0.1:8002,127.0.0.1:8003 \
  --data-dir=/tmp/zeroraft/node1
```

### Run 3-Node Cluster (Docker)

```bash
make docker-up

# Attach to node1's CLI
docker exec -it zeroraft-node1-1 /zeroraft --cli
```

### CLI Commands

```
> /status
State: Leader, Term: 3, CommitIndex: 5

> /set message "hello raft"
OK, committed at index 6

> /get message
hello raft

> /leader
Leader: 1 (127.0.0.1:8001)

> /chaos loss=0.3
Packet loss probability set to 30%
```

---

## Command Reference

| Command | Description |
|:--------|:------------|
| `/status` | Show node role, term, commit index |
| `/set key value` | Write to cluster (forwards to leader) |
| `/get key` | Read from local state machine |
| `/leader` | Show current leader |
| `/chaos loss=X` | Set packet loss probability (0.0-1.0) |
| `/exit` | Exit CLI |

---

## Testing

```bash
# Run all tests with race detector
make test

# Run specific package
go test -race ./internal/raft

# Integration tests (3-node cluster)
go test -race ./test/integration
```

Coverage: 84%

---

## Benchmarking

```bash
./scripts/bench.sh raw    # raw syscall implementation
./scripts/bench.sh net    # net.UDPConn baseline
```

Expected results:

| Metric | net.UDPConn | Raw syscall |
|:-------|:------------|:------------|
| Throughput (cmds/sec) | ~1,100 | ~1,250 |
| p99 latency (ms) | 95 | 85 |
| Syscalls per RPC | 8 | 2 |

Raw UDP is faster because it bypasses Go's netpoll.

---

## Profiling

pprof is available on port 6060:

```bash
# CPU profile (30 seconds)
curl http://localhost:6060/debug/pprof/profile?seconds=30 > cpu.prof
go tool pprof -http=:8080 cpu.prof

# Memory profile
curl http://localhost:6060/debug/pprof/heap > mem.prof
go tool pprof -http=:8080 mem.prof
```

---

## Packet Capture (PCAP)

```bash
./zeroraft --id=1 --addr=127.0.0.1:8001 --pcap=cluster.pcap
```

Open `cluster.pcap` in Wireshark to see every RPC packet.

---

## Project Structure

```
zeroraft/
├── cmd/zeroraft/
│   └── main.go
├── internal/
│   ├── transport/      # raw UDP, codec, chaos, pcap
│   ├── raft/           # FSM, log, state machine, persistence
│   └── client/         # CLI
├── test/
│   ├── integration/    # 3-node cluster tests
│   └── benchmark/
├── scripts/
├── .github/workflows/
├── Dockerfile
├── docker-compose.yml
├── Makefile
└── techdebt.md
```

---

## How Raft Works

### Leader Election

```
Follower --(no heartbeat for 150-300ms)--> Candidate
Candidate --(wins majority)-------------> Leader
Leader --(heartbeat every 50ms)----------> Follower (resets timers)
```

Election timeout is random per node to prevent split votes.

### Log Replication

1. Client sends command to leader
2. Leader appends to its log
3. Leader sends AppendEntries to followers
4. Followers append and reply
5. Leader commits after majority
6. Leader applies to state machine
7. Followers apply on next heartbeat

### Conflict Resolution

When logs diverge, leader decreases `nextIndex` until follower's log matches.

---

## Docker Deployment

```bash
make docker-up

# Check health
docker ps
# All containers should show "healthy"

# Kill leader, watch re-election
docker stop $(docker ps -q --filter "name=node1")

# New leader appears in ~300ms
docker logs zeroraft-node2-1 | grep "became leader"
```

---

## FAQ

**Q: Why not just read the Raft paper?**
A: Reading is not understanding. Implementing reveals edge cases the paper glosses over.

**Q: Why UDP instead of TCP?**
A: Raft assumes unreliable networks. UDP forces you to handle loss, reordering, duplication.

**Q: Is this faster than etcd?**
A: No. etcd is highly optimized. ZeroRaft is for learning.

**Q: Can I use this in production?**
A: Please don't. Use etcd or Consul.

**Q: How long did this take?**
A: Several weeks following the technical specification.

---

## License

Apache License 2.0. See [LICENSE](LICENSE).

---

<div align="center">
  <a href="https://github.com/f4ga/ZeroRaft">github.com/f4ga/ZeroRaft</a>
</div>
