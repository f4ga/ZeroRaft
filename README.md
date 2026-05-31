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

## What is ZeroRaft?

ZeroRaft is an implementation of the Raft consensus protocol written from scratch.

The main constraint: no `net` package, no existing Raft frameworks, no ORM, no message brokers. Only Go, syscall, and manual control over network operations.

**This is not production software. It's for learning.**

---

## What You'll Learn

| Concept | How ZeroRaft shows it |
|:--------|:----------------------|
| Raw sockets | `syscall.Socket`, `bind`, `recvfrom`, `sendto` |
| RPC encoding | 4-byte length prefix + JSON |
| Leader election | Random timeouts (150–300ms), `RequestVote` |
| Log replication | `AppendEntries`, `nextIndex`/`matchIndex` |
| Persistence | Atomic file writes via temp+rename |
| Network chaos | Packet loss injection, PCAP capture |
| Profiling | Built-in `pprof`, flame graphs |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│   CLI (readline)    │    State Machine    │    Persistence  │
├─────────────────────────────────────────────────────────────┤
│                     RAFT CORE MODULE                         │
├─────────────────────────────────────────────────────────────┤
│                    TRANSPORT LAYER                            │
│  • Raw UDP: syscall.Socket, recvfrom, sendto                 │
│  • Codec: [4 bytes length (BE)] + JSON                       │
│  • Chaos: packet loss simulation                             │
├─────────────────────────────────────────────────────────────┤
│                    SYSTEM INTERFACE                           │
│  • pprof: CPU, memory profiling                              │
│  • Healthcheck: /zeroraft --health                           │
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

Expected results (preliminary):

| Metric | net.UDPConn | Raw syscall |
|:-------|:------------|:------------|
| Throughput (cmds/sec) | ~1,100 | ~1,250 |
| p99 latency (ms) | 95 | 85 |
| Syscalls per RPC | 8 | 2 |

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

Open `cluster.pcap` in Wireshark.

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
Follower --(no heartbeat)--> Candidate --(wins majority)--> Leader
Leader --(heartbeat every 50ms)--> Follower (resets timers)
```

Election timeout is random (150-300ms) to prevent split votes.

### Log Replication

1. Client sends command to leader
2. Leader appends to log
3. Leader sends AppendEntries to followers
4. Followers append and reply
5. Leader commits after majority
6. Leader applies to state machine
7. Followers apply on next heartbeat

### Conflict Resolution

When logs diverge, leader decreases `nextIndex` until follower's log matches.

---

## Why No `net` Package?

```go
// What you won't see in ZeroRaft:
conn, _ := net.ListenUDP("udp", addr)
```

ZeroRaft uses `syscall` directly:

```go
fd, _ := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_UDP)
syscall.Bind(fd, sockaddr)
syscall.Recvfrom(fd, buf, 0)
syscall.Sendto(fd, data, 0, addr)
```

This makes every syscall visible and traceable.

---

## Docker Deployment

```bash
make docker-up

# Check health
docker ps

# Kill leader, watch re-election
docker stop $(docker ps -q --filter "name=node1")
```

---

## Technical Debt

Known simplifications are tracked in [`techdebt.md`](techdebt.md):

| Issue | Description |
|:------|:------------|
| No log persistence | Log is in-memory only |
| No snapshots | Log grows indefinitely |
| No membership changes | Cannot add/remove nodes |
| JSON encoding | Slower than binary |

---

## Comparison with etcd

| Aspect | ZeroRaft | etcd |
|:-------|:---------|:-----|
| Purpose | Learning | Production |
| Network | Raw syscall | net + gRPC |
| Code size | ~5k lines | ~50k lines |
| Production-ready | No | Yes |

---

## License

Apache License 2.0. See [LICENSE](LICENSE).

---

## Acknowledgments

- Raft paper by Diego Ongaro and John Ousterhout
- MIT 6.824 Distributed Systems course
- etcd for design validation

---

<div align="center">
  <a href="https://github.com/f4ga/ZeroRaft">github.com/f4ga/ZeroRaft</a>
</div>
