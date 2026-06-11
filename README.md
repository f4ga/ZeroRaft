<div align="center">
  <h1>ZeroRaft</h1>
  <p>Raft consensus protocol from scratch.<br>No <code>net</code> package, no libraries — only Go and syscalls.</p>

  <a href="https://github.com/f4ga/ZeroRaft/actions/workflows/ci.yml"><img src="https://github.com/f4ga/ZeroRaft/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://goreportcard.com/report/github.com/f4ga/ZeroRaft"><img src="https://goreportcard.com/badge/github.com/f4ga/ZeroRaft" alt="Go Report Card"></a>
  <img src="https://img.shields.io/badge/coverage-77%25-brightgreen" alt="Coverage">
  <a href="CHANGELOG.md"><img src="https://img.shields.io/badge/release-v0.1.0-blue" alt="Release"></a>
  <a href="https://go.dev/"><img src="https://img.shields.io/badge/Go-1.23+-00ADD8?logo=go" alt="Go Version"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue" alt="License"></a>

</div>

---

## 📖 Table of Contents

| Section | Description |
|---------|-------------|
| [What is ZeroRaft?](#what-is-zeroraft) | Concept and goals |
| [What's Inside](#whats-inside) | Implemented features at a glance |
| [Architecture](#architecture) | High‑level component diagram |
| [Quick Start](#quick-start) | Clone, build, run tests, run CLI |
| [Docker Cluster](#docker-cluster) | Run a 3-node cluster with Docker |
| [Testing](#testing) | Unit, integration, coverage |
| [Codec Benchmarks](#codec-benchmarks) | JSON vs Protobuf performance on real hardware |
| [Project Structure](#project-structure) | Directory layout |
| [How Raft Works](#how-raft-works) | Election, replication, conflict resolution |
| [Changelog](#changelog) | Release history |
| [What's Missing](#whats-missing) | Known gaps and simplifications |
| [License](#license) | Apache 2.0 |

---

## What is ZeroRaft?

ZeroRaft is a **complete from‑scratch implementation** of the Raft consensus protocol.  
The main rule: no `net` package, no existing Raft frameworks, no ORM, no message brokers.  
Just Go, raw syscalls (`syscall.Socket`, `bind`, `recvfrom`, `sendto`), and manual control over every byte.

**This is not production software – it is for learning.**  
You will see exactly how a distributed consensus system works at the lowest level.

> ✅ **Real UDP networking:** Nodes communicate over real UDP sockets created with `syscall.Socket`. The `main.go` binary creates a raw UDP socket, binds to the configured address, and sends/receives Raft RPC messages over the network. No mock transport, no simulation — real packets on the wire.

---

## What's Inside

| Component | Status | Remarks |
|-----------|--------|---------|
| Raw UDP transport (syscall) | ✅ | Wired into `main.go` — real network communication |
| JSON & Protobuf codecs | ✅ | Configurable via `Codec` interface; benchmarks included |
| Leader election, log replication | ✅ | Full Raft FSM with conflict resolution |
| Persistence (`currentTerm`, `votedFor`) | ✅ | Atomic write via temp+rename |
| State machine (in‑memory `map`) | ✅ | Supports `set`/`get` commands |
| CLI (interactive, leader forwarding) | ✅ | Works over real UDP — commands forwarded to leader |
| Packet loss simulation | ✅ | `/chaos loss=0.3` drops packets in the transport layer |
| PCAP capture (Wireshark) | ✅ | `--pcap` flag records all network packets |
| pprof profiling | ✅ | `--pprof :6060` – CPU, memory, goroutine profiles |
| Dockerfile & docker-compose | ✅ | 3‑node cluster with real healthcheck |
| Integration tests (3 nodes, loss) | ✅ | Full cluster tests with real UDP or in-memory router |
| **Codec benchmarks** | ✅ | JSON vs Protobuf – see results below |
| **CI smoke tests** | ✅ | Docker compose cluster tested in CI |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│   CLI (readline)    │    State Machine    │    Persistence  │
├─────────────────────────────────────────────────────────────┤
│                     RAFT CORE MODULE                         │
│  • Follower / Candidate / Leader                             │
│  • Election timers (150‑300ms random)                        │
│  • Log: []LogEntry {Index, Term, Command}                    │
│  • commitIndex, lastApplied, nextIndex[], matchIndex[]       │
├─────────────────────────────────────────────────────────────┤
│                    TRANSPORT LAYER                            │
│  • Raw UDP: syscall.Socket, recvfrom, sendto                 │
│  • Codec: [4 bytes length (BE)] + JSON / Protobuf            │
│  • Chaos: packet loss (0‑100%)                               │
├─────────────────────────────────────────────────────────────┤
│                    SYSTEM INTERFACE                           │
│  • pprof on :6060 (CPU, memory, mutex, goroutine)            │
│  • Signals: SIGINT, SIGTERM graceful shutdown                │
└─────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Prerequisites
- Go 1.23+
- (Optional) Docker

### Clone & Build

```bash
git clone https://github.com/f4ga/ZeroRaft.git
cd ZeroRaft
make build
```

### Run a Single Node

```bash
./bin/zeroraft --id=1 --addr=127.0.0.1:8001 --peers=1=127.0.0.1:8001
```

Inside the CLI you can type:
```
> /set foo bar
> /get foo
> /status
> /chaos loss=0.3
```

### Run a 3-Node Cluster (local)

Open three terminals:

**Terminal 1:**
```bash
./bin/zeroraft --id=1 --addr=127.0.0.1:8001 --peers=2=127.0.0.1:8002,3=127.0.0.1:8003 --data-dir=/tmp/zeroraft1
```

**Terminal 2:**
```bash
./bin/zeroraft --id=2 --addr=127.0.0.1:8002 --peers=1=127.0.0.1:8001,3=127.0.0.1:8003 --data-dir=/tmp/zeroraft2
```

**Terminal 3:**
```bash
./bin/zeroraft --id=3 --addr=127.0.0.1:8003 --peers=1=127.0.0.1:8001,2=127.0.0.1:8002 --data-dir=/tmp/zeroraft3
```

After a few seconds, one node will become the leader. Use `/status` to check.

---

## Docker Cluster

The easiest way to see ZeroRaft in action is with Docker Compose:

```bash
# Build and start a 3-node cluster
make docker-up

# Check container health
docker ps

# Send a command to node1
echo -e '/set smoke hello\n/exit' | docker compose exec -T node1 /usr/local/bin/zeroraft --id=1 --addr=node1:9000 --peers=node2:9000,node3:9000 --data-dir=/data

# Verify replication on node2
echo -e '/get smoke\n/exit' | docker compose exec -T node2 /usr/local/bin/zeroraft --id=2 --addr=node2:9000 --peers=node1:9000,node3:9000 --data-dir=/data

# Stop the cluster
make docker-down
```

### Healthcheck

Each container has a real healthcheck that verifies the Raft node is not in `Candidate` state:

```bash
docker compose ps
# All three should show "(healthy)" after a few seconds
```

### Simulating Packet Loss

```bash
# Attach to the leader container and set 30% packet loss
docker compose exec node1 /usr/local/bin/zeroraft --id=1 --addr=node1:9000 --peers=node2:9000,node3:9000 --data-dir=/data
> /chaos loss=0.3
```

---

## Testing

```bash
# All tests with race detector
make test

# Specific package
go test -race ./internal/raft

# Integration tests (3‑node cluster)
go test -race ./test/integration

# Coverage (excluding generated protobuf)
go test -coverprofile=coverage.out ./internal/...
cat coverage.out | grep -v "internal/api/raft.pb.go" > coverage.filtered.out
go tool cover -func=coverage.filtered.out | grep total
```

Current total: **77%** ✅

---

## Codec Benchmarks

Hardware: **12th Gen Intel(R) Core(TM) i3-1215U (8 cores)**
Command: `go test -bench=. -benchmem ./internal/codec/`

| Benchmark | Time (ns/op) | Bytes/op | Allocs/op |
|-----------|--------------|----------|------------|
| **JSONEncode** | 255 | 240 | 3 |
| **ProtobufEncode** | 199 | 200 | 4 |
| **JSONDecode** | 4402 | 896 | 22 |
| **ProtobufDecode** | 1097 | 872 | 20 |
| **JSONSize** (encode only) | 570 | 496 | 3 |
| **ProtobufSize** | 457 | 528 | 7 |

**Conclusion:**
- Protobuf decoding is **~4× faster** than JSON.
- Protobuf encoding is **~1.3× faster**.
- Both codecs are included; you can switch with `codec.NewCodec(codec.CodecTypeProtobuf)`.

To run only codec benchmarks yourself:

```bash
go test -bench=BenchmarkJSONEncode -benchmem ./internal/codec/
go test -bench=BenchmarkProtobufDecode -benchmem ./internal/codec/
```

### Network Throughput (planned for v0.2.0)

```bash
./scripts/bench.sh raw
```

> Note: The net.UDPConn comparison is not yet implemented.

---

## Project Structure

```
zeroraft/
├── cmd/zeroraft/           # main.go (real UDP transport)
├── internal/
│   ├── api/                # generated protobuf
│   ├── client/             # CLI
│   ├── codec/              # JSON + Protobuf codecs
│   ├── raft/               # Raft core (FSM, log, persistence)
│   └── transport/          # raw UDP, chaos, PCAP
├── test/
│   ├── integration/        # 3‑node cluster tests (real UDP + simulated)
│   └── benchmark/          # (optional)
├── scripts/                # bench.sh, chaos.sh
├── .github/workflows/      # CI (lint, test, coverage, docker smoke)
├── Dockerfile
├── docker-compose.yml
├── Makefile
└── README.md
```

---

## How Raft Works

- **Leader election** – random timeouts (150‑300ms), `RequestVote` RPC, majority wins.
- **Log replication** – `AppendEntries` RPC, conflict resolution via `nextIndex` backoff.
- **Safety** – leader only commits entries from its own term.
- **Persistence** – `currentTerm` and `votedFor` are saved to disk atomically.

All this is exercised by the integration tests and works over real UDP sockets.

---

## What's Missing

| Issue | Status | Notes |
|-------|--------|-------|
| Log persistence to disk (BS‑01) | ⏳ | Planned |
| Snapshots (BS‑02) | ⏳ | Planned |
| Membership changes (BS‑03) | ⏳ | Planned |
| TLS for client communication | ⏳ | Planned |
| TUI dashboard (BS‑06) | ⏳ | Planned |

---

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for the full release history.

---

## License

Apache License 2.0. See [LICENSE](LICENSE).

---

<div align="center">
  <a href="https://github.com/f4ga/ZeroRaft">github.com/f4ga/ZeroRaft</a>
</div>