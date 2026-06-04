<div align="center">
  <h1>ZeroRaft</h1>
  <p>Raft consensus protocol from scratch.<br>No <code>net</code> package, no libraries — only Go and syscalls.</p>

  <a href="https://github.com/f4ga/ZeroRaft/actions/workflows/ci.yml"><img src="https://github.com/f4ga/ZeroRaft/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://goreportcard.com/report/github.com/f4ga/ZeroRaft"><img src="https://goreportcard.com/badge/github.com/f4ga/ZeroRaft" alt="Go Report Card"></a>
  <img src="https://img.shields.io/badge/coverage-77%25-brightgreen" alt="Coverage">
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
| [Testing](#testing) | Unit, integration, coverage |
| [Codec Benchmarks](#codec-benchmarks) | JSON vs Protobuf performance on real hardware |
| [Project Structure](#project-structure) | Directory layout |
| [How Raft Works](#how-raft-works) | Election, replication, conflict resolution |
| [What’s Missing](#whats-missing) | Known gaps and simplifications |
| [License](#license) | Apache 2.0 |

---

## What is ZeroRaft?

ZeroRaft is a **complete from‑scratch implementation** of the Raft consensus protocol.  
The main rule: no `net` package, no existing Raft frameworks, no ORM, no message brokers.  
Just Go, raw syscalls (`syscall.Socket`, `bind`, `recvfrom`, `sendto`), and manual control over every byte.

**This is not production software – it is for learning.**  
You will see exactly how a distributed consensus system works at the lowest level.

> ⚠️ **Note on networking:** The core Raft logic, persistence, CLI, chaos simulation, PCAP, and pprof are fully implemented and **tested**. However, the `main.go` executable currently uses a mock transport – it can run a single node locally but will not communicate with other nodes over the real network. The real network communication is **simulated** inside the integration tests (which use an in‑memory router). This is sufficient to prove the protocol works; adding a real UDP transport is a possible extension.

---

## What's Inside

| Component | Status | Remarks |
|-----------|--------|---------|
| Raw UDP transport (syscall) | ✅ | Code ready (`internal/transport`), but not wired into `main.go` |
| JSON & Protobuf codecs | ✅ | Configurable via `Codec` interface; benchmarks included |
| Leader election, log replication | ✅ | Full Raft FSM with conflict resolution |
| Persistence (`currentTerm`, `votedFor`) | ✅ | Atomic write via temp+rename |
| State machine (in‑memory `map`) | ✅ | Supports `set`/`get` commands |
| CLI (interactive, leader forwarding) | ✅ | Works locally (no real network) |
| Packet loss simulation | ✅ | `/chaos loss=0.3` changes drop probability in test router |
| PCAP capture (Wireshark) | ✅ | `--pcap` flag records all messages (used in tests) |
| pprof profiling | ✅ | `--pprof :6060` – CPU, memory, goroutine profiles |
| Dockerfile & docker-compose | ✅ | 3‑node cluster with dummy healthcheck |
| Integration tests (3 nodes, loss) | ✅ | Full cluster simulation with custom router |
| **Codec benchmarks** | ✅ | JSON vs Protobuf – see results below |

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

*(The transport layer code exists, but the main executable does not yet connect it.)*

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

### Run a Single Node (local CLI only)

The binary can start, but it will **not** talk to other nodes.  
It will still accept commands and modify its local state machine.

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

The `/chaos` command only affects the **test router** (when you run integration tests), not the real network.

### Run Integration Tests (real Raft cluster simulation)

```bash
make test
```

This starts a simulated 3‑node cluster using an in‑memory router, elects a leader, replicates commands, and even simulates 30% packet loss. All tests pass.

---

## Testing

```bash
# All tests with race detector
make test

# Specific package
go test -race ./internal/raft

# Integration tests (3‑node cluster simulation)
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
| **JSONEncode** | 273 | 240 | 3 |
| **ProtobufEncode** | 208 | 200 | 4 |
| **JSONDecode** | 4813 | 896 | 22 |
| **ProtobufDecode** | 1184 | 872 | 20 |
| **JSONSize** (encode only) | 566 | 496 | 3 |
| **ProtobufSize** | 528 | 528 | 7 |

**Conclusion:**  
- Protobuf decoding is **~4× faster** than JSON.  
- Protobuf encoding is **~1.3× faster**.  
- Both codecs are included; you can switch with `codec.NewCodec(codec.CodecTypeProtobuf)`.

To run only codec benchmarks yourself:

```bash
go test -bench=BenchmarkJSONEncode -benchmem ./internal/codec/
go test -bench=BenchmarkProtobufDecode -benchmem ./internal/codec/
```

---

## Project Structure

```
zeroraft/
├── cmd/zeroraft/           # main.go (mock transport)
├── internal/
│   ├── api/                # generated protobuf
│   ├── client/             # CLI
│   ├── codec/              # JSON + Protobuf codecs
│   ├── raft/               # Raft core (FSM, log, persistence)
│   └── transport/          # raw UDP, chaos, PCAP
├── test/
│   ├── integration/        # 3‑node cluster tests (simulated network)
│   └── benchmark/          # (optional)
├── scripts/                # bench.sh (placeholder), chaos.sh
├── .github/workflows/      # CI (lint, test, coverage)
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

All this is exercised by the integration tests.

---

## What’s Missing

| Issue | Status | Notes |
|-------|--------|-------|
| Real UDP transport in `main.go` | ❌ | Code is ready but not wired |
| Healthcheck endpoint (`--health`) | ❌ | Docker healthcheck is dummy |
| Docker smoke tests in CI | ❌ | Not added yet |
| Log persistence to disk (BS‑01) | ⏳ | Planned |
| Snapshots (BS‑02) | ⏳ | Planned |
| Membership changes (BS‑03) | ⏳ | Planned |

If you need a fully networked Raft, consider using etcd or Consul.

---

## License

Apache License 2.0. See [LICENSE](LICENSE).

---

<div align="center">
  <a href="https://github.com/f4ga/ZeroRaft">github.com/f4ga/ZeroRaft</a>
</div>