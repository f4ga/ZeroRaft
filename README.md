# ZeroRaft

<div align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=12,18,24,27,30&height=200&section=header&text=ZeroRaft&fontSize=70&fontAlignY=40" />
  <br />
  <img src="https://capsule-render.vercel.app/api?type=rect&color=gradient&customColorList=1&height=60&text=🔥%20Raft%20on%20raw%20sockets%20–%20no%20net%2C%20no%20magic%2C%20only%20syscall%20and%20json&fontSize=20&fontAlignY=50" />
  <br/><br/>

  [![CI](https://github.com/f4ga/ZeroRaft/actions/workflows/ci.yml/badge.svg)](https://github.com/f4ga/ZeroRaft/actions/workflows/ci.yml)
  [![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?logo=go)](https://go.dev/)
  [![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

  <div align="center">
    <a href="README.md"><img src="https://img.shields.io/badge/🇬🇧-English-blue?style=for-the-badge" alt="English"></a>
    &nbsp;&nbsp;
    <a href="README_RU.md"><img src="https://img.shields.io/badge/🇷🇺-Русский-red?style=for-the-badge" alt="Русский"></a>
  </div>
</div>

---

## 📖 What is ZeroRaft?

**ZeroRaft** is a from‑scratch implementation of the Raft consensus protocol.  
The catch: **no ready‑made network libraries**. No `net` package. Just `syscall.Socket`, `bind`, `recvfrom`, `sendto`. No existing Raft frameworks. No ORM, no message brokers.

The goal is not to clone etcd — it's to **feel** how a distributed system works at the lowest level: from a UDP datagram to profiling latencies.

---

## 🧩 Current status

| Component | Status | What’s done |
|-----------|--------|--------------|
| **Project scaffold** | ✅ | `Makefile`, `go.mod`, CI (GitHub Actions), linting, multi‑stage Docker, `docker-compose` for 3 nodes |
| **Raw UDP transport** | ✅ | `syscall.Socket`, `bind`, `recvfrom`, `sendto`. No `net`. Tests with real sockets, race detector enabled |
| **Codec (length+JSON)** | 🔜 | 4‑byte big‑endian prefix + JSON. `RequestVote`, `AppendEntries`, type detection without full parsing |
| **Leader election** | 🔜 | Follower/Candidate/Leader, random timeouts (150–300 ms), `RequestVote` logic, heartbeat via empty `AppendEntries` |
| **Persistence** | 🔜 | `currentTerm` and `votedFor` saved atomically (write‑temp‑file + rename). Restore after restart |
| **Log replication + state machine** | 🔜 | `LogEntry`, `RaftLog` (append/truncate). `AppendEntries` with conflict resolution. In‑memory `map[string]string` |
| **CLI + command forwarding** | 🔜 | Interactive readline. `/status`, `/set`, `/get`, `/leader`, `/chaos`. Follower forwards writes to leader |
| **Chaos, pcap, pprof** | 🔜 | Packet loss simulation (`/chaos loss=0.3`). Raw UDP capture to PCAP (Wireshark). Built‑in `pprof` (CPU, memory) |
| **Docker orchestration + healthcheck** | 🔜 | `docker-compose up` brings 3 nodes with persistent volumes. Healthcheck. Auto‑recovery on container kill |
| **Docs + benchmarks + Habr article** | 🔜 | Full `README`, benchmark scripts, draft for Habr (English/Russian) |

> ✅ = done, 🔜 = in progress (next in line)

---

## ⚡ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         ZeroRaft Node                            │
├─────────────────────────────────────────────────────────────────┤
│  ┌────────────┐    ┌────────────┐    ┌────────────────────┐     │
│  │    CLI     │    │  Raft FSM  │    │    Persistence     │     │
│  │ (readline) │◄──►│ (election, │◄──►│ (term + votedFor)  │     │
│  └────────────┘    │   log, SM) │    └────────────────────┘     │
│                    └─────┬──────┘                                │
│                          │                                       │
│                          ▼                                       │
│         ┌────────────────────────────────┐                     │
│         │         Transport Layer        │                     │
│         │  raw UDP (syscall)             │                     │
│         │  codec (length+JSON)           │                     │
│         │  chaos (loss simulation)       │                     │
│         │  pcap (Wireshark)              │                     │
│         └────────────────────────────────┘                     │
│                          │                                       │
│                          ▼                                       │
│                    ┌─────────┐                                 │
│                    │  pprof  │ (port 6060)                     │
│                    └─────────┘                                 │
└─────────────────────────────────────────────────────────────────┘
```

Every RPC (`RequestVote`, `AppendEntries`) goes through `syscall.Sendto`. Replies come via `syscall.Recvfrom`. No goroutine pools — just manual management.

---

## 🛠️ Quick start

### Requirements
- Go 1.23+
- Docker (optional)

### Build locally

```bash
git clone https://github.com/f4ga/ZeroRaft.git
cd ZeroRaft
make build
```

### Run a single node (debug)

```bash
./bin/zeroraft --id=1 --addr=127.0.0.1:8001 --peers=127.0.0.1:8002,127.0.0.1:8003 --data-dir=./data
```

### 3‑node cluster with Docker

```bash
make docker-up
# attach to any node:
docker exec -it zeroraft_node1_1 /zeroraft --cli
```

### CLI commands

| Command | Description |
|---------|-------------|
| `/status` | node state, term, commitIndex, leader address |
| `/set key value` | write to cluster (auto‑forward to leader) |
| `/get key` | read from local state machine |
| `/leader` | show current leader ID + address |
| `/chaos loss=0.3` | set packet loss probability (0..1) |
| `/exit` | leave CLI |

---

## 📊 Expected benchmarks (after completion)

| Mode | Throughput (cmds/sec) | p99 latency (ms) |
|------|------------------------|------------------|
| `net.UDPConn` | ~800 | 1.2 |
| raw syscall | ~850 | 1.1 |
| raw + 30% loss | ~550 | 3.5 |

> Real numbers will be published after benchmarks.

---

## 🧠 ZeroRaft vs etcd

| Aspect | ZeroRaft | etcd |
|--------|----------|------|
| Internal visibility | 🔥 Full control, everything is open | Black box |
| Network stack | Custom `syscall` | `net` + `grpc` |
| Code size | ~5k lines (self‑contained) | ~50k lines core |
| Educational value | 🚀 Maximum | Low |
| Production‑ready | ❌ (learning project) | ✅ |

ZeroRaft is not a production‑grade etcd replacement. It exists to **teach**.

---

## 📄 License

ZeroRaft is distributed under the **Apache License 2.0**.  
See [LICENSE](LICENSE) for details.

---

<div align="center">
  <i>Solid as stone. Light as ash.</i>
  <br/><br/>
  <a href="https://github.com/f4ga/ZeroRaft">github.com/f4ga/ZeroRaft</a>
  <br/><br/>
  <img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=12,18,24,27,30&height=120&section=footer" />
</div>