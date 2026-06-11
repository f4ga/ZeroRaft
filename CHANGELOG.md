# Changelog

## [0.1.0] – 2026-06-11

### Added
- Raw UDP transport via syscall (`syscall.Socket`, `bind`, `recvfrom`, `sendto`) — RT-01, RT-02, RT-03
- JSON and Protobuf codecs with length‑prefix framing (4-byte big-endian + payload) — RT-04, RT-05
- Leader election with random timeouts (150–300ms) and heartbeat (50ms) — RT-06…RT-10
- Log replication and state machine (in‑memory `map[string]string`) — RT-11…RT-15
- Persistence for `currentTerm` and `votedFor` (atomic write via temp file + rename) — RT-16…RT-18
- Interactive CLI with `/status`, `/set`, `/get`, `/leader`, `/chaos` commands — RT-19, RT-20
- Packet loss simulation (`/chaos loss=0.3`) — RT-29
- PCAP capture (`--pcap` flag) in DLT_RAW format — BS-04
- pprof HTTP server on `:6060` (CPU, memory, mutex, goroutine profiles) — RT-28
- Docker image (multi‑stage build) and `docker-compose.yml` with 3-node cluster and real healthcheck — RT-26
- `--health` flag and `IsHealthy()` method for container orchestration — RT-30
- `--cli` client mode for one‑shot command execution
- Integration tests with real UDP sockets (`TestRealUDPTransport`)
- GitHub Actions CI: lint, test with race detector, coverage, Docker build, Docker smoke test — RT-25
- Codec benchmarks (JSON vs Protobuf) with real hardware results
- Socket buffer tuning: `SO_RCVBUF` and `SO_SNDBUF` set to 4 MB — RT-27

### Fixed
- Data races in `TestConcurrentSendReceive`
- Port 0 handling in `NewRawSocket`
- Docker container restart loop (non‑interactive mode, `--cli` mode, increased healthcheck `start_period`)
- Smoke test flakiness (replaced `sleep` with retry loop)
- `golangci-lint` warnings (`errcheck`, `staticcheck`, `gocritic`)
- Hostname resolution in `ResolveAddr` for Docker container names

### Changed
- Moved codec from `transport` to separate `codec` package
- Exported `transport.ResolveAddr` for hostname resolution
- Added `RaftNode.FindPeerIDByAddr`
- Removed requirement for node to list itself in `--peers`
- Updated README: removed mock/simulation warnings, added Docker cluster section
- Updated benchmark results with fresh measurements

### Known Issues
- Log persistence is in‑memory only (BS-01 planned for 0.2.0)
- Snapshots not implemented (BS-02 planned for 0.2.0)
- Membership changes not implemented (BS-03 planned for 0.3.0)
- IPv6 not supported (IPv4 only)
- Network benchmark (net comparison) is a placeholder