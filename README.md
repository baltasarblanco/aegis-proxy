# 🛡️ AEGIS: Ultra-High-Performance L4 Proxy

AEGIS is a bare-metal, zero-allocation TCP (L4) Proxy and front-end router written in pure Rust. Engineered to act as an indestructible shield for high-throughput backend databases (specifically, a custom LSM-Tree engine named Chronos), it operates completely in the Linux Kernel space bypassing standard library bottlenecks.

## 🧠 Architecture: The Silicon Sympathy Approach
AEGIS discards traditional global thread-pools and standard `epoll` event loops. It operates entirely on a **Thread-per-Core (Shared-Nothing)** paradigm to achieve absolute zero lock-contention and linear scalability.

* **Core Pinning (`core_affinity`):** Spawns exactly N OS threads (matching physical/logical cores) and magnetically pins them to the CPU silicon. Cache L1/L2 is strictly preserved.
* **Hardware Load Balancing (`SO_REUSEPORT`):** Raw sockets are forged in C (`socket2`) before entering Rust. We rely on the NIC and the Linux Kernel's eBPF/Hash algorithms to distribute incoming SYN packets directly to the isolated cores.
* **Quantum I/O (`io_uring`):** Uses `tokio-uring` to completely bypass `epoll` syscall overhead, handling network operations proactively via shared-memory Submission/Completion Queues with the Kernel.
* **Zero-Allocation Thermal Pools:** Pre-allocated Thread-Local Memory Pools (`VecDeque` with pre-reserved capacities). Memory is recycled, never dropped. **Zero `malloc` calls under DDoS loads.**
* **Quantum Padding:** Telemetry atomics are wrapped in `#[repr(align(64))]` to perfectly align with CPU cache lines, completely eradicating False Sharing across cores.

## 🚀 The 5 Phases of AEGIS
- [x] **Phase 1: Silicon Topology.** Core detection, thread pinning, raw socket forging, and isolated `io_uring` runtimes.
- [x] **Phase 2: The Thermal Loop.** Zero-allocation runtime. Pre-allocated Thread-Local Memory Pools (Slab Allocators).
- [x] **Phase 3: Particle Accelerator.** L4 Routing and Zero-Copy data passing directly through Kernel shared rings.
- [x] **Phase 4: Stark HUD.** Lock-free atomic telemetry (Data Plane) and Graceful Shutdown coordinated via a Broadcast Control Plane.
- [x] **Phase 5: Symbiosis.** Persistent connection pooling (Thread-Local Hot Pipes) with the Chronos LSM-Tree backend.

## 📊 Benchmarks & Telemetry
Tested on local consumer hardware (AMD Ryzen) routing traffic to a local LSM-Tree Database (Chronos). Implementing Thread-Local Connection Pooling **doubled throughput** and completely eradicated TCP Handshake overhead, achieving **sub-millisecond P99 latencies** across the entire stack.

**Attack Vector (500k Sustained Keep-Alive):** `ab -k -n 500000 -c 200 http://127.0.0.1:8081/`

| Metric | Result | Note |
| :--- | :--- | :--- |
| **Complete Requests** | `500,000` | 100% Success Rate |
| **Failed Requests** | `0` | Zero dropped connections |
| **Requests per Second** | `~8,400+ [#/sec]` | Full Proxy + DB Round-Trips |
| **P99 Latency** | `< 1 ms` | 99% of requests routed in less than 1 millisecond |
| **Max Latency** | `4 ms` | Absolute worst-case scenario |

## 🛰️ Observability (Grafana & Prometheus)
AEGIS features a lock-free, zero-cost HTTP metrics satellite running on the Control Plane (`port 8082`). You can visualize the RPS and Bandwidth in real-time.

1. Boot the proxy and the backend:
```bash
cargo run --release
```
2. Launch the telemetry stack:

```bash
docker compose up -d
```

3. Open http://localhost:3000 to view the live dashboard during load testing.
