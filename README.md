# 🛡️ AEGIS: Ultra-High-Performance L4 Proxy

AEGIS is a bare-metal, zero-allocation TCP (L4) Proxy and front-end router written in pure Rust. Engineered to act as an indestructible shield for high-throughput backend databases (specifically, a custom LSM-Tree engine named Chronos), it operates completely in the Linux Kernel space bypassing standard library bottlenecks.

## 🧠 Architecture: The Silicon Sympathy Approach
AEGIS discards traditional global thread-pools and standard `epoll` event loops. It operates entirely on a **Thread-per-Core (Shared-Nothing)** paradigm to achieve absolute zero lock-contention and linear scalability.

* **Core Pinning (`core_affinity`):** Spawns exactly N OS threads (matching physical/logical cores) and magnetically pins them to the CPU silicon. Cache L1/L2 is strictly preserved.
* **Hardware Load Balancing (`SO_REUSEPORT`):** Raw sockets are forged in C (`socket2`) before entering Rust. We rely on the NIC and the Linux Kernel's eBPF/Hash algorithms to distribute incoming SYN packets directly to the isolated cores.
* **Quantum I/O (`io_uring`):** Uses `tokio-uring` to completely bypass `epoll` syscall overhead, handling network operations proactively via shared-memory Submission/Completion Queues with the Kernel.
* **Zero-Allocation Thermal Pools:** Pre-allocated Thread-Local Memory Pools (`VecDeque` with pre-reserved capacities). Memory is recycled, never dropped. **Zero `malloc` calls under DDoS loads.**
* **Lock-Free Atomic Telemetry:** Real-time HUD and traffic tracking using `AtomicUsize` with `Ordering::Relaxed`. Zero Mutexes, zero thread blocking.

## 🚀 The 5 Phases of AEGIS
- [x] **Phase 1: Silicon Topology.** Core detection, thread pinning, raw socket forging, and isolated `io_uring` runtimes.
- [x] **Phase 2: The Thermal Loop.** Zero-allocation runtime. Pre-allocated Thread-Local Memory Pools (Slab Allocators).
- [x] **Phase 3: Particle Accelerator.** L4 Routing and Zero-Copy data passing directly through Kernel shared rings.
- [x] **Phase 4: Stark HUD.** Lock-free atomic telemetry (Data Plane) and Graceful Shutdown coordinated via a Broadcast Control Plane.
- [ ] **Phase 5: Symbiosis.** Persistent connection pooling and load balancing with the Chronos LSM-Tree backend.

## 📊 Benchmarks & Telemetry (Phase 4)
Tested on local consumer hardware (AMD Ryzen) routing traffic to a local LSM-Tree Database. The proxy successfully absorbed and routed a **100,000 Request Spike** with **Zero Dropped Packets** and sub-millisecond P99 latencies.

**Attack Vector:** `ab -n 100000 -c 200 http://127.0.0.1:8081/`

| Metric | Result | Note |
| :--- | :--- | :--- |
| **Complete Requests** | `100,000` | 100% Success Rate |
| **Failed Requests** | `0` | Zero dropped connections |
| **Concurrency Level** | `200` | Simultaneous active sockets |
| **Requests per Second** | `~4,460 [#/sec]` | Full L4 Routing Round-Trips |
| **P99 Latency** | `1 ms` | 99% of requests routed in <= 1 millisecond |
| **Max Latency** | `9 ms` | Absolute worst-case scenario |

### Connection Times (ms)
```text
              min  mean[+/-sd] median   max
Connect:        0    0   0.0      0       0
Processing:     0    0   0.2      0       9
Waiting:        0    0   0.2      0       9
Total:          0    0   0.2      0       9
```
## 🛠️ Quick Start

1. Compile with aggressive LTO optimizations:
```bash
cargo build --release
```
2. Execute the binary (requires Linux Kernel 6.1+ for optimal io_uring support):

```bash
./target/release/aegis_proxy
```

3. Monitor the Atomic HUD in the control plane terminal while blasting the proxy with your benchmarking tool of choice.