# 🛡️ AEGIS: Ultra-High-Performance L4 Proxy

AEGIS is a bare-metal, zero-copy TCP (L4) Proxy and front-end router written in pure Rust. It is engineered to act as an indestructible shield and Connection Pooler for high-throughput backend databases (specifically, a custom LSM-Tree engine named Chronos).

## 🧠 Architecture: The Silicon Sympathy Approach
AEGIS discards traditional global thread-pools and standard `epoll` event loops. It operates entirely on a **Thread-per-Core (Shared-Nothing)** paradigm to achieve absolute zero lock-contention and linear scalability.

* **Core Pinning (`core_affinity`):** Spawns exactly N OS threads (matching physical/logical cores) and magnetically pins them to the CPU silicon. Cache L1/L2 is strictly preserved.
* **Hardware Load Balancing (`SO_REUSEPORT`):** Raw sockets are forged in C (`socket2`) before entering Rust. We rely on the NIC and the Linux Kernel's eBPF/Hash algorithms to distribute incoming SYN packets directly to the isolated cores.
* **Quantum I/O (`io_uring`):** Uses `tokio-uring` to completely bypass `epoll` syscall overhead, handling network operations proactively via shared-memory Submission/Completion Queues with the Kernel.

## 🚀 The 5 Phases of AEGIS
- [x] **Phase 1: Silicon Topology.** Core detection, thread pinning, raw socket forging, and isolated `io_uring` runtimes.
- [ ] **Phase 2: The Thermal Loop.** Zero-allocation runtime. Pre-allocated Thread-Local Memory Pools (Slab Allocators) to eliminate `malloc` under DDoS loads.
- [ ] **Phase 3: Particle Accelerator.** L4 Routing and Zero-Copy data passing directly through Kernel shared rings.
- [ ] **Phase 4: Stark HUD.** Lock-free atomic telemetry (Data Plane) and Graceful Shutdown coordinated via a Broadcast Control Plane.
- [ ] **Phase 5: Symbiosis.** Persistent connection pooling with the Chronos LSM-Tree backend.

## 🔬 Telemetry & Proof of Concept (Phase 1)
TTo run the current state of the Hydra and verify hardware load balancing:

1. Compile with aggressive LTO optimizations:
```bash
cargo build --release
```
2. Execute the binary (requires Linux Kernel 6.1+ for optimal io_uring):

```bash
./target/release/aegis_proxy
```
3. In a separate terminal, verify the hardware-level socket distribution:
```bash
ss -lntp | grep 8080
```

You will see independent file descriptors attached to the same port, handled by isolated PIDs/TIDs.