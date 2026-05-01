# 🛡️ AEGIS — L4 TCP Proxy

![Rust](https://img.shields.io/badge/rust-stable-orange?style=flat-square)
![io_uring](https://img.shields.io/badge/IO-io__uring-blue?style=flat-square)
![License](https://img.shields.io/badge/license-MIT-lightgrey?style=flat-square)
![RPS](https://img.shields.io/badge/RPS-8.4k-brightgreen)
![P99](https://img.shields.io/badge/P99-%3C1ms-brightgreen)

A Layer 4 TCP proxy written in Rust. Built for low‑latency routing to backend databases (like [Chronos](https://github.com/baltasarblanco/chronos_lsm)), it uses thread‑per‑core, `io_uring`, and zero‑allocation hot paths to keep tail latency under 1 ms.

---

## ⚙️ Design

- **Thread‑per‑core architecture**: one OS thread per physical core, pinned via `core_affinity`. No shared data, no locks.
- **`SO_REUSEPORT` + socket sharding**: the kernel distributes incoming connections across threads directly.
- **`io_uring` I/O**: submission and completion queues replace `epoll` to batch syscalls and reduce overhead.
- **Zero‑allocation hot path**: pre‑allocated thread‑local buffers (`VecDeque`) recycle connection memory; no `malloc` during traffic forwarding.
- **Persistent backend connections**: a per‑thread pool of pre‑connected sockets to the backend eliminates repeated TCP handshakes.
- **Lock‑free telemetry**: atomic counters aligned to 64‑byte cache lines (no false sharing), exported via a separate HTTP endpoint.

---

## 📊 Benchmarks

*Local consumer hardware (AMD Ryzen), routing to a local LSM‑Tree backend. 500 k requests, 200 concurrent connections, `ab -k -n 500000 -c 200 http://127.0.0.1:8081/`.*

| Metric               | Result          |
|----------------------|-----------------|
| Requests completed   | 500 000         |
| Failed requests      | 0               |
| Requests per second  | **8 400+**      |
| P99 latency          | **< 1 ms**      |
| Max latency          | 4 ms            |

---

## 🧱 Architecture

1. **Control plane** – spawns worker threads, binds shared‑nothing sockets, exposes `/metrics` on `:8082`.
2. **Worker threads** – each runs an independent `io_uring` event loop, accepts connections from its own socket, and forwards traffic using a local backend connection pool.
3. **Telemetry** – per‑thread atomic metrics (connections, bytes, errors) aggregated and served via a lightweight HTTP handler.

---

## 🚀 Quick Start

### 1. Clone
```bash
git clone https://github.com/baltasarblanco/aegis-proxy.git
cd aegis-proxy
```

### 2. Run (needs a backend on localhost, e.g. Chronos)
```bash
cargo run --release
# AEGIS listening on 127.0.0.1:8081, telemetry on :8082
```

### Load test
```bash
ab -k -n 100000 -c 100 http://127.0.0.1:8081/
```

### 4. Observe metrics
```bash
curl http://localhost:8082/metrics
```
*(Optional) Start Prometheus + Grafana:*
```bash
docker compose up -d
# Grafana at http://localhost:3000
```

---

## 📄 License
MIT

---

*Built by Baltasar Blanco — systems engineer, Rustacean.*