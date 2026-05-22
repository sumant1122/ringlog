# RingLog: Thread-Per-Core io_uring Message Broker in Rust

A highly-optimized, single-node, mini-Kafka broker built from scratch in Rust. This system leverages a **thread-per-core** architecture powered by `io_uring` via the `tokio-uring` framework, bypassing traditional OS thread context switches and synchronization overhead.

---

## ⚡ Key Architectural Features

1. **`io_uring` Asynchronous System Calls**: Uses direct Linux kernel ring buffers for both Disk WAL I/O (`File::write_at`/`File::read_at`) and network I/O (`TcpStream::read`/`TcpStream::write`).
2. **Kernel-User Ownership Passing**: Implements strict data ownership transfer where memory buffers (`Vec<u8>`) are passed to the kernel and reaped asynchronously, achieving zero-copy properties.
3. **Single-Thread Hot Path**: The server runs entirely inside a single CPU thread event loop. Cross-client synchronization and tail-log notifications are achieved using thread-local asynchronous coordination (`tokio::sync::watch` and `tokio::sync::Mutex`), avoiding standard thread-locking contention.
4. **Append-Only Write-Ahead Log (WAL)**: Messages are sequentially packed with a 4-byte length header and flushed directly to disk.

---

## 📊 Wire Protocol Specifications

The broker communicates over TCP using a simplified binary framing protocol:

### 1. Connection Handshake
On initial connection, a client must send a single byte designating its role:
* **`0x01`**: Producer Client
* **`0x02`**: Consumer Client

### 2. Producer Protocol
Once handshaking as a Producer, the client continuously streams records:
* **Request Format**: `[4-Byte Big-Endian Length] [Payload Bytes]`
* **Response ACK**: The server appends the record to the WAL and immediately returns an `[8-Byte Big-Endian Physical Offset]` representing the exact byte location of the message.

### 3. Consumer Protocol
Once handshaking as a Consumer, the client requests a stream of records starting from a specific point:
* **Request Format**: `[8-Byte Big-Endian Starting Offset]`
* **Response Stream**: The server tails the WAL and streams back matches: `[4-Byte Big-Endian Length] [Payload Bytes]`. When it reaches the end of the log, it yields and awaits notifications of new writes.

---

## 🛠️ Prerequisites

* **OS**: Linux (Kernel **5.1** or newer is required for basic `io_uring` support, **5.6+** is highly recommended for full socket support).
* **Rust**: Modern stable toolchain (`edition = "2021"`).

---

## 🚀 How to Run

The single binary supports four distinct operational modes via command-line arguments.

> [!TIP]
> Since this is a high-performance infrastructure project, always compile with the `--release` flag in production environments to enable maximum compiler optimizations for `io_uring` memory layouts:
> ```bash
> cargo run --release -- [subcommand]
> ```

### 1. Run the Real-Time Simulation Demo (Recommended)
This runs a fully self-contained simulation of the broker, producer, and consumer all running concurrently inside the same thread-local event loop:

```bash
cargo run
```

### 2. Run the Dedicated Broker Server
To start the standalone broker server listening on port `12000`:

```bash
cargo run -- server
```
*Creates/appends to a persistent file `commit.log` in your working directory, automatically preserving/restoring the write offsets.*

### 3. Run the Interactive TCP Producer Client
In a new terminal window, connect to a running broker server and dynamically type messages to publish them to the WAL:

```bash
cargo run -- producer
```
*Simply type your message in the console and hit Enter. The client will print the assigned physical byte offset ACK returned from the server.*

### 4. Run the Real-Time TCP Consumer Client
In a new terminal window, connect to a running broker server to stream all historical and newly landing messages:

```bash
cargo run -- consumer [starting_offset]
```
*If `starting_offset` is omitted, it defaults to `0` (replaying the entire log).*
