# Contributing to RingLog

Thank you for your interest in contributing to **RingLog**! We welcome community contributions to make this high-performance message broker even better.

As an open-source project, we expect all contributors to adhere to standard development guidelines and write clean, warning-free, and idiomatic Rust.

---

## 🛠️ Development Environment Setup

To get started with developing on RingLog:

1. **Operating System**: Linux is strictly required. `io_uring` features require Linux Kernel **5.1** or newer, though **5.6+** is highly recommended for full asynchronous TCP socket compatibility.
2. **Rust Toolchain**: Install stable Rust via [rustup](https://rustup.rs/):
   ```bash
   rustup update stable
   ```
3. **Cloning the repository**:
   ```bash
   git clone https://github.com/sumant1122/ringlog.git
   cd ringlog
   ```

---

## 🚀 Running & Verification

### Code Compilation
Verify the code compiles without any errors or warnings:
```bash
cargo check
```

### Formatting
We adhere strictly to standard Rust formatting guidelines. Always format your changes before opening a Pull Request:
```bash
cargo fmt --all -- --check
```
*To automatically format the code, run: `cargo fmt`*

### Linting
Run Clippy to catch common mistakes and enforce idiomatic patterns:
```bash
cargo clippy -- -D warnings
```

### Running the Integration Demo
Verify that the concurrent producer-consumer loop runs successfully:
```bash
cargo run
```

---

## 📥 Submitting a Pull Request

1. **Fork the Repository**: Create a personal fork on GitHub.
2. **Create a Feature Branch**: Use a descriptive name (e.g. `feat/sparse-index` or `fix/connection-cleanup`).
3. **Commit Messages**: We recommend utilizing the [Conventional Commits](https://www.conventionalcommits.org/) specification:
   * `feat: add sparse memory indexing`
   * `fix: handle unexpected EOF on TCP streams`
   * `docs: update wire protocol examples`
4. **Push & Open PR**: Push changes to your fork and open a Pull Request against the `master` branch. Ensure the description explains what changes were made and how they were tested.

Thank you for helping build high-performance infrastructure!
