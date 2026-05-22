use std::io;
use std::path::Path;
use std::sync::Arc;
use tokio::io::AsyncBufReadExt;
use tokio::sync::{watch, Mutex};
use tokio_uring::buf::IoBuf;
use tokio_uring::fs::{File, OpenOptions};
use tokio_uring::net::{TcpListener, TcpStream};

/// High-performance Commit Log utilizing io_uring for all disk operations.
pub struct UringCommitLog {
    file: File,
    write_offset: u64,
}

impl UringCommitLog {
    /// Opens or creates the Write-Ahead Log at the specified path.
    pub async fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Recover the write offset by checking the file size.
        let write_offset = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await?;

        Ok(Self { file, write_offset })
    }

    /// Appends a message to the WAL using io_uring.
    /// Format on disk: [4-byte big-endian length] [payload]
    pub async fn append(&mut self, message: Vec<u8>) -> io::Result<u64> {
        let msg_len = message.len() as u32;

        // Allocate a single contiguous buffer to minimize SQE submissions
        let mut buf = Vec::with_capacity(4 + message.len());
        buf.extend_from_slice(&msg_len.to_be_bytes());
        buf.extend(message);

        let offset = self.write_offset;
        let (res, buf) = self.file.write_at(buf, offset).await;
        let bytes_written = res?;

        if bytes_written != buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write the entire frame to the WAL",
            ));
        }

        self.write_offset += bytes_written as u64;
        Ok(offset)
    }

    /// Reads a message from the WAL at the specified physical offset using io_uring.
    pub async fn read_at(&self, offset: u64) -> io::Result<Option<(Vec<u8>, u64)>> {
        // 1. Read the 4-byte length prefix
        let len_buf = vec![0u8; 4];
        let (res, len_buf) = self.file.read_at(len_buf, offset).await;
        let bytes_read = res?;

        if bytes_read == 0 {
            // EOF: No message here yet
            return Ok(None);
        }
        if bytes_read < 4 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "corrupted WAL: incomplete length prefix",
            ));
        }

        let msg_len = u32::from_be_bytes([len_buf[0], len_buf[1], len_buf[2], len_buf[3]]) as usize;

        // 2. Read the payload
        let payload_buf = vec![0u8; msg_len];
        let (res, payload_buf) = self.file.read_at(payload_buf, offset + 4).await;
        let bytes_read_payload = res?;

        if bytes_read_payload < msg_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "corrupted WAL: incomplete payload read",
            ));
        }

        let next_offset = offset + 4 + msg_len as u64;
        Ok(Some((payload_buf, next_offset)))
    }
}

/// Helper function to read exactly `len` bytes from a TcpStream using io_uring.
async fn read_exact(stream: &TcpStream, mut buf: Vec<u8>, len: usize) -> io::Result<Vec<u8>> {
    let mut total_read = 0;
    while total_read < len {
        let slice = buf.slice(total_read..len);
        let (res, slice) = stream.read(slice).await;
        let n = res?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed by peer before full read",
            ));
        }
        buf = slice.into_inner();
        total_read += n;
    }
    Ok(buf)
}

/// Helper function to write an entire buffer to a TcpStream using io_uring.
async fn write_all(stream: &TcpStream, mut buf: Vec<u8>) -> io::Result<Vec<u8>> {
    let len = buf.len();
    let mut total_written = 0;
    while total_written < len {
        let slice = buf.slice(total_written..len);
        let (res, slice) = stream.write(slice).await;
        let n = res?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write bytes to socket connection",
            ));
        }
        buf = slice.into_inner();
        total_written += n;
    }
    Ok(buf)
}

/// Dynamic Connection Handler utilizing fully asynchronous io_uring calls.
async fn handle_connection(
    stream: TcpStream,
    log: Arc<Mutex<UringCommitLog>>,
    waker_tx: watch::Sender<u64>,
    waker_rx: watch::Receiver<u64>,
) -> io::Result<()> {
    // 1. Read the 1-byte handshake identifier
    let handshake_buf = vec![0u8; 1];
    let handshake_buf = read_exact(&stream, handshake_buf, 1).await?;
    let client_type = handshake_buf[0];

    match client_type {
        0x01 => {
            // Producer Connection
            println!("[Server] Producer client connected");
            loop {
                // Read 4-byte length prefix
                let len_buf = vec![0u8; 4];
                let len_buf = match read_exact(&stream, len_buf, 4).await {
                    Ok(b) => b,
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        println!("[Server] Producer disconnected cleanly");
                        break;
                    }
                    Err(e) => return Err(e),
                };
                let msg_len =
                    u32::from_be_bytes([len_buf[0], len_buf[1], len_buf[2], len_buf[3]]) as usize;

                // Read msg_len bytes payload
                let payload_buf = vec![0u8; msg_len];
                let payload_buf = read_exact(&stream, payload_buf, msg_len).await?;

                // Append to WAL
                let offset = {
                    let mut log_guard = log.lock().await;
                    log_guard.append(payload_buf).await?
                };

                // Notify consumers by updating the waker_tx with the new write offset
                let _ = waker_tx.send(offset + 4 + msg_len as u64);

                // Send 8-byte big-endian assigned offset back to client as ACK
                let ack_buf = (offset).to_be_bytes().to_vec();
                let _ = write_all(&stream, ack_buf).await?;
            }
        }
        0x02 => {
            // Consumer Connection
            println!("[Server] Consumer client connected");

            // Read 8-byte starting offset
            let offset_buf = vec![0u8; 8];
            let offset_buf = read_exact(&stream, offset_buf, 8).await?;
            let mut consumer_offset = u64::from_be_bytes([
                offset_buf[0],
                offset_buf[1],
                offset_buf[2],
                offset_buf[3],
                offset_buf[4],
                offset_buf[5],
                offset_buf[6],
                offset_buf[7],
            ]);

            println!(
                "[Server] Consumer streaming starting at offset {}",
                consumer_offset
            );

            let mut rx = waker_rx.clone();
            loop {
                // Try reading from the WAL at consumer_offset
                let read_res = {
                    let log_guard = log.lock().await;
                    log_guard.read_at(consumer_offset).await?
                };

                match read_res {
                    Some((payload, next_offset)) => {
                        // We found a message! Write it to the consumer socket.
                        // Wire frame: [4-byte length] [payload]
                        let msg_len = payload.len() as u32;
                        let mut send_buf = Vec::with_capacity(4 + payload.len());
                        send_buf.extend_from_slice(&msg_len.to_be_bytes());
                        send_buf.extend(payload);

                        let _ = write_all(&stream, send_buf).await?;
                        consumer_offset = next_offset;
                    }
                    None => {
                        // End of WAL. We must wait for new messages.
                        let current_offset = *rx.borrow();
                        if consumer_offset < current_offset {
                            // There is new data, skip waiting
                            continue;
                        }

                        // Wait for notification of new appends
                        if rx.changed().await.is_err() {
                            // Broker shutdown
                            break;
                        }
                    }
                }
            }
            println!("[Server] Consumer disconnected cleanly");
        }
        _ => {
            eprintln!("[Server] Unknown client type: {}", client_type);
        }
    }

    Ok(())
}

/// Custom integration demo demonstrating high performance concurrently.
async fn run_demo() -> io::Result<()> {
    let port = 12000;
    let addr = format!("127.0.0.1:{}", port);

    // Create/clear the commit log file for the demo
    let log_path = "demo_commit.log";
    let _ = std::fs::remove_file(log_path); // start fresh

    println!("[Demo] Initializing UringCommitLog at '{}'...", log_path);
    let log = Arc::new(Mutex::new(UringCommitLog::open(log_path).await?));

    // Setup the notify watch channel with the initial file size (0)
    let (tx, rx) = watch::channel(0u64);

    // Start the TCP server in the background
    let server_addr: std::net::SocketAddr = addr.parse().unwrap();
    let listener = TcpListener::bind(server_addr)?;
    println!(
        "[Demo] Broker Server listening on {} using io_uring...",
        addr
    );

    let server_log = Arc::clone(&log);
    let server_tx = tx.clone();
    let server_rx = rx.clone();
    tokio_uring::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _peer_addr)) => {
                    let log_clone = Arc::clone(&server_log);
                    let tx_clone = server_tx.clone();
                    let rx_clone = server_rx.clone();
                    tokio_uring::spawn(async move {
                        if let Err(e) =
                            handle_connection(stream, log_clone, tx_clone, rx_clone).await
                        {
                            eprintln!("[Server Error] Connection handling error: {:?}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("[Server Error] accept error: {:?}", e);
                    break;
                }
            }
        }
    });

    // Give the server a small moment to bind
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Spawn the Consumer Client
    let consumer_addr = server_addr;
    tokio_uring::spawn(async move {
        println!("[Consumer Client] Connecting to broker...");
        let stream = TcpStream::connect(consumer_addr)
            .await
            .expect("Consumer client failed to connect");

        // 1. Handshake as Consumer (0x02)
        let handshake = vec![0x02];
        let _ = write_all(&stream, handshake).await.unwrap();

        // 2. Send 8-byte starting offset (0)
        let start_offset: u64 = 0;
        let _ = write_all(&stream, start_offset.to_be_bytes().to_vec())
            .await
            .unwrap();
        println!("[Consumer Client] Requested stream starting from offset 0");

        // 3. Receive stream of messages
        loop {
            // Read 4-byte length prefix
            let len_buf = vec![0u8; 4];
            let len_buf = match read_exact(&stream, len_buf, 4).await {
                Ok(b) => b,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    println!("[Consumer Client] Stream ended (Server closed connection)");
                    break;
                }
                Err(e) => {
                    eprintln!("[Consumer Client] Error reading length prefix: {:?}", e);
                    break;
                }
            };
            let msg_len =
                u32::from_be_bytes([len_buf[0], len_buf[1], len_buf[2], len_buf[3]]) as usize;

            // Read msg_len bytes payload
            let payload_buf = vec![0u8; msg_len];
            let payload = read_exact(&stream, payload_buf, msg_len).await.unwrap();
            let msg_str = String::from_utf8_lossy(&payload);
            println!(
                "\x1b[32m[Consumer Client] Received Message: '{}'\x1b[0m",
                msg_str
            );
        }
    });

    // Spawn the Producer Client
    let producer_addr = server_addr;
    tokio_uring::spawn(async move {
        println!("[Producer Client] Connecting to broker...");
        let stream = TcpStream::connect(producer_addr)
            .await
            .expect("Producer client failed to connect");

        // 1. Handshake as Producer (0x01)
        let handshake = vec![0x01];
        let _ = write_all(&stream, handshake).await.unwrap();

        // 2. Publish a few messages with a delay
        let messages = vec![
            "Rust is lightning fast",
            "io_uring bypasses thread context switches",
            "Zero-copy record streaming achieved",
            "Antigravity Agent pair programming",
        ];

        for (i, msg) in messages.into_iter().enumerate() {
            tokio::time::sleep(std::time::Duration::from_millis(800)).await;

            let payload = msg.as_bytes().to_vec();
            let msg_len = payload.len() as u32;

            let mut frame = Vec::with_capacity(4 + payload.len());
            frame.extend_from_slice(&msg_len.to_be_bytes());
            frame.extend(payload);

            println!("[Producer Client] Sending message {}: '{}'", i + 1, msg);
            let _ = write_all(&stream, frame).await.unwrap();

            // Read the ACK back (8-byte offset)
            let ack_buf = vec![0u8; 8];
            let ack_buf = read_exact(&stream, ack_buf, 8).await.unwrap();
            let ack_offset = u64::from_be_bytes([
                ack_buf[0], ack_buf[1], ack_buf[2], ack_buf[3], ack_buf[4], ack_buf[5], ack_buf[6],
                ack_buf[7],
            ]);
            println!(
                "[Producer Client] Received ACK: Message written at physical offset {}",
                ack_offset
            );
        }

        println!("[Producer Client] Finished producing. Closing connection.");
    });

    // Let the demo run for a bit, then exit
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    println!("[Demo] Completed successfully! Shutting down...");

    // Cleanup demo WAL
    let _ = std::fs::remove_file(log_path);

    Ok(())
}

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() > 1 && args[1] == "server" {
        // Mode 1: Dedicated Broker Server
        println!("============================================================");
        println!("              RINGLOG BROKER DEDICATED SERVER               ");
        println!("============================================================");
        println!("[Server] Starting single-threaded io_uring broker on 127.0.0.1:12000...\n");

        tokio_uring::start(async {
            let log_path = "commit.log";
            let log = Arc::new(Mutex::new(UringCommitLog::open(log_path).await.unwrap()));
            let (tx, rx) = watch::channel(log.lock().await.write_offset);

            let addr: std::net::SocketAddr = "127.0.0.1:12000".parse().unwrap();
            let listener = TcpListener::bind(addr).unwrap();
            println!("[Server] Listening for TCP connections on {}...", addr);

            loop {
                let (stream, _peer_addr) = listener.accept().await.unwrap();
                let log_clone = Arc::clone(&log);
                let tx_clone = tx.clone();
                let rx_clone = rx.clone();
                tokio_uring::spawn(async move {
                    if let Err(e) = handle_connection(stream, log_clone, tx_clone, rx_clone).await {
                        eprintln!("[Server Error] Connection error: {:?}", e);
                    }
                });
            }
        });
    } else if args.len() > 1 && args[1] == "producer" {
        // Mode 2: Interactive CLI Producer Client
        println!("============================================================");
        println!("                  RINGLOG TCP PRODUCER                      ");
        println!("============================================================");
        println!("[Producer] Connecting to broker at 127.0.0.1:12000...\n");

        tokio_uring::start(async {
            let stream = TcpStream::connect("127.0.0.1:12000".parse().unwrap())
                .await
                .expect("Failed to connect to broker server. Is it running?");

            // Send Producer handshake
            let _ = write_all(&stream, vec![0x01]).await.unwrap();
            println!("[Producer] Connected. Type messages below and press ENTER to publish.\n");

            let mut stdin_lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
            while let Ok(Some(line)) = stdin_lines.next_line().await {
                let line_trimmed = line.trim();
                if line_trimmed.is_empty() {
                    continue;
                }

                let payload = line_trimmed.as_bytes().to_vec();
                let msg_len = payload.len() as u32;

                let mut frame = Vec::with_capacity(4 + payload.len());
                frame.extend_from_slice(&msg_len.to_be_bytes());
                frame.extend(payload);

                // Send record to broker
                let _ = write_all(&stream, frame).await.unwrap();

                // Read 8-byte assigned physical offset ACK
                let ack_buf = vec![0u8; 8];
                let ack_buf = read_exact(&stream, ack_buf, 8).await.unwrap();
                let ack_offset = u64::from_be_bytes([
                    ack_buf[0], ack_buf[1], ack_buf[2], ack_buf[3], ack_buf[4], ack_buf[5],
                    ack_buf[6], ack_buf[7],
                ]);
                println!(
                    "\x1b[34m[ACK] Message written at WAL offset {}\x1b[0m",
                    ack_offset
                );
            }
        });
    } else if args.len() > 1 && args[1] == "consumer" {
        // Mode 3: CLI Consumer Client
        let start_offset: u64 = if args.len() > 2 {
            args[2].parse().unwrap_or(0)
        } else {
            0
        };

        println!("============================================================");
        println!("                  RINGLOG TCP CONSUMER                      ");
        println!("============================================================");
        println!(
            "[Consumer] Connecting to broker at 127.0.0.1:12000 starting at offset {}...\n",
            start_offset
        );

        tokio_uring::start(async move {
            let stream = TcpStream::connect("127.0.0.1:12000".parse().unwrap())
                .await
                .expect("Failed to connect to broker server. Is it running?");

            // Send Consumer handshake
            let _ = write_all(&stream, vec![0x02]).await.unwrap();

            // Send 8-byte starting offset
            let _ = write_all(&stream, start_offset.to_be_bytes().to_vec())
                .await
                .unwrap();
            println!("[Consumer] Connected. Streaming messages from the broker in real-time:\n");

            loop {
                // Read 4-byte length prefix
                let len_buf = vec![0u8; 4];
                let len_buf = match read_exact(&stream, len_buf, 4).await {
                    Ok(b) => b,
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        println!("[Consumer] Connection closed by server.");
                        break;
                    }
                    Err(e) => {
                        eprintln!("[Consumer Error] Failed reading record length: {:?}", e);
                        break;
                    }
                };
                let msg_len =
                    u32::from_be_bytes([len_buf[0], len_buf[1], len_buf[2], len_buf[3]]) as usize;

                // Read msg_len payload bytes
                let payload_buf = vec![0u8; msg_len];
                let payload = read_exact(&stream, payload_buf, msg_len).await.unwrap();
                let msg_str = String::from_utf8_lossy(&payload);
                println!("\x1b[32m[Stream] Received: '{}'\x1b[0m", msg_str);
            }
        });
    } else {
        // Mode 4: Runs standard integration demo
        println!("============================================================");
        println!("             RINGLOG: HIGH-PERFORMANCE BROKER              ");
        println!("============================================================");
        println!("Starting real-time integration demo on a single thread-local event loop...\n");
        tokio_uring::start(async {
            if let Err(e) = run_demo().await {
                eprintln!("[Demo Error] demo execution failed: {:?}", e);
            }
        });
    }
    Ok(())
}
