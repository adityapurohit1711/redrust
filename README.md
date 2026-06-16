# redrust

A minimal Redis clone written in Rust — a learning project built from raw TCP up.

`redrust` is an in-memory key-value server that speaks the actual [RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/) (REdis Serialization Protocol), so it works with the official `redis-cli` and any standard Redis client.

## Features

- TCP server built on [`tokio`](https://tokio.rs/) — handles multiple clients concurrently
- Hand-written RESP protocol parser (no parsing libraries)
- Shared, thread-safe key-value store via `Arc<RwLock<HashMap>>`
- Lazy key expiry with per-key TTL
- RESP-encoded responses — compatible with `redis-cli`

## Supported commands

| Command | Description |
|---------|-------------|
| `SET key value` | Store a string value |
| `GET key` | Fetch a value (respects expiry) |
| `DEL key` | Delete a key |
| `EXPIRE key seconds` | Set a TTL on an existing key |
| `PING` | Health check — replies `PONG` |

## Running

Start the server (listens on `127.0.0.1:8008`):

```sh
cargo run
```

## Usage

### With `redis-cli`

```sh
redis-cli -p 8008
```

```
127.0.0.1:8008> SET foo bar
OK
127.0.0.1:8008> GET foo
"bar"
127.0.0.1:8008> EXPIRE foo 5
(integer) 1
127.0.0.1:8008> PING
PONG
```

### With raw RESP over `nc`

```sh
# SET foo bar
printf '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n' | nc localhost 8008

# GET foo
printf '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n' | nc localhost 8008
```

## How it works

1. **TCP layer** — `TcpListener` accepts connections; each client is handled in its own `tokio` task.
2. **Parsing** — incoming bytes are parsed as RESP arrays of bulk strings (`*N\r\n$len\r\nvalue\r\n...`) into a command and its arguments.
3. **Storage** — commands operate on a `HashMap<String, Entry>` shared across all connections behind an `Arc<RwLock>` (many concurrent reads, exclusive writes). Each `Entry` holds the value plus an optional expiry timestamp.
4. **Expiry** — keys expire lazily: an expired key is detected and removed on access (on `GET`), returning a nil reply.
5. **Responses** — replies are encoded back into RESP so standard Redis clients understand them.

## Benchmarks

Measured with `redis-benchmark`, 1,000,000 requests, 100 concurrent connections, against a 100k-key random keyspace, on the same machine over loopback:

```sh
redis-benchmark -p 8008 -n 1000000 -c 100 -r 100000 -t set,get
```

| Operation | redrust | real Redis | redrust vs Redis |
|-----------|---------|------------|------------------|
| SET | 102,291 req/s | 164,096 req/s | ~62% |
| GET | 105,463 req/s | 167,785 req/s | ~63% |

Latency (milliseconds):

| | redrust p50 / p99 | real Redis p50 / p99 |
|---|---|---|
| SET | 0.519 / 1.023 | 0.295 / 0.991 |
| GET | 0.503 / 0.703 | 0.295 / 0.783 |

A naive, fully memory-safe implementation sustains **~62% of production Redis throughput** with a competitive latency distribution — redrust's GET p99 (0.70ms) is actually on par with Redis's (0.78ms).

### Optimization notes

Two targeted changes moved GET throughput from ~68k to ~105k req/s:

1. **Removed per-response allocation.** The `GET` reply was built with `format!`, allocating a fresh `String` on every hit. Replaced with a per-connection `Vec<u8>` buffer that is reused (`clear()` + `write!` + `extend_from_slice`), eliminating steady-state allocation on the hot path.
2. **Removed per-request logging.** A `println!` on every command was serializing all connections on the stdout lock; removing it lifted both throughput and tail latency.

The remaining ~1.6× gap to Redis is mostly architectural: Redis is single-threaded with no lock overhead and batches I/O via its event loop, whereas redrust takes an `Arc<RwLock>` per operation and does a syscall per command.

## Roadmap

- [x] `SET` / `GET`
- [x] `DEL`
- [x] `EXPIRE` (lazy TTL)
- [x] `PING`
- [ ] `TTL` / `PERSIST`
- [ ] Active expiration (background sweep)
- [ ] More data types (lists, hashes)
- [ ] Persistence
- [ ] Sharded / lock-free store to close the throughput gap

## Why

Built to learn Rust's strengths — async networking, fearless concurrency, and zero-cost abstractions — by implementing something where performance and safety actually matter. The benchmarking exercise turned it into a lesson in *measuring* and *optimizing* a hot path, not just making it work.
