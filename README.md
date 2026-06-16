# redrust

A minimal Redis clone written in Rust — a learning project built from raw TCP up.

`redrust` is an in-memory key-value server that speaks the actual [RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/) (REdis Serialization Protocol), so it works with the official `redis-cli` and any standard Redis client.

## Features

- TCP server built on [`tokio`](https://tokio.rs/) — handles multiple clients concurrently
- Hand-written RESP protocol parser (no parsing libraries)
- Shared, thread-safe key-value store via `Arc<RwLock<HashMap>>`
- Supports `SET` and `GET`
- RESP-encoded responses — compatible with `redis-cli`

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
3. **Storage** — commands operate on a `HashMap<String, String>` shared across all connections behind an `Arc<RwLock>` (many concurrent reads, exclusive writes).
4. **Responses** — replies are encoded back into RESP so standard Redis clients understand them.

## Roadmap

- [ ] `DEL` / `EXISTS`
- [ ] Key expiry / TTL
- [ ] More data types (lists, hashes)
- [ ] Persistence

## Why

Built to learn Rust's strengths — async networking, fearless concurrency, and zero-cost abstractions — by implementing something where performance and safety actually matter.
