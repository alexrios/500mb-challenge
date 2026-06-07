# The 500MB Club — Zig submission

A telemetry ingestion/query service for [The 500MB Club Challenge](https://github.com/The-500MB-Club/the_500mb_club_challenge),
written in **Zig 0.16** with a focus on correctness and maximum performance
inside the 2 CPU / 500 MiB budget.

## Architecture

```
k6 ──► nginx :8080 (strict round-robin) ──► 3× Zig API :8000 ──► Redis (ZSET)
```

- **API**: a single-threaded **epoll reactor** per replica. One non-blocking
  listener, a pool of client connections, and a single **pipelined** Redis
  connection that multiplexes every request. No libc, no threads, no
  per-request allocation — the binary is a ~260 KB fully-static musl executable
  that runs on `scratch`.
- **Storage**: Redis **sorted set per device** (`z:<id>`), scored by the
  telemetry timestamp. Tolerates out-of-order timestamps (telemetry is not
  monotonic), supports windowed range queries with offset pagination, and is
  trimmed (`ZREMRANGEBYRANK`) to a fixed size for bounded memory / flat
  stability. Samples are stored as packed 57-byte binary records, not JSON.
- **Correctness under round-robin**: persistence is acknowledged before the
  `202` is sent (the response is built on the `ZADD` ack), so a sample is
  visible to the other replicas the moment the client is told "accepted".

### Why this is fast and frugal

- Hand-written JSON parser for the fixed telemetry schema (no reflection).
- Binary records + manual response serialization.
- RESP pipelining: unlimited concurrent client requests over one Redis socket.
- Lazy, reused connection buffers — RSS tracks live concurrency, not capacity.

Measured locally (2-CPU / Docker budget): **aggregate RSS ≈ 32 MB** at the
base load, p99 ≈ 1.7–2.9 ms across all operations, zero errors at ~4.8k RPS.

## Layout

| Path | Purpose |
|---|---|
| `src/main.zig` | epoll reactor, routing, handlers |
| `src/http.zig` | HTTP/1.1 request parser |
| `src/json.zig` | telemetry parse / pack / serialize |
| `src/redis.zig` | RESP2 builders + reply scanners |
| `src/sys.zig` | raw Linux syscall wrappers (no libc) |
| `src/util.zig` | growable buffer |
| `Dockerfile` | self-contained multi-arch static build |
| `docker-compose.yml` | full stack (3 API + nginx + redis) |
| `nginx.conf` | strict round-robin load balancer |

## Build & run

Toolchain and tasks are pinned via [mise](https://mise.jdx.dev):

```bash
mise run build        # native ReleaseFast binary
mise run build-pi     # static aarch64-musl binary for the Raspberry Pi 5
mise run test         # unit tests
mise run up           # docker compose up (3 API + nginx + redis)
mise run smoke        # k6 smoke test through the LB
```

The published image is multi-arch:

```bash
IMAGE=ghcr.io/alexrios/500mb-zig:latest ./scripts/build-image.sh
```

> Local note: if host port `8080` is busy, run with `LB_HOST_PORT=18080`.

## Endpoints

Implements the full [API contract](https://github.com/The-500MB-Club/the_500mb_club_challenge/blob/master/docs/en/api.md):
`POST /devices/{id}/telemetry`, `.../telemetry/batch`,
`GET /devices/{id}/telemetry`, `.../anomaly`, plus `/healthz`, `/readyz`,
`/metrics`. Every response carries `X-Instance-Id`.

## License

MIT — see [LICENSE](LICENSE).
