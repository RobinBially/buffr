# buffr

**Record API traffic once. Replay it forever.**

`buffr` is a transparent proxy that sits between your app and any HTTP/WebSocket API. First run captures every interaction to a JSON cassette. Every run after that serves it locally — no network, no API keys, no flakiness.

```
your app → buffr → api.openai.com   (first run: records)
your app → buffr                    (every run after: replays)
```

## Modes

| Mode | What it does |
|------|-------------|
| `auto` | Record-on-miss — replays known requests, records new ones |
| `record` | Forward everything and write to cassette |
| `replay` | Serve from cassette only, no network |

## Quickstart

```sh
# One command. Cassette is auto-named after the target host.
buffr auto --target https://api.openai.com --port 8080
```

Point your app at `http://localhost:8080` instead of `https://api.openai.com`. Done.

## Docker

```sh
docker run -e BUFFR_MODE=auto \
           -e BUFFR_TARGET=https://api.openai.com \
           -v ./cassettes:/data \
           -p 8080:8080 \
           ghcr.io/robinbially/buffr:latest
```

### Multiple APIs in one container

```sh
docker run \
  -e BUFFR_0_TARGET=https://api.openai.com    -e BUFFR_0_PORT=8081 \
  -e BUFFR_1_TARGET=https://api.anthropic.com -e BUFFR_1_PORT=8082 \
  -e BUFFR_2_TARGET=https://api.elevenlabs.io -e BUFFR_2_PORT=8083 \
  -v ./cassettes:/data \
  -p 8081:8081 -p 8082:8082 -p 8083:8083 \
  ghcr.io/robinbially/buffr:latest
```

Each instance gets its own port and cassette (`api.openai.com.json`, …). Add `BUFFR_N_*` groups indefinitely — indices must be contiguous starting at 0.

## Configuration

Every flag has an environment variable equivalent. Flags take precedence.

| Flag | Env | Default |
|------|-----|---------|
| `--target` | `BUFFR_TARGET` | — |
| `--port` | `BUFFR_PORT` | `8080` |
| `--cassette` | `BUFFR_CASSETTE` | `<target-host>.json` |
| _(subcommand)_ | `BUFFR_MODE` | — |

## What gets recorded

- **HTTP** — request + response, any method, any path
- **SSE** (`text/event-stream`) — each chunk with original inter-chunk timing
- **WebSocket** — bidirectional frames in order, with delays

Cassettes are plain JSON — human-readable, diff-friendly, editable by hand.

## Development

```sh
go test ./...
go run ./cmd/buffr auto --target https://api.openai.com
```

## License

MIT
