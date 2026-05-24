<div align="center">

# buffr

### Record API traffic once. Replay it forever.

A transparent proxy for HTTP, SSE and WebSocket APIs.<br />
First run captures every interaction. Every run after that is instant, free and deterministic.

<br />

<img src="https://img.shields.io/badge/HTTP-4A90D9?style=for-the-badge&logo=globe&logoColor=white" />
<img src="https://img.shields.io/badge/SSE-8B5CF6?style=for-the-badge&logo=rss&logoColor=white" />
<img src="https://img.shields.io/badge/WebSocket-F59E0B?style=for-the-badge&logo=socketdotio&logoColor=white" />
<img src="https://img.shields.io/badge/Docker-0EA5E9?style=for-the-badge&logo=docker&logoColor=white" />
<img src="https://img.shields.io/badge/Multi--target-6366F1?style=for-the-badge&logo=merge&logoColor=white" />
<img src="https://img.shields.io/badge/Zero_config-000000?style=for-the-badge&logo=checkmarx&logoColor=white" />

<br /><br />

[![Release](https://img.shields.io/github/v/release/RobinBially/buffr?style=flat-square&label=latest)](https://github.com/RobinBially/buffr/releases/latest)

</div>

---

## Why buffr?

Every test that hits a real API is a gamble. The response changes. The rate limit kicks in. The latency spikes. You pay per token.

buffr removes the API from the equation. Point it at any upstream, run your app once — every call is recorded to a JSON cassette. From then on, tests run against the cassette: zero latency, zero cost, zero flakiness. The app never knows the difference.

## How it works

```
your app → buffr → api.openai.com   (first run: records everything)
your app → buffr                    (every run after: replays from cassette)
```

In `auto` mode buffr serves cached responses when it has them and falls back to the real API when it doesn't — the cassette fills itself up incrementally.

## Modes

| Mode | Behaviour |
|------|-----------|
| **`auto`** | Replay on hit, record on miss — cassette builds up automatically |
| **`record`** | Forward all traffic to upstream and write to cassette |
| **`replay`** | Serve from cassette only, no network |

## Quickstart

```sh
buffr auto --target https://api.openai.com --port 8080
```

Point your app at `http://localhost:8080` instead of `https://api.openai.com`. Done. Cassette is auto-named `api.openai.com.json` in the current directory.

## Docker

```sh
docker run \
  -e BUFFR_MODE=auto \
  -e BUFFR_TARGET=https://api.openai.com \
  -v ./cassettes:/data \
  -p 8080:8080 \
  ghcr.io/robinbially/buffr:latest
```

### Multiple APIs, one container

Configure via `BUFFR_TARGETS` — each entry gets its own port and cassette:

```sh
docker run \
  -e BUFFR_TARGETS='
    - target: https://api.openai.com
      port: 8081
    - target: https://api.anthropic.com
      port: 8082
    - target: https://api.elevenlabs.io
      port: 8083
  ' \
  -v ./cassettes:/data \
  -p 8081:8081 -p 8082:8082 -p 8083:8083 \
  ghcr.io/robinbially/buffr:latest
```

`mode` and `cassette` are optional per entry — defaults to `auto` and `<host>.json`.

## Configuration

All flags have environment variable equivalents. Flags take precedence.

| Flag | Env | Default |
|------|-----|---------|
| `--target` | `BUFFR_TARGET` | — |
| `--port` | `BUFFR_PORT` | `8080` |
| `--cassette` | `BUFFR_CASSETTE` | `<target-host>.json` |
| _(subcommand)_ | `BUFFR_MODE` | — |

## What gets recorded

| Protocol | What buffr captures |
|----------|-------------------|
| **🌐 HTTP** | Request + response, any method, any path |
| **⚡ SSE** (`text/event-stream`) | Each chunk with original inter-chunk timing |
| **🔌 WebSocket** | Bidirectional frames in order, with per-frame delays |

Cassettes are plain JSON — readable in diffs, editable by hand.

## WebSocket example

```python
# Record once against the real API
import websocket
ws = websocket.create_connection("ws://localhost:8080/v1/realtime")
ws.send('{"type":"session.update","session":{"modalities":["text"]}}')
print(ws.recv())
ws.close()

# Replay in tests — same code, no network
```

## Logging

Every request is logged with method, path, status, duration and source:

```
time=12:34:56.123 level=INFO msg=listening mode=auto addr=:8080 cassette=api.openai.com.json
time=12:34:57.045 level=INFO msg="POST /v1/chat/completions" status=200 dur=823ms src=upstream
time=12:34:58.891 level=INFO msg="POST /v1/chat/completions" status=200 dur=2ms   src=cassette
time=12:34:59.001 level=WARN msg="POST /v1/embeddings"                            src=miss
time=12:35:00.450 level=INFO msg="WS /v1/realtime"           frames=14 dur=3.2s   src=cassette
```

## Development

```sh
go test ./...
go run ./cmd/buffr auto --target https://api.openai.com
```

## License

MIT
