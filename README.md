<div align="center">

# buffr

### Record API traffic once. Replay it forever.

A VCR-style record/replay proxy for HTTP, SSE and WebSocket APIs — language-agnostic,<br />
drop-in for OpenAI, Anthropic or any upstream. First run records; every run after is<br />
instant, free and deterministic.

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

Every test that hits a real API is a gamble: responses drift, rate limits kick in, latency spikes, tokens cost money. buffr puts a proxy in front of any upstream and records every call to a JSON cassette. After that, your suite runs against the cassette — zero latency, zero cost, zero flakiness. The app never knows the difference.

```
your app → buffr → api.openai.com   first run: records everything
your app → buffr                    every run after: replays from cassette
```

It's a proxy, not a library — works with Python, Go, Node, Rust, anything that speaks HTTP. No fixtures, no hand-rolled mocks; the cassette _is_ the test data.

## Modes

| Mode | Behaviour |
|------|-----------|
| **`auto`** | Replay on hit, record on miss — cassette fills itself incrementally |
| **`record`** | Forward everything to upstream and write to cassette |
| **`replay`** | Serve from cassette only, no network |

## Quickstart

```sh
buffr auto --target https://api.openai.com --port 8080
```

Point your app at `http://localhost:8080` instead of `https://api.openai.com`. Done. The cassette is auto-named `api.openai.com.json` in the current directory.

## Docker

```sh
docker run \
  -e BUFFR_MODE=auto \
  -e BUFFR_TARGET=https://api.openai.com \
  -v ./cassettes:/data \
  -p 8080:8080 \
  ghcr.io/robinbially/buffr:latest
```

<details>
<summary><b>Multiple APIs, one container</b></summary>

<br />

`BUFFR_TARGETS` gives each upstream its own port and cassette. `mode` and `cassette` are optional per entry (default `auto`, `<host>.json`):

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

</details>

## Forward-proxy mode (catch-all)

Reverse-proxy modes need a `--target` per upstream and a configurable `base_url`. That can't reach hosts hardcoded inside a library — `huggingface.co` downloads, vendor SDKs, `wss://…/v1/realtime`.

Forward-proxy mode intercepts *everything* the client routes through it — a transport-level recorder (like VCR.py) but language-agnostic. The client sets standard proxy env vars and trusts buffr's CA; buffr terminates TLS with on-the-fly leaf certs and records/replays per destination host. No `base_url` changes.

```sh
buffr proxy --auto --port 8080      # or --record / --replay
buffr ca > buffr-ca.pem             # export the CA cert for the client to trust
```

Client side — no app code changes (`httpx`, `requests`, `aiohttp` all honor these):

```sh
export HTTPS_PROXY=http://buffr:8080
export HTTP_PROXY=http://buffr:8080
export NO_PROXY=localhost,127.0.0.1,database,qdrant,s3
export SSL_CERT_FILE=/path/to/buffr-ca.pem         # httpx, requests, aiohttp
export REQUESTS_CA_BUNDLE=/path/to/buffr-ca.pem    # requests / older stacks
```

The CA is minted on first start and persisted (`<data>/buffr-ca.pem` + `.key`), so the client trusts it once. HTTP, SSE and WebSocket are all intercepted; binary and gzip/br bodies are stored byte-faithfully.

> **Limitation:** cert-pinned or HSTS-preloaded SDKs reject any MITM cert by design — keep those on the reverse-proxy `--target` wiring. HTTP/2 is downgraded to HTTP/1.1 on the intercepted leg (egress to the real upstream may still use h2).

<details>
<summary><b>Per-host config (<code>BUFFR_PROXY</code>) and env vars</b></summary>

<br />

```yaml
BUFFR_PROXY: |
  mode: auto                       # auto | record | replay
  bypass: [localhost, 127.0.0.1, database, qdrant, s3]   # tunneled, never recorded
  hosts:
    - host: inference-shared.homeport.ai
      cassette: /data/vllm.json
      match:
        ignore:                    # same rules as reverse mode, per host
          - in: request.body
            pattern: 'chatcmpl-[A-Za-z0-9]{16,32}'
            replace_with: '<CHATCMPL_ID>'
            sync_response: true
    - host: google.serper.dev
      cassette: /data/serper.json
    - host: '*'                    # fallback for any other host
      cassette: /data/misc.json
```

- **`bypass`** — hosts (and subdomains) tunneled straight through, no TLS interception or recording, for infra/local services. The client's `NO_PROXY` is honored too.
- **Unlisted hosts** fall back to `'*'`, or — if absent — record to `<data>/<host>.json`. Unlisted is *not* the same as bypassed.
- Matching keys on **method + host + path + query + (normalized) body**, so a shared cassette never cross-matches between hosts.

| Env | Default | Purpose |
|-----|---------|---------|
| `BUFFR_PROXY` | — | YAML: mode, bypass list, per-host cassette + `match.ignore` |
| `BUFFR_CA_CERT` | `<data>/buffr-ca.pem` | CA cert path (also what `buffr ca` prints) |
| `BUFFR_CA_KEY` | `<data>/buffr-ca.key` | CA private key path |
| `BUFFR_DATA_DIR` | `.` | base dir for default cassettes + CA |

</details>

## What gets recorded

| Protocol | What buffr captures |
|----------|-------------------|
| **🌐 HTTP** | Request + response, any method, any path |
| **⚡ SSE** (`text/event-stream`) | Each chunk with original inter-chunk timing |
| **🔌 WebSocket** | Bidirectional frames in order, with per-frame delays |

Cassettes are plain JSON — readable in diffs, editable by hand.

## Configuration

All flags have environment variable equivalents; flags take precedence.

| Flag | Env | Default |
|------|-----|---------|
| `--target` | `BUFFR_TARGET` | — |
| `--port` | `BUFFR_PORT` | `8080` |
| `--cassette` | `BUFFR_CASSETTE` | `<target-host>.json` |
| _(subcommand)_ | `BUFFR_MODE` | — |

<details>
<summary><b>Faster replays (<code>BUFFR_REPLAY_NODELAY</code>)</b></summary>

<br />

buffr records the wall-clock delay before each SSE chunk / WebSocket frame. By default these delays are dropped on replay so chunks/frames are emitted back-to-back — payloads are identical, only the inter-chunk timing is gone. This keeps replays fast instead of re-spending the original generation time (often seconds per call) on every run.

Set `BUFFR_REPLAY_NODELAY=0` to reproduce the recorded cadence faithfully — do this when the streaming timing itself is under test.

</details>

<details>
<summary><b>Matching across non-deterministic requests (<code>match.ignore</code>)</b></summary>

<br />

When a request body or path carries per-run noise (a run ID, UUID, timestamp), no cassette entry ever matches and the hit rate collapses. `match.ignore` rewrites those substrings before matching — the same rule runs on the recorded and the live request, so they normalize to the same signature.

```yaml
BUFFR_TARGETS: |
  - target: http://192.168.178.27:1234
    port: 8083
    mode: auto
    cassette: /data/lm-studio.json
    match:
      ignore:
        - in: request.body
          pattern: '/runs/\d{8}-\d{6}-\d{3}/'
          replace_with: '/runs/<RUN_ID>/'
          sync_response: true   # echo the live run_id back in the response
        - in: request.path
          pattern: '/tasks/[0-9a-f-]{36}'
          replace_with: '/tasks/<TASK_ID>'
```

- **`in`** — `request.body` or `request.path`
- **`pattern`** — Go regex ([RE2](https://github.com/google/re2/wiki/Syntax))
- **`replace_with`** — literal replacement (use a placeholder like `<RUN_ID>` for readability)
- **`sync_response`** _(default false)_ — when the upstream echoes the ID back, buffr records the matched value and swaps in the live request's value at replay time, so the client sees its own ID, not the frozen one.

Invalid rules log a warning and are skipped — they don't take the proxy down.

</details>

<details>
<summary><b>WebSocket example & log format</b></summary>

<br />

```python
# Record once against the real API
import websocket
ws = websocket.create_connection("ws://localhost:8080/v1/realtime")
ws.send('{"type":"session.update","session":{"modalities":["text"]}}')
print(ws.recv())
ws.close()
# Replay in tests — same code, no network
```

Every request logs method, path, status, duration and source:

```
time=12:34:56.123 level=INFO msg=listening mode=auto addr=:8080 cassette=api.openai.com.json
time=12:34:57.045 level=INFO msg="POST /v1/chat/completions" status=200 dur=823ms src=upstream
time=12:34:58.891 level=INFO msg="POST /v1/chat/completions" status=200 dur=2ms   src=cassette
time=12:34:59.001 level=WARN msg="POST /v1/embeddings"                            src=miss
time=12:35:00.450 level=INFO msg="WS /v1/realtime"           frames=14 dur=3.2s   src=cassette
```

</details>

## Development

```sh
go test ./...
go run ./cmd/buffr auto --target https://api.openai.com
```

## License

MIT
