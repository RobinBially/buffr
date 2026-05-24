# buffr

Record HTTP, SSE and WebSocket traffic once, replay it deterministically in tests.

`buffr` sits between a client and an upstream API. In `record` mode it
passes traffic through and saves every interaction to a JSON cassette. In
`replay` mode it serves those cassettes locally — same request shape in,
identical response out, including server-sent-event chunk timing and
bidirectional WebSocket frame ordering.

The original use case is testing LLM-driven code against real provider
responses (OpenAI chat completions over SSE, OpenAI Realtime over WebSocket)
without an LLM in the loop on every test run.

## Status

MVP. Supports:

- HTTP requests + responses
- Server-sent events (text/event-stream) — chunked replay with original timing
- WebSocket — bidirectional frame capture, drift detection on client-to-server frames

Out of scope (intentionally):

- TLS termination at the proxy (clients point at `http://` / `ws://`; the proxy
  handles upstream TLS itself).
- Request matching on non-deterministic bodies without a normalizer.
- Multi-cassette routing per port.

## Usage

```sh
# Record
buffr record --target https://api.openai.com --port 8080 --cassette session.json

# Replay
buffr replay --cassette session.json --port 8080
```

Point your client at `http://localhost:8080`. WebSocket clients use
`ws://localhost:8080/<original path>`.

## Cassette format

```json
{
  "version": 1,
  "interactions": [
    {
      "type": "http",
      "request": {
        "method": "POST",
        "path": "/v1/chat/completions",
        "headers": {"content-type": "application/json"},
        "body": "{...}"
      },
      "response": {
        "status": 200,
        "headers": {"content-type": "text/event-stream"},
        "body_chunks": [
          {"data": "data: {...}\n\n", "delay_ms": 0}
        ]
      }
    },
    {
      "type": "websocket",
      "request": {
        "path": "/v1/realtime",
        "query": "model=gpt-4o-realtime-preview",
        "headers": {"authorization": "<redacted>"}
      },
      "frames": [
        {"direction": "client_to_server", "opcode": "text", "data": "...", "delay_ms": 0},
        {"direction": "server_to_client", "opcode": "text", "data": "...", "delay_ms": 50}
      ]
    }
  ]
}
```

## Development

```sh
go build ./...
go test ./...
go run ./cmd/buffr replay --cassette examples/hello.json --port 8080
```

## License

MIT.
