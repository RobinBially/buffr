# ── build stage ──────────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /buffr ./cmd/buffr

# ── runtime stage ─────────────────────────────────────────────────────────────
# alpine keeps CA certificates (needed for HTTPS upstream in record mode) and
# a shell for interactive debugging, while staying small (~10 MB).
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /buffr /usr/local/bin/buffr

# Cassettes are expected to be mounted here.
VOLUME ["/data"]
WORKDIR /data

EXPOSE 8080

ENTRYPOINT ["buffr"]
