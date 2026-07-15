# syntax=docker/dockerfile:1.7

FROM golang:1.25.2-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal

ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/hold-expirer ./cmd/hold-expirer && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/outbox-worker ./cmd/outbox-worker

FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
    && addgroup -S railway \
    && adduser -S -G railway -h /nonexistent -s /sbin/nologin railway
COPY --from=build --chown=railway:railway /out/api /usr/local/bin/railway-api
COPY --from=build --chown=railway:railway /out/hold-expirer /usr/local/bin/hold-expirer
COPY --from=build --chown=railway:railway /out/outbox-worker /usr/local/bin/outbox-worker

USER railway:railway
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -T 2 -O /dev/null http://127.0.0.1:8080/livez || exit 1
ENTRYPOINT ["/usr/local/bin/railway-api"]
