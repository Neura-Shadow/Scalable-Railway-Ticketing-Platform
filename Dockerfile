# syntax=docker/dockerfile:1.7

FROM golang:1.25.12-alpine AS build
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
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/admission-worker ./cmd/admission-worker && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/hold-expirer ./cmd/hold-expirer && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/outbox-worker ./cmd/outbox-worker && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/read-model-worker ./cmd/read-model-worker && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/read-model-admin ./cmd/read-model-admin && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/shard-admin ./cmd/shard-admin && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/reconcile ./cmd/reconcile && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/migrate

FROM alpine:3.22 AS runtime
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S railway \
    && adduser -S -G railway -h /nonexistent -s /sbin/nologin railway
COPY --from=build --chown=railway:railway /out/api /usr/local/bin/railway-api
COPY --from=build --chown=railway:railway /out/admission-worker /usr/local/bin/admission-worker
COPY --from=build --chown=railway:railway /out/hold-expirer /usr/local/bin/hold-expirer
COPY --from=build --chown=railway:railway /out/outbox-worker /usr/local/bin/outbox-worker
COPY --from=build --chown=railway:railway /out/read-model-worker /usr/local/bin/read-model-worker
COPY --from=build --chown=railway:railway /out/read-model-admin /usr/local/bin/read-model-admin
COPY --from=build --chown=railway:railway /out/shard-admin /usr/local/bin/shard-admin
COPY --from=build --chown=railway:railway /out/reconcile /usr/local/bin/reconcile

USER railway:railway

FROM runtime AS api
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -T 2 -O /dev/null http://127.0.0.1:8080/livez || exit 1
ENTRYPOINT ["/usr/local/bin/railway-api"]

FROM runtime AS hold-expirer
EXPOSE 9090
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -T 2 -O /dev/null http://127.0.0.1:9090/livez || exit 1
ENTRYPOINT ["/usr/local/bin/hold-expirer"]

FROM runtime AS admission-worker
EXPOSE 9090
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -T 2 -O /dev/null http://127.0.0.1:9090/livez || exit 1
ENTRYPOINT ["/usr/local/bin/admission-worker"]

FROM runtime AS outbox-worker
EXPOSE 9090
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -T 2 -O /dev/null http://127.0.0.1:9090/livez || exit 1
ENTRYPOINT ["/usr/local/bin/outbox-worker"]

FROM runtime AS read-model-worker
EXPOSE 9090
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -T 2 -O /dev/null http://127.0.0.1:9090/livez || exit 1
ENTRYPOINT ["/usr/local/bin/read-model-worker"]

FROM runtime AS migrate
COPY --from=build --chown=railway:railway /out/migrate /usr/local/bin/migrate
COPY --chown=railway:railway migrations /migrations
ENTRYPOINT ["/usr/local/bin/migrate", "-path", "/migrations"]

FROM runtime AS reconcile
ENTRYPOINT ["/usr/local/bin/reconcile"]

FROM runtime AS read-model-admin
ENTRYPOINT ["/usr/local/bin/read-model-admin"]

FROM runtime AS shard-admin
ENTRYPOINT ["/usr/local/bin/shard-admin"]

FROM api AS final
