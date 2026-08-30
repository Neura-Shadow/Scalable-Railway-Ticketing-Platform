# syntax=docker/dockerfile:1.7

FROM golang:1.25.13-alpine AS build
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
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/booking-command-reconciler ./cmd/booking-command-reconciler && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/physical-shard-admin ./cmd/physical-shard-admin && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/payment-sandbox ./cmd/payment-sandbox && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/payment-stripe-contract ./cmd/payment-stripe-contract && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/payment-worker ./cmd/payment-worker && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/payment-reconciler ./cmd/payment-reconciler && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/payment-admin ./cmd/payment-admin && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/settlement-worker ./cmd/settlement-worker && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/settlement-admin ./cmd/settlement-admin && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/dr-admin ./cmd/dr-admin && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/backup-admin ./cmd/backup-admin && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/migrate

FROM alpine:3.22 AS pgbackrest-build
ARG PGBACKREST_VERSION=2.59.0
ARG PGBACKREST_SHA256=faaf8faa14a6392279654ee216a493fcd07b0c513af4b55fe34faec062cb8875
RUN apk add --no-cache build-base bzip2-dev curl libssh2-dev libxml2-dev lz4-dev meson ninja openssl-dev pkgconf postgresql-dev zstd-dev \
    && curl --fail --location --show-error --silent --retry 5 \
        "https://github.com/pgbackrest/pgbackrest/releases/download/release/${PGBACKREST_VERSION}/pgbackrest-${PGBACKREST_VERSION}.tar.gz" \
        --output /tmp/pgbackrest.tar.gz \
    && echo "${PGBACKREST_SHA256}  /tmp/pgbackrest.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/pgbackrest.tar.gz -C /tmp \
    && meson setup --buildtype=release /tmp/pgbackrest-build "/tmp/pgbackrest-${PGBACKREST_VERSION}" \
    && ninja -C /tmp/pgbackrest-build \
    && install -D -m 0555 /tmp/pgbackrest-build/src/pgbackrest /out/pgbackrest \
    && test "$(/out/pgbackrest version)" = "pgBackRest ${PGBACKREST_VERSION}"

FROM postgres:16.14-alpine AS postgres-dr
RUN apk upgrade --no-cache libcrypto3 libssl3 \
    && apk add --no-cache jq libbz2 libssh2 libxml2 lz4-libs openssl zstd-libs \
    && install -d -o postgres -g postgres -m 0750 /var/lib/pgbackrest \
    && install -d -o postgres -g postgres -m 0700 /var/lib/postgresql/restore
COPY --from=pgbackrest-build --chown=root:root --chmod=0555 /out/pgbackrest /usr/bin/pgbackrest
COPY --chmod=0555 deploy/postgres/dr/pgbackrest-secret.sh /etc/railway/pgbackrest-secret.sh
COPY --chmod=0555 deploy/postgres/dr/restore-validation.sh /etc/railway/restore-validation.sh
COPY --chmod=0444 deploy/postgres/dr/pgbackrest.conf /etc/pgbackrest/pgbackrest.conf

FROM alpine:3.22 AS runtime
RUN apk upgrade --no-cache libcrypto3 libssl3 \
    && apk add --no-cache ca-certificates tzdata \
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
COPY --from=build --chown=railway:railway /out/booking-command-reconciler /usr/local/bin/booking-command-reconciler
COPY --from=build --chown=railway:railway /out/physical-shard-admin /usr/local/bin/physical-shard-admin
COPY --from=build --chown=railway:railway /out/payment-sandbox /usr/local/bin/payment-sandbox
COPY --from=build --chown=railway:railway /out/payment-stripe-contract /usr/local/bin/payment-stripe-contract
COPY --from=build --chown=railway:railway /out/payment-worker /usr/local/bin/payment-worker
COPY --from=build --chown=railway:railway /out/payment-reconciler /usr/local/bin/payment-reconciler
COPY --from=build --chown=railway:railway /out/payment-admin /usr/local/bin/payment-admin
COPY --from=build --chown=railway:railway /out/settlement-worker /usr/local/bin/settlement-worker
COPY --from=build --chown=railway:railway /out/settlement-admin /usr/local/bin/settlement-admin
COPY --from=build --chown=railway:railway /out/dr-admin /usr/local/bin/dr-admin
COPY --from=build --chown=railway:railway /out/backup-admin /usr/local/bin/backup-admin

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

FROM runtime AS booking-command-reconciler
EXPOSE 9090
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -T 2 -O /dev/null http://127.0.0.1:9090/livez || exit 1
ENTRYPOINT ["/usr/local/bin/booking-command-reconciler"]

FROM runtime AS read-model-admin
ENTRYPOINT ["/usr/local/bin/read-model-admin"]

FROM runtime AS shard-admin
ENTRYPOINT ["/usr/local/bin/shard-admin"]

FROM runtime AS physical-shard-admin
ENTRYPOINT ["/usr/local/bin/physical-shard-admin"]

FROM runtime AS payment-sandbox
USER root
RUN install -d -o railway -g railway -m 0700 /var/lib/payment-sandbox
USER railway:railway
EXPOSE 8099
HEALTHCHECK --interval=10s --timeout=3s --start-period=3s --retries=5 \
    CMD wget -q -T 2 -O /dev/null http://127.0.0.1:8099/readyz || exit 1
ENTRYPOINT ["/usr/local/bin/payment-sandbox"]

FROM runtime AS payment-stripe-contract
EXPOSE 8100
HEALTHCHECK --interval=10s --timeout=3s --start-period=3s --retries=5 \
    CMD wget -q -T 2 -O /dev/null http://127.0.0.1:8100/readyz || exit 1
ENTRYPOINT ["/usr/local/bin/payment-stripe-contract"]

FROM runtime AS payment-worker
EXPOSE 9090
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -T 2 -O /dev/null http://127.0.0.1:9090/livez || exit 1
ENTRYPOINT ["/usr/local/bin/payment-worker"]

FROM runtime AS payment-reconciler
EXPOSE 9090
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -q -T 2 -O /dev/null http://127.0.0.1:9090/livez || exit 1
ENTRYPOINT ["/usr/local/bin/payment-reconciler"]

FROM runtime AS payment-admin
ENTRYPOINT ["/usr/local/bin/payment-admin"]

FROM runtime AS settlement-worker
EXPOSE 9090
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -T 2 -O /dev/null http://127.0.0.1:9090/livez || exit 1
ENTRYPOINT ["/usr/local/bin/settlement-worker"]

FROM runtime AS settlement-admin
ENTRYPOINT ["/usr/local/bin/settlement-admin"]

FROM runtime AS dr-admin
ENTRYPOINT ["/usr/local/bin/dr-admin"]

FROM postgres-dr AS backup-admin
COPY --from=build --chown=postgres:postgres /out/backup-admin /usr/local/bin/backup-admin
USER postgres
ENTRYPOINT ["/usr/local/bin/backup-admin"]

FROM api AS final
