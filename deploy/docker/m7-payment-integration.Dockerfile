FROM golang:1.25.13-alpine

RUN apk add --no-cache build-base \
    && addgroup -S probe \
    && adduser -S -G probe -h /home/probe probe \
    && mkdir -p /home/probe/go /home/probe/.cache/go-build \
    && chown -R probe:probe /home/probe

WORKDIR /src
COPY go.mod go.sum ./

ENV GOPATH=/home/probe/go
ENV GOCACHE=/home/probe/.cache/go-build

USER probe

RUN go mod download
