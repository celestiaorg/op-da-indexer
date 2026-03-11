VERSION := env('VERSION', `git describe --tags --exact-match 2> /dev/null || echo untagged`)
GITCOMMIT := env('GITCOMMIT', `git rev-parse HEAD 2> /dev/null || true`)
GITDATE := env('GITDATE', `git show -s --format='%ct' 2> /dev/null || true`)
BINARY := "./bin/op-da-indexer"
IMAGE := "ghcr.io/celestiaorg/op-da-indexer"
PLATFORM := "linux/amd64"

default:
    just --list

# Build op-da-indexer binary
build:
    mkdir -p bin
    go build -ldflags '-X main.GitCommit={{ GITCOMMIT }} -X main.GitDate={{ GITDATE }} -X main.Version={{ VERSION }}' -o {{ BINARY }} ./cmd

# Clean build artifacts
clean:
    rm -rf bin

# Run tests
test:
    go test -v ./...

# Run fuzzing tests
fuzz:
    go test -fuzz=. -fuzztime=10s ./...

# Build docker image, optionally push to ghcr
docker MODE="local":
    #!/usr/bin/env bash
    set -euo pipefail

    OUTPUT=$([ "{{ MODE }}" = "push" ] && echo "--push" || echo "--load")

    docker buildx build \
        -f Dockerfile \
        --target op-da-indexer \
        --platform {{ PLATFORM }} \
        -t {{ IMAGE }}:{{ VERSION }} \
        -t {{ IMAGE }}:latest \
        ${OUTPUT} \
        --progress plain \
        .
