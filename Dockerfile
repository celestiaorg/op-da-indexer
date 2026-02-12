ARG TARGET_BASE_IMAGE=alpine:3.23

FROM --platform=$BUILDPLATFORM golang:1.26-alpine3.23 AS builder
WORKDIR /app

ARG TARGETOS
ARG TARGETARCH
ARG GIT_COMMIT
ARG GIT_DATE

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    mkdir -p /app/bin && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath \
      -o /app/bin/op-da-indexer \
      ./cmd

FROM $TARGET_BASE_IMAGE AS op-da-indexer
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/bin/op-da-indexer /usr/local/bin/op-da-indexer
CMD ["op-da-indexer"]
