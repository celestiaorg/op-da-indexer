VERSION := env('VERSION', `git describe --tags --exact-match 2> /dev/null || echo untagged`)
GITCOMMIT := env('GITCOMMIT', `git rev-parse HEAD 2> /dev/null || true`)
GITDATE := env('GITDATE', `git show -s --format='%ct' 2> /dev/null || true`)
BINARY := "./bin/op-celestia-indexer"

# Build op-celestia-indexer binary
op-celestia-indexer:
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
