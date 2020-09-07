
project = git-foundation
sha = $(shell git rev-parse --short HEAD)

build: ## Build the binary.
	go build -ldflags "$(ldflags)" -o dist/$(project)

tools:
	@mkdir -p $(TOOLS_BIN_DIR)
	@curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(TOOLS_BIN_DIR) v$(GOLANGCILINT_VERSION) > /dev/null 2>&1

deps: tools ## Install dependencies.
	go mod download

tidy:
	go mod tidy

lint: tools
	$(TOOLS_BIN_DIR)/golangci-lint run

test:
	go test ./...

ci: lint test

ls-keys:
	fdbcli --exec 'getrange "" \xFF'

fdb-status:
	fdbcli --exec 'status'