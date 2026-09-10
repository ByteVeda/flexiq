# Task entrypoints for the Go client.
#
# Go has no package.json, so this file is what `pnpm run` is to sdks/node and
# `./gradlew` is to sdks/java: one name per thing CI does, so a contributor can
# reproduce a red check without reading the workflow.
#
# Every target here is also a step in .github/workflows/ci-go.yml. When one
# moves, both move.

# Pinned rather than @latest: a linter that changes its rule set on a Tuesday
# turns a green branch red on a morning nobody touched Go. Keep in step with
# GOLANGCI_LINT_VERSION in .github/workflows/ci-go.yml.
GOLANGCI_LINT_VERSION ?= v2.13.2
GOLANGCI_LINT ?= golangci-lint

GO ?= go
PACKAGES ?= ./...

.DEFAULT_GOAL := help

.PHONY: help
help: ## List the targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Compile every package
	$(GO) build $(PACKAGES)

.PHONY: vet
vet: ## Run go vet
	$(GO) vet $(PACKAGES)

.PHONY: test
test: ## Run the suite
	$(GO) test -count=1 $(PACKAGES)

.PHONY: race
race: ## Run the suite under the race detector, as CI does
	$(GO) test -race -count=1 $(PACKAGES)

.PHONY: server
server: ## Build the flexiq-server that `make e2e` drives
	cargo build --manifest-path ../../Cargo.toml -p flexiq-server --features grpc

.PHONY: e2e
e2e: ## Run the suite against a real flexiq-server — `make server` first
	$(GO) test -tags integration -count=1 $(PACKAGES)

.PHONY: cover
cover: ## Run the suite and write coverage.out
	$(GO) test -count=1 -coverprofile=coverage.out -coverpkg=./... $(PACKAGES)
	$(GO) tool cover -func=coverage.out | tail -1

.PHONY: fmt
fmt: ## Rewrite formatting and import grouping
	$(GOLANGCI_LINT) fmt

.PHONY: lint
lint: ## Run golangci-lint with the committed config
	$(GOLANGCI_LINT) run

.PHONY: tidy
tidy: ## Prune go.mod and go.sum
	$(GO) mod tidy

.PHONY: generate
generate: ## Regenerate internal/pb from ../../contracts/proto
	buf generate

.PHONY: check
check: build vet lint race ## Everything CI runs, in CI's order

.PHONY: tools
tools: ## Install the pinned linter into GOBIN
	$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
