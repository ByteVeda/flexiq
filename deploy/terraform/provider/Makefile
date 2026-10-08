# Task entrypoints for the Terraform provider, one name per thing CI does.

# Pinned, and kept in step with sdks/go/Makefile, so both modules lint alike.
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

.PHONY: binary
binary: ## Write the provider binary, terraform-provider-flexiq
	$(GO) build -o terraform-provider-flexiq .

.PHONY: vet
vet: ## Run go vet
	$(GO) vet $(PACKAGES)

.PHONY: test
test: ## Run the unit tests under the race detector
	$(GO) test -race -count=1 $(PACKAGES)

.PHONY: server
server: ## Build the flexiq-server that `make testacc` drives
	cargo build -j1 --manifest-path ../../../Cargo.toml -p flexiq-server --features grpc

.PHONY: testacc
testacc: ## Run the acceptance tests against a real flexiq-server — `make server` first
	# terraform-plugin-testing downloads a terraform binary into a temp
	# directory when none is on PATH or in TF_ACC_TERRAFORM_PATH.
	TF_ACC=1 $(GO) test -race -count=1 -timeout 20m $(PACKAGES)

.PHONY: fmt
fmt: ## Rewrite formatting and import grouping
	$(GOLANGCI_LINT) fmt

.PHONY: lint
lint: ## Run golangci-lint with the committed config
	$(GOLANGCI_LINT) run

.PHONY: tidy
tidy: ## Prune go.mod and go.sum
	$(GO) mod tidy

.PHONY: check
check: build vet lint test ## Everything CI runs, in CI's order

.PHONY: tools
tools: ## Install the pinned linter into GOBIN
	$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
