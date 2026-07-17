SHELL := /usr/bin/env bash

BINARY := bin/cocoon-sandbox-operator
MAIN := ./cmd/cocoon-sandbox-operator
VERSION_PKG := github.com/cocoonstack/cocoon-sandbox-operator/internal/version

GIT_VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo unknown)
GIT_SHA ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE ?= $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
LD_FLAGS := -s -w -X $(VERSION_PKG).gitVersion=$(GIT_VERSION) -X $(VERSION_PKG).gitSHA=$(GIT_SHA) -X $(VERSION_PKG).buildDate=$(BUILD_DATE)

.DEFAULT_GOAL := all

.PHONY: all
all: fmt-check vet test build

.PHONY: build
build: ## Build the operator binary.
	mkdir -p bin
	go build -ldflags "$(LD_FLAGS)" -o $(BINARY) $(MAIN)

.PHONY: test
test: ## Run unit tests.
	go test ./...

.PHONY: test-race
test-race: ## Run unit tests with the race detector.
	go test -race ./...

.PHONY: coverage
coverage: ## Write unit-test coverage to bin/coverage.out.
	mkdir -p bin
	go test -coverprofile=bin/coverage.out ./...

.PHONY: vet
vet: ## Run go vet.
	go vet ./...

.PHONY: lint
lint: ## Run golangci-lint v2 when installed.
	golangci-lint run

.PHONY: fmt
fmt: ## Format Go source files.
	gofmt -w $$(find . -name '*.go' -not -path './.git/*')

.PHONY: fmt-check
fmt-check: ## Fail if a Go source file is not gofmt-clean.
	@test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './.git/*'))"

.PHONY: generate
generate: ## Regenerate CRDs, deep copies, and RBAC.
	go mod download -modfile=tools.mod
	go generate ./...

.PHONY: deps
deps: ## Download module dependencies.
	go mod download
	go mod download -modfile=tools.mod

.PHONY: clean
clean: ## Remove build and coverage outputs.
	rm -rf bin

.PHONY: help
help: ## Show available targets.
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
