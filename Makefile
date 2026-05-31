SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

MODULE      := github.com/lannister-dev/go-node-agent
BIN_DIR     := bin
BIN         := $(BIN_DIR)/agent
PKG         := ./...
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_TIME  := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS     := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildTime=$(BUILD_TIME)
IMAGE       ?= harbor.lannister-dev.ru/vpn/node-agent
IMAGE_TAG   ?= $(VERSION)
IMAGE_ENTRY_PROXY ?= harbor.lannister-dev.ru/vpn-service/entry-proxy

.PHONY: help
help:
	@awk 'BEGIN{FS=":.*##"; printf "Targets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: tools
tools: ## Install dev tools (buf, golangci-lint, protoc-gen-go, mockery, govulncheck)
	go install github.com/bufbuild/buf/cmd/buf@latest
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/vektra/mockery/v2@latest
	go install golang.org/x/vuln/cmd/govulncheck@latest

.PHONY: build
build: ## Build static binary → bin/agent
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o $(BIN) ./cmd/agent

.PHONY: run
run: ## Run agent locally
	go run ./cmd/agent

.PHONY: test
test: ## Run all tests with race detector
	go test -race -count=1 -timeout=60s $(PKG)

.PHONY: cover
cover: ## Run tests with coverage report
	go test -race -count=1 -coverprofile=coverage.out -covermode=atomic $(PKG)
	go tool cover -html=coverage.out -o coverage.html

.PHONY: fuzz
fuzz: ## Run all fuzz targets for 60s each
	@for pkg in $$(go list ./... | xargs -I {} sh -c 'grep -l "func Fuzz" $$(go list -f "{{.Dir}}" {})/*.go 2>/dev/null | head -1 && echo {}' | grep -v "^$$"); do \
		echo "fuzzing $$pkg"; \
		go test -fuzz=. -fuzztime=60s $$pkg || exit 1; \
	done

.PHONY: vet
vet: ## go vet
	go vet $(PKG)

.PHONY: lint
lint: ## golangci-lint
	golangci-lint run --timeout=5m

.PHONY: tidy
tidy: ## go mod tidy
	go mod tidy

.PHONY: vuln
vuln: ## govulncheck
	govulncheck $(PKG)

.PHONY: proto
proto: ## Regenerate protobuf via buf
	cd api/proto && buf generate

.PHONY: proto-lint
proto-lint: ## Lint .proto files
	cd api/proto && buf lint

.PHONY: proto-breaking
proto-breaking: ## Check proto for breaking changes vs main
	cd api/proto && buf breaking --against '../../.git#branch=main,subdir=api/proto'

.PHONY: mocks
mocks: ## Regenerate mocks
	mockery

.PHONY: docker
docker: ## Build container image
	docker build -t $(IMAGE):$(IMAGE_TAG) .

.PHONY: docker-push
docker-push: docker ## Build + push image
	docker push $(IMAGE):$(IMAGE_TAG)

.PHONY: build-entry-proxy
build-entry-proxy: ## Build embedded entry proxy (needs with_utls for REALITY)
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 go build -trimpath -tags with_utls -ldflags="-s -w" -o $(BIN_DIR)/entry-proxy ./cmd/entry-proxy

.PHONY: docker-entry-proxy
docker-entry-proxy: ## Build entry-proxy image (with_utls)
	docker build -f Dockerfile.entry-proxy -t $(IMAGE_ENTRY_PROXY):$(IMAGE_TAG) .

.PHONY: docker-entry-proxy-push
docker-entry-proxy-push: docker-entry-proxy ## Build + push entry-proxy image
	docker push $(IMAGE_ENTRY_PROXY):$(IMAGE_TAG)

.PHONY: smoke
smoke: ## Run docker-gated smoke tests against real sing-box (requires Docker)
	go test -tags=smoke -race -count=1 -timeout=5m ./test/smoke/...

.PHONY: ci
ci: vet lint test vuln ## Full CI suite

.PHONY: clean
clean:
	rm -rf $(BIN_DIR) dist coverage.out coverage.html
