BIN_DIR    := ./bin
IMAGE_NAME ?= charon
IMAGE_TAG  ?= latest

OPENRESPONSES_DIR ?= ../openresponses

.PHONY: all fmt fmt-check vet lint presubmit test test-unit test-integration test-compliance test-disruptive test-system test-compliance-bun tidy update build image clean

all: fmt tidy lint test build

# ── Formatting ────────────────────────────────────────────────────────────────

fmt:
	go fmt ./...

# fmt-check reports unformatted files without modifying them (used in CI and presubmit).
fmt-check:
	@out=$$(gofmt -l .); [ -z "$$out" ] || (printf "unformatted files:\n$$out\nrun 'go fmt ./...' to fix\n" && exit 1)

# ── Static analysis ───────────────────────────────────────────────────────────

vet:
	go vet ./...

lint:
	golangci-lint run ./...

# ── Presubmit (fast local gate, target <5s with warm cache) ───────────────────
#
# Covers: format check, vet, build, and all tests that do not require real
# timers or disk-heavy backends (-short skips those; they run in full CI).
presubmit: fmt-check vet
	go test -short -count=1 ./...

# ── Test targets ──────────────────────────────────────────────────────────────

# test: full suite including race detector (matches CI).
test:
	go test -race ./...

# test-unit: all non-integration, non-compliance tests with race detector.
test-unit:
	go test -race $$(go list ./... | grep -v '^github.com/elevran/charon/test/')

# test-integration: wires up the full stack in-process and checks end-to-end paths.
test-integration:
	go test -race ./test/integration/...

# test-compliance: Go compliance suite (mock inference, no external deps).
# Tests moved from test/compliance/ into cmd/proxy/ (package main).
test-compliance:
	go test -race ./cmd/proxy/...

# test-disruptive: end-to-end disruptive tests (proxy/stack failure paths).
# Tests moved from test/disruptive/ into cmd/proxy/ (package main).
test-disruptive:
	go test -race ./cmd/proxy/...

# test-system: canonical openresponses.org suite via bun.
# Requires: bun (https://bun.sh) and OPENRESPONSES_DIR set to a clone of
# https://github.com/openresponses/openresponses
# Note: build tag uses underscores because Go does not allow hyphens in tags.
test-system: test-compliance-bun

test-compliance-bun:
	OPENRESPONSES_DIR=$(OPENRESPONSES_DIR) \
	go test -tags openresponses_bun_compliance -v -count=1 \
	  ./cmd/proxy/... -run TestBunComplianceSuite

# ── Dependency management ─────────────────────────────────────────────────────

tidy:
	go mod tidy

update:
	go get -u ./...
	go mod tidy

# ── Build & package ───────────────────────────────────────────────────────────

build:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/charon ./cmd/charon
	go build -o $(BIN_DIR)/proxy ./cmd/proxy

image:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

clean:
	rm -rf $(BIN_DIR)
