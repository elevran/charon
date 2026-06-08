BINARY     := charon
CMD        := ./cmd/charon
BUILD_DIR  := ./build
IMAGE_NAME ?= charon
IMAGE_TAG  ?= latest

GOOS   ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

OUT := $(BUILD_DIR)/$(GOOS)/$(GOARCH)/$(BINARY)

OPENRESPONSES_DIR ?= ../openresponses

.PHONY: all fmt lint test test-compliance-bun tidy update build image clean

all: fmt tidy lint test build

fmt:
	go fmt ./...

lint:
	golangci-lint run ./...

test:
	go test ./... -race

# Runs the canonical openresponses.org compliance suite via bun.
# Requires: bun (https://bun.sh) and OPENRESPONSES_DIR set to a clone of
# https://github.com/openresponses/openresponses
# Note: build tag uses underscores because Go does not allow hyphens in tags.
test-compliance-bun:
	OPENRESPONSES_DIR=$(OPENRESPONSES_DIR) \
	go test -tags openresponses_bun_compliance -v -count=1 \
	  ./test/compliance/... -run TestBunComplianceSuite

tidy:
	go mod tidy

update:
	go get -u ./...
	go mod tidy

build:
	mkdir -p $(BUILD_DIR)/$(GOOS)/$(GOARCH)
	go build -o $(OUT) $(CMD)

image:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

clean:
	rm -rf $(BUILD_DIR)
