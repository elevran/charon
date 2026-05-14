BINARY     := charon
CMD        := ./cmd/charon
BUILD_DIR  := ./build
IMAGE_NAME ?= charon
IMAGE_TAG  ?= latest

GOOS   ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

OUT := $(BUILD_DIR)/$(GOOS)/$(GOARCH)/$(BINARY)

.PHONY: all fmt lint test tidy update build image clean

all: fmt tidy lint test build

fmt:
	go fmt ./...

lint:
	golangci-lint run ./...

test:
	go test ./... -race

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
