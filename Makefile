
BASE_VERSION = 1.0.0
EXTRA_VERSION ?= $(shell git rev-parse  HEAD)
WORKSPACE=$(shell pwd)
PROJECT_NAME=mtcloud

# Tool commands (overridable)
GO_CMD             ?= go
GO_DEP_CMD         ?= dep
DOCKER_CMD         ?= docker
DOCKER_COMPOSE_CMD ?= docker-compose
IS_RELEASE=true
PKGNAME = mtcloud.com/mtstorage
# defined in version/version.go
METADATA_VAR = Version=$(BASE_VERSION)
METADATA_VAR += CommitSHA=$(EXTRA_VERSION)
METADATA_VAR += BaseVersion=$(BASEIMAGE_RELEASE)
METADATA_VAR += BaseDockerLabel=$(BASE_DOCKER_LABEL)
METADATA_VAR += DockerNamespace=$(DOCKER_NS)
METADATA_VAR += BaseDockerNamespace=$(BASE_DOCKER_NS)
METADATA_VAR += BuildDate="$(shell date "+%Y年%m月%d日%H:%M:%S" )"

DOCKER_REPO = harbor.mty.wang/mtoss
GO_LDFLAGS = $(patsubst %,-X $(PKGNAME)/version.%,$(METADATA_VAR))
ARCH=$(shell go env GOARCH)
ifneq ($(IS_RELEASE),true)
PROJECT_VERSION=$(BASE_VERSION)-$(EXTRA_VERSION)
APP_TAG ?= latest
else
PROJECT_VERSION=$(BASE_VERSION)
#APP_TAG ?= $(ARCH)-$(BASE_VERSION)
APP_TAG ?= latest
endif
export GO_LDFLAGS

BUILD_DIR ?= .build
SHELL := /bin/bash

# build chunker
.PHONY: all
all:  format
	@echo "building  binary "
	@mkdir -p $(BUILD_DIR)/bin

	go build -o $(BUILD_DIR)/bin/chunker  -ldflags "$(GO_LDFLAGS) -X $(PKGNAME)/version.ProgramName=chunker"  cmd/chunker/main.go
	go build -o $(BUILD_DIR)/bin/nameserver  -ldflags "$(GO_LDFLAGS) -X $(PKGNAME)/version.ProgramName=nameserver"  cmd/nameserver/main.go
	#go build -o $(BUILD_DIR)/bin/controller  -ldflags "${GO_LDFLAGS} -X $(PKGNAME)/version.ProgramName=controller"  cmd/controller/controller.go
	@scripts/publish.sh $(WORKSPACE) nameserver
	@scripts/publish.sh $(WORKSPACE) chunker
	@#scripts/publish.sh $(WORKSPACE) controller

.PHONY: build
build:
	@echo "building adapter binary "
	@mkdir -p $(BUILD_DIR)/bin
	go build -o $(BUILD_DIR)/bin/chunker  -ldflags "$(GO_LDFLAGS)"  cmd/chunker/*.go
	go build -o $(BUILD_DIR)/bin/nameserver  -ldflags "$(GO_LDFLAGS)"  cmd/nameserver/*.go
	go build -o $(BUILD_DIR)/bin/controller  -ldflags "$(GO_LDFLAGS)"  cmd/controller/*.go

.PHONY: chunker
chunker:
	@echo "building chunker binary "
	go build -o $(BUILD_DIR)/bin/chunker  -ldflags "$(GO_LDFLAGS)"  cmd/chunker/*.go

.PHONY: nameserver
nameserver:
	@echo "building nameserver binary "
	go build -o $(BUILD_DIR)/bin/nameserver  -ldflags "$(GO_LDFLAGS)"  cmd/nameserver/*.go

.PHONY: controller
controller:
	@echo "building controller binary "
	go build -o $(BUILD_DIR)/bin/controller  -ldflags "$(GO_LDFLAGS)"  cmd/controller/*.go

.PHONY: dist
dist: clean
	@echo "Package $(PROJECT_NAME) clean finished"

.PHONY: docker
docker: all
	@echo "Building  docker mtstorage image"
	@cp -r .build docker/nameserver
	sudo docker build -t $(DOCKER_REPO)/nameserver:$(APP_TAG) docker/nameserver
	@rm -rf  docker/nameserver/.build

	@cp -r .build docker/chunker
	sudo docker build -t $(DOCKER_REPO)/chunker:$(APP_TAG) docker/chunker
	@rm -rf  docker/chunker/.build

	@cp -r .build docker/controller
	sudo docker build -t $(DOCKER_REPO)/controller:$(APP_TAG) docker/controller
	@rm -rf  docker/controller/.build
	#docker push  $(DOCKER_REPO)/$(PROJECT_NAME):$(APP_TAG)
	sudo docker push  $(DOCKER_REPO)/chunker:$(APP_TAG)
	sudo docker push  $(DOCKER_REPO)/nameserver:$(APP_TAG)
	sudo docker push  $(DOCKER_REPO)/controller:$(APP_TAG)

.PHONY: docker_chunker
docker_chunker: all
	@echo "Building  docker chunker image"
	@cp -r .build docker/chunker
	docker build -t $(DOCKER_REPO)/chunker:$(APP_TAG) docker/chunker
	@rm -rf  docker/chunker/.build

	docker push  $(DOCKER_REPO)/chunker:$(APP_TAG)

.PHONY: docker_nameserver
docker_nameserver: all
	@echo "Building  docker meserver image"
	@cp -r .build docker/nameserver
	docker build -t $(DOCKER_REPO)/nameserver:$(APP_TAG) docker/nameserver
	@rm -rf  docker/nameserver/.build

	docker push  $(DOCKER_REPO)/nameserver:$(APP_TAG)

.PHONY: docker_controller
docker_controller: all
	@echo "Building  docker meserver image"
	@cp -r .build docker/controller
	docker build -t $(DOCKER_REPO)/controller:$(APP_TAG) docker/controller
	@rm -rf  docker/controller/.build

	docker push  $(DOCKER_REPO)/controller:$(APP_TAG)


format:
	@echo "go fmt"
	@go fmt ./...
	@echo "gofmt finished"
vet:
	@echo "go vet"
	@go vet ./...
	@echo "ok"
# download go module
.PHONY: mod
mod:
	go mod download

# clean
.PHONY: clean
clean:
	@rm -rf $(BUILD_DIR)
