BIN      := bin
MODULE   := github.com/zhavkk/Diploma
PROTO_DIR := api/proto
PROTO_OUT := api/proto/gen

.PHONY: all build build-orchestrator build-node-agent build-cli \
        proto lint test clean \
        docker-build up down logs \
        deploy-local help

all: proto build

help:
	@echo "Targets:"
	@echo "  build              — собрать все бинарники в ./bin/"
	@echo "  build-orchestrator — собрать только orchestrator"
	@echo "  build-node-agent   — собрать только node-agent"
	@echo "  build-cli          — собрать только ha-ctl"
	@echo "  proto              — сгенерировать Go код из proto-файлов"
	@echo "  lint               — запустить golangci-lint"
	@echo "  test               — запустить тесты"
	@echo "  clean              — удалить bin/ и proto/gen/"
	@echo "  docker-build       — собрать Docker образы"
	@echo "  up                 — docker compose up -d"
	@echo "  down               — docker compose down"
	@echo "  logs               — docker compose logs -f"


build: build-orchestrator build-node-agent build-cli

build-orchestrator:
	@mkdir -p $(BIN)
	go build -o $(BIN)/orchestrator ./services/orchestrator/cmd
	@echo "orchestrator → $(BIN)/orchestrator"

build-node-agent:
	@mkdir -p $(BIN)
	go build -o $(BIN)/node-agent ./services/node-agent/cmd
	@echo "node-agent   → $(BIN)/node-agent"

build-cli:
	@mkdir -p $(BIN)
	go build -o $(BIN)/ha-ctl ./services/cli/cmd
	@echo "ha-ctl       → $(BIN)/ha-ctl"


proto:
	@rm -rf $(PROTO_OUT)
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=. --go_opt=paths=import --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=paths=import --go-grpc_opt=module=$(MODULE) \
		$(PROTO_DIR)/orchestrator.proto $(PROTO_DIR)/nodeagent.proto
	@echo "proto generated → $(PROTO_OUT)/"

lint:
	golangci-lint run ./...

test:
	go test ./... -v -race -count=1


docker-build:
	docker build -f deployments/Dockerfile.orchestrator -t ha-orchestrator:latest .
	docker build -f deployments/Dockerfile.node-agent   -t ha-node-agent:latest   .

up:
	docker compose -f deployments/docker-compose.yml up -d

down:
	docker compose -f deployments/docker-compose.yml down

logs:
	docker compose -f deployments/docker-compose.yml logs -f


clean:
	rm -rf $(BIN) $(PROTO_OUT)
	@echo "cleaned"
