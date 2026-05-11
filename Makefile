BIN      := bin
MODULE   := github.com/zhavkk/Diploma
PROTO_DIR := api/proto
PROTO_OUT := api/proto/gen

.PHONY: all build build-orchestrator build-node-agent build-cli \
        proto lint test clean \
        docker-build docker-build-cli up down logs \
        ansible-check deploy-local help \
        monitoring-up monitoring-down monitoring-logs \
        prometheus-reload grafana-reset

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
	@echo "  monitoring-up      — запустить только мониторинг (Prometheus + Grafana)"
	@echo "  monitoring-down    — остановить мониторинг"
	@echo "  monitoring-logs    — логи мониторинга"
	@echo "  prometheus-reload  — перезагрузить конфигурацию Prometheus"
	@echo "  grafana-reset      — сбросить Grafana (удалить данные)"


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


docker-build: docker-build-cli
	docker build -f deployments/Dockerfile.orchestrator -t ha-orchestrator:latest .
	docker build -f deployments/Dockerfile.node-agent   -t ha-node-agent:latest   .

# Build CLI Docker image
docker-build-cli:
	docker build -f deployments/Dockerfile.cli -t ha-cli:latest .

up:
	docker compose -f deployments/docker-compose.yml up -d

down:
	docker compose -f deployments/docker-compose.yml down

logs:
	docker compose -f deployments/docker-compose.yml logs -f

# Monitoring commands
monitoring-up:
	docker compose -f deployments/docker-compose.yml up -d prometheus grafana
	@echo "Monitoring started:"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Grafana:    http://localhost:3000 (admin/admin)"

monitoring-down:
	docker compose -f deployments/docker-compose.yml stop prometheus grafana

monitoring-logs:
	docker compose -f deployments/docker-compose.yml logs -f prometheus grafana

prometheus-reload:
	curl -X POST http://localhost:9090/-/reload
	@echo "Prometheus configuration reloaded"

grafana-reset:
	docker compose -f deployments/docker-compose.yml stop grafana
	docker volume rm diploma_grafana-data 2>/dev/null || true
	docker compose -f deployments/docker-compose.yml up -d grafana
	@echo "Grafana reset complete (admin/admin)"


# Validate Ansible playbook syntax
ansible-check:
	ansible-playbook --syntax-check -i deployments/ansible/inventory.yml deployments/ansible/site.yml

clean:
	rm -rf $(BIN) $(PROTO_OUT)
	@echo "cleaned"
