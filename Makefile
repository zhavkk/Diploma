BIN      := bin
MODULE   := github.com/zhavkk/Diploma
PROTO_DIR := api/proto
PROTO_OUT := api/proto/gen
COMPOSE  := docker compose -f deployments/docker-compose.yml

.PHONY: all build build-orchestrator build-node-agent build-cli \
        proto lint test clean \
        docker-build docker-build-cli up down logs local-env-up kill workload-logs \
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
	@echo "  local-env-up       — поднять полный локальный стенд с Swagger/monitoring"
	@echo "  kill               — убить текущую PostgreSQL master-ноду в локальном стенде"
	@echo "  workload-logs      — смотреть логи фонового PostgreSQL writer workload"
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
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

local-env-up: build
	$(COMPOSE) down -v --remove-orphans
	$(COMPOSE) up -d --build
	@echo "Waiting for local HA PostgreSQL cluster..."
	@for i in $$(seq 1 60); do \
		nodes=$$(./$(BIN)/ha-ctl --addr localhost:50051 nodes 2>/dev/null || true); \
		if curl -fsS http://localhost:8080/healthz >/dev/null 2>&1 && \
		   curl -fsS http://localhost:8080/api/v1/swagger.yaml >/dev/null 2>&1 && \
		   $(COMPOSE) exec -T log-writer psql -v ON_ERROR_STOP=1 -c "INSERT INTO ha_random_logs (source, level, message) VALUES ('make', 'info', 'local-env-up readiness probe');" >/dev/null 2>&1 && \
		   echo "$$nodes" | grep -Eq '^pg-primary[[:space:]]+primary[[:space:]]+yes' && \
		   echo "$$nodes" | grep -Eq '^pg-replica1[[:space:]]+replica[[:space:]]+yes' && \
		   echo "$$nodes" | grep -Eq '^pg-replica2[[:space:]]+replica[[:space:]]+yes'; then \
			break; \
		fi; \
		if [ "$$i" = "60" ]; then \
			echo "local-env-up: cluster did not become ready in time"; \
			$(COMPOSE) ps; \
			exit 1; \
		fi; \
		sleep 2; \
	done
	@echo ""
	@echo "Local HA PostgreSQL cluster is ready:"
	@echo "  Swagger UI:      http://localhost:8080/swagger/"
	@echo "  OpenAPI spec:    http://localhost:8080/api/v1/swagger.yaml"
	@echo "  Orchestrator:    http://localhost:8080/api/v1/status"
	@echo "  gRPC API:        localhost:50051"
	@echo "  Write endpoint:  localhost:5000"
	@echo "  Read endpoint:   localhost:5001"
	@echo "  HAProxy stats:   http://localhost:7000"
	@echo "  Prometheus:      http://localhost:9090"
	@echo "  Grafana:         http://localhost:3000 (admin/admin)"
	@echo ""
	@./$(BIN)/ha-ctl --addr localhost:50051 status
	@./$(BIN)/ha-ctl --addr localhost:50051 nodes

kill:
	@primary=$$(./$(BIN)/ha-ctl --addr localhost:50051 status | awk '/^Primary:/ {print $$2}'); \
	if [ -z "$$primary" ]; then \
		echo "kill: cannot determine current primary; is local-env-up running?"; \
		exit 1; \
	fi; \
	case "$$primary" in \
		pg-primary) svc="pg-primary" ;; \
		pg-replica1) svc="pg-replica1" ;; \
		pg-replica2) svc="pg-replica2" ;; \
		*) echo "kill: unknown primary node $$primary"; exit 1 ;; \
	esac; \
	echo "Killing current master node: $$primary ($$svc)"; \
	start_ms=$$(date +%s%3N); \
	$(COMPOSE) kill "$$svc"; \
	echo "Measuring RTO via HAProxy write endpoint..."; \
	rto_ms=""; \
	for i in $$(seq 1 450); do \
		if $(COMPOSE) exec -T log-writer env PGCONNECT_TIMEOUT=1 psql -v ON_ERROR_STOP=1 -c "INSERT INTO ha_random_logs (source, level, message) VALUES ('make', 'info', 'rto probe after killing $$primary');" >/dev/null 2>&1; then \
			end_ms=$$(date +%s%3N); \
			rto_ms=$$((end_ms - start_ms)); \
			break; \
		fi; \
		sleep 0.2; \
	done; \
	echo "Waiting for failover convergence..."; \
	converged=0; \
	for i in $$(seq 1 45); do \
		status=$$(./$(BIN)/ha-ctl --addr localhost:50051 status 2>/dev/null || true); \
		nodes=$$(./$(BIN)/ha-ctl --addr localhost:50051 nodes 2>/dev/null || true); \
		events=$$(./$(BIN)/ha-ctl --addr localhost:50051 events 2>/dev/null || true); \
		new_primary=$$(echo "$$status" | awk '/^Primary:/ {print $$2}'); \
		if [ -n "$$new_primary" ] && [ "$$new_primary" != "$$primary" ] && \
		   echo "$$nodes" | grep -Eq "^$$new_primary[[:space:]]+primary[[:space:]]+yes" && \
		   echo "$$events" | grep -Eq "^$$primary[[:space:]]+$$new_primary[[:space:]]+"; then \
			converged=1; \
			break; \
		fi; \
		sleep 2; \
	done; \
	echo ""; \
	./$(BIN)/ha-ctl --addr localhost:50051 status || true; \
	./$(BIN)/ha-ctl --addr localhost:50051 nodes || true; \
	./$(BIN)/ha-ctl --addr localhost:50051 events || true; \
	echo ""; \
	if [ -z "$$rto_ms" ]; then \
		echo "RTO: write endpoint did not recover in time"; \
		exit 1; \
	fi; \
	echo "RTO: $$rto_ms ms"; \
	if [ "$$converged" != "1" ]; then \
		echo "kill: failover did not converge in time"; \
		exit 1; \
	fi; \
	echo "Follow cluster logs with: make logs"; \
	echo "Follow workload logs with: make workload-logs"

workload-logs:
	$(COMPOSE) logs -f log-writer

# Monitoring commands
monitoring-up:
	$(COMPOSE) up -d prometheus grafana
	@echo "Monitoring started:"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Grafana:    http://localhost:3000 (admin/admin)"

monitoring-down:
	$(COMPOSE) stop prometheus grafana

monitoring-logs:
	$(COMPOSE) logs -f prometheus grafana

prometheus-reload:
	curl -X POST http://localhost:9090/-/reload
	@echo "Prometheus configuration reloaded"

grafana-reset:
	$(COMPOSE) stop grafana
	docker volume rm diploma_grafana-data 2>/dev/null || true
	$(COMPOSE) up -d grafana
	@echo "Grafana reset complete (admin/admin)"


# Validate Ansible playbook syntax
ansible-check:
	ansible-playbook --syntax-check -i deployments/ansible/inventory.yml deployments/ansible/site.yml

clean:
	rm -rf $(BIN) $(PROTO_OUT)
	@echo "cleaned"
