# Мониторинг HA PostgreSQL кластера

В состав кластера интегрированы Prometheus и Grafana для мониторинга и визуализации метрик.

## 🚀 Быстрый старт

### Запуск полного кластера с мониторингом
```bash
make up
```

### Запуск только мониторинга
```bash
make monitoring-up
```

### Доступ к интерфейсам

- **Prometheus:** http://localhost:9090
- **Grafana:** http://localhost:3000 (логин: `admin` / пароль: `admin`)

## 📊 Метрики

### Источники метрик

Система собирает метрики со следующих компонентов:

#### 1. Orchestrator (`orchestrator:8080/metrics`)
- `ha_cluster_nodes_healthy` — количество здоровых узлов
- `ha_failover_total` — количество попыток failover
- `ha_failover_duration_seconds` — длительность failover
- `ha_replication_reconfigure_total` — количество перенастроек репликации

#### 2. Node Agents (`node-agent-*:8081/metrics`)
- `ha_monitor_heartbeats_received_total` — количество полученных heartbeat'ов
- `ha_probe_collect_duration_seconds` — время сбора метрик
- `ha_replication_lag_bytes` — отставание репликации

#### 3. PostgreSQL Exporters (`postgres-exporter-*:9187/metrics`)
- `pg_stat_database_*` — статистика баз данных
- `pg_stat_activity_count` — количество подключений
- `pg_stat_database_size_bytes` — размер базы данных
- `pg_stat_replication_*` — статистика репликации

#### 4. HAProxy (`haproxy:7000/metrics`)
- Статистика балансировщика нагрузки

## 📈 Дашборды Grafana

### Основной дашборд: "HA PostgreSQL Cluster Monitoring"

Автоматически загружаемый дашборд содержит следующие панели:

1. **Healthy Nodes** — количество здоровых узлов кластера
2. **Replication Lag** — отставание репликации в байтах
3. **Heartbeat Rate** — частота heartbeat'ов от node-agent'ов
4. **Failover Events** — события failover
5. **Probe Collection Duration** — время сбора метрик
6. **Primary Database I/O** — операции ввода-вывода на primary
7. **Database Connections** — количество подключений к базам данных
8. **Database Size** — размер баз данных

### Настройка дашбордов

Дашборды автоматически provisioning'ятся из директории:
```
deployments/configs/grafana/dashboards/
```

Для добавления нового дашборда:
1. Создайте JSON файл в указанной директории
2. Перезапустите Grafana: `make monitoring-down && make monitoring-up`

## 🔧 Управление мониторингом

### Make команды

```bash
# Запуск мониторинга
make monitoring-up

# Остановка мониторинга
make monitoring-down

# Логи мониторинга
make monitoring-logs

# Перезагрузка конфигурации Prometheus
make prometheus-reload

# Сброс Grafana (удаление всех данных)
make grafana-reset
```

### Конфигурация Prometheus

Файл конфигурации: `deployments/configs/prometheus/prometheus.yml`

Для изменения параметров сбора метрик:
1. Отредактируйте `prometheus.yml`
2. Перезагрузите конфигурацию: `make prometheus-reload`

### Конфигурация Grafana

Файлы конфигурации:
- `deployments/configs/grafana/provisioning/datasources/prometheus.yml` — источник данных
- `deployments/configs/grafana/provisioning/dashboards/dashboards.yml` — provisioning дашбордов

## 🔍 Полезные запросы Prometheus

### Проверка здоровья кластера
```promql
ha_cluster_nodes_healthy
```

### Отставание репликации по узлам
```promql
ha_replication_lag_bytes
```

### Частота heartbeat'ов
```promql
rate(ha_monitor_heartbeats_received_total[5m])
```

### Количество событий failover
```promql
ha_failover_total
```

### Статистика PostgreSQL Primary
```promql
pg_stat_database_size_bytes{node_id="pg-primary"}
pg_stat_activity_count{node_id="pg-primary"}
```

### Репликация PostgreSQL
```promql
pg_stat_replication_lag_bytes
```

## 🚨 Алертинг (в разработке)

В будущем планируется интеграция с AlertManager для настройки алертов на основе метрик.

Примеры возможных алертов:
- Отсутствие heartbeat'ов от узла
- Высокое отставание репликации
- Мало здоровых узлов в кластере
- Длительный failover

## 📦 Хранение данных

### Prometheus
- **Директория:** Docker volume `prometheus-data`
- **Время хранения:** 200 часов (по умолчанию)
- **Изменение:** отредактируйте `--storage.tsdb.retention.time` в `docker-compose.yml`

### Grafana
- **Директория:** Docker volume `grafana-data`
- **Содержит:** дашборды, пользователи, настройки
- **Сброс:** `make grafana-reset`

## 🔒 Безопасность

### Prometheus
- Доступен без авторизации (для internal использования)
- Рекомендуется ограничить доступ через firewall

### Grafana
- Данные по умолчанию: `admin` / `admin`
- **Рекомендуется:** изменить пароль при первом входе
- Настройка через переменные окружения в `docker-compose.yml`

## 🛠️ Troubleshooting

### Prometheus не собирает метрики
1. Проверьте доступность endpoints: `curl http://localhost:8080/metrics`
2. Проверьте конфигурацию: `docker compose logs prometheus`
3. Перезагрузите конфигурацию: `make prometheus-reload`

### Grafana не показывает данные
1. Проверьте datasource: Configuration → Data Sources → Prometheus
2. Проверьте подключение к Prometheus: "Test" кнопка
3. Проверьте временной диапазон на дашборде

### Дашборды не загружаются
1. Проверьте provisioning: `docker compose logs grafana | grep provisioning`
2. Проверьте права доступа к файлам дашбордов
3. Сбросьте Grafana: `make grafana-reset`

## 📚 Дополнительные ресурсы

- [Prometheus документация](https://prometheus.io/docs/)
- [Grafana документация](https://grafana.com/docs/)
- [PostgreSQL Exporter](https://github.com/prometheus-community/postgres_exporter)
- [HAProxy Exporter](https://github.com/prometheus/haproxy_exporter)
