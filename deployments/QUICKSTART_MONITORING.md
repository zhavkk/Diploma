# 🚀 Быстрый старт: Мониторинг HA PostgreSQL кластера

## Запуск кластера с мониторингом

### 1. Сборка Docker образов
```bash
make docker-build
```

### 2. Запуск полного кластера
```bash
make up
```

Это запустит:
- etcd (координация)
- PostgreSQL primary + 2 реплики
- 3 node-agent'а
- Orchestrator
- HAProxy
- **Prometheus** (мониторинг)
- **Grafana** (визуализация)
- 3 PostgreSQL exporter'а

## 📊 Доступ к мониторингу

### Prometheus
- **URL:** http://localhost:9090
- **Что можно делать:**
  - Просматривать все метрики
  - Выполнять PromQL запросы
  - Проверять targets (статус сбора метрик)

### Grafana
- **URL:** http://localhost:3000
- **Логин:** `admin`
- **Пароль:** `admin`
- **Что уже настроено:**
  - Дашборд "HA PostgreSQL Cluster Monitoring"
  - Подключение к Prometheus
  - Автозагрузка дашбордов

## 🎯 Что посмотреть в первую очередь

### 1. Grafana дашборд
1. Откройте http://localhost:3000
2. Войдите под `admin`/`admin`
3. Откройте дашборд "HA PostgreSQL Cluster Monitoring"
4. Вы увидите:
   - Количество здоровых узлов
   - Отставание репликации
   - Частоту heartbeat'ов
   - События failover
   - Статистику PostgreSQL

### 2. Prometheus targets
1. Откройте http://localhost:9090
2. Перейдите в Status → Targets
3. Убедитесь, что все targets в состоянии "UP"

## 🔍 Полезные команды

### Управление мониторингом
```bash
# Только запустить мониторинг
make monitoring-up

# Остановить мониторинг
make monitoring-down

# Логи мониторинга
make monitoring-logs

# Перезагрузить конфиг Prometheus
make prometheus-reload

# Сбросить Grafana
make grafana-reset
```

### Проверка метрик
```bash
# Проверить метрики orchestrator
curl http://localhost:8080/metrics

# Проверить метрики node-agent primary
curl http://localhost:8081/metrics

# Проверить метрики node-agent replica 1
curl http://localhost:8082/metrics

# Проверить метрики node-agent replica 2
curl http://localhost:8083/metrics
```

### Тестирование failover с наблюдением в Grafana
```bash
# 1. Откройте Grafana дашборд
# 2. Остановите primary:
docker stop $(docker compose -f deployments/docker-compose.yml ps -q pg-primary)

# 3. Наблюдайте как:
#    - Healthy Nodes уменьшится до 2
#    - Произойдёт failover
#    - Replication Lag изменится
#    - Появятся записи в Failover Events

# 4. Восстановите primary:
docker start $(docker compose -f deployments/docker-compose.yml ps -q pg-primary)
```

## 📈 Основные метрики

### Метрики кластера
- `ha_cluster_nodes_healthy` — здоровье кластера
- `ha_failover_total` — количество failover'ов
- `ha_failover_duration_seconds` — время failover

### Метрики репликации
- `ha_replication_lag_bytes` — отставание репликации
- `ha_monitor_heartbeats_received_total` — heartbeat'и

### Метрики PostgreSQL
- `pg_stat_database_size_bytes` — размер БД
- `pg_stat_activity_count` — подключения
- `pg_stat_replication_*` — репликация

## 🛠️ Troubleshooting

### Нет данных в Grafana
1. Проверьте что Prometheus запущен:
   ```bash
   docker compose -f deployments/docker-compose.yml ps prometheus
   ```

2. Проверьте что Prometheus собирает метрики:
   ```bash
   # Откройте http://localhost:9090/targets
   ```

3. Проверьте datasource в Grafana:
   - Configuration → Data Sources → Prometheus → Test

### Дашборд не появился
1. Проверьте логи Grafana:
   ```bash
   docker compose -f deployments/docker-compose.yml logs grafana | grep provisioning
   ```

2. Сбросьте Grafana:
   ```bash
   make grafana-reset
   ```

### Порт уже занят
Если порты 9090 или 3000 уже заняты, измените их в `docker-compose.yml`:
```yaml
services:
  prometheus:
    ports:
      - "9091:9090"  # измените 9090 на 9091

  grafana:
    ports:
      - "3001:3000"  # измените 3000 на 3001
```

## 📚 Дополнительная информация

Полная документация по мониторингу: `deployments/MONITORING.md`

## 🎉 Готово!

Теперь у вас есть полностью рабочий HA PostgreSQL кластер с мониторингом!

Попробуйте:
1. Посмотреть на красивые графики в Grafana
2. Выполнить тестовый failover
3. Понаблюдать за метриками в реальном времени
4. Создать свои собственные дашборды
