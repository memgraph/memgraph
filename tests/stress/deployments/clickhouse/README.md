# ClickHouse + Grafana Metrics Stack

Store Prometheus metrics exports in ClickHouse and visualize with Grafana.

## Quick Start

### 1. Start the Stack

```sh
cd tests/stress/deployments/clickhouse
docker compose up -d
```

This starts:
- **ClickHouse** on ports 8123 (HTTP) and 9000 (native)
- **Grafana** on port 3000 with ClickHouse plugin pre-installed

### 2. Import Metrics

```sh
# Install requests if needed
pip install requests

# Import the metrics file
python3 import_metrics.py ../my_metrics.json
```

### 3. View in Grafana

1. Open http://localhost:3000
2. Login: `admin` / `admin`
3. Go to **Dashboards → Metrics → Metrics Overview**

The dashboard shows:
- Metrics summary table
- Memory & CPU time series
- Total metric names, data points, and instances

### 4. Query ClickHouse Directly

```sh
# Using clickhouse-client
docker exec -it clickhouse-metrics clickhouse-client

# Or via HTTP
curl 'http://localhost:8123/' --data-binary "SELECT * FROM metrics.prometheus_timeseries LIMIT 10"
```

Example queries:

```sql
-- Latest value for each metric
SELECT * FROM metrics.metrics_latest;

-- Count metrics by name
SELECT metric_name, count() as cnt
FROM metrics.prometheus_timeseries
GROUP BY metric_name
ORDER BY cnt DESC;

-- Time series for a specific metric
SELECT timestamp, value
FROM metrics.prometheus_timeseries
WHERE metric_name = 'up'
ORDER BY timestamp;
```

## Services

| Service | Port | URL |
|---------|------|-----|
| Grafana | 3000 | http://localhost:3000 |
| ClickHouse HTTP | 8123 | http://localhost:8123 |
| ClickHouse Native | 9000 | - |

## Stop

```sh
docker-compose down

# To also remove data
docker-compose down -v
```
