#!/bin/bash
set -e

clickhouse-client -n <<-EOSQL
    -- Create database for Prometheus metrics
    CREATE DATABASE IF NOT EXISTS metrics;

    -- Table to store raw Prometheus metrics exports
    CREATE TABLE IF NOT EXISTS metrics.prometheus_exports (
        id UUID DEFAULT generateUUIDv4(),
        timestamp DateTime64(3) DEFAULT now(),
        cluster String,
        metric_name String,
        instance String,
        job String,
        labels Map(String, String),
        value Float64,
        export_timestamp DateTime64(3)
    ) ENGINE = MergeTree()
    ORDER BY (metric_name, timestamp)
    PARTITION BY toYYYYMM(timestamp);

    -- Table for time-series data (for Grafana queries)
    CREATE TABLE IF NOT EXISTS metrics.prometheus_timeseries (
        timestamp DateTime64(3),
        metric_name String,
        instance String,
        job String,
        labels String,
        value Float64
    ) ENGINE = MergeTree()
    ORDER BY (metric_name, timestamp)
    PARTITION BY toYYYYMM(timestamp)
    TTL toDateTime(timestamp) + INTERVAL 90 DAY;

    -- View for easy querying
    CREATE VIEW IF NOT EXISTS metrics.metrics_latest AS
    SELECT
        metric_name,
        instance,
        argMax(value, timestamp) as latest_value,
        max(timestamp) as last_seen
    FROM metrics.prometheus_timeseries
    GROUP BY metric_name, instance;
EOSQL
