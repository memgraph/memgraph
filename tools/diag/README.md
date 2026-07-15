# diag — Memgraph diagnostic poller (usage guide)

A small, **read-only** Python script that watches a running Memgraph instance and records a
**timeseries** of its memory, transaction, and per-database storage state over time — so we
can play it back, graph it, and dig into *why* RAM grows and what state the instance is in
right before a restart. It **does not draw conclusions or diagnose** — it faithfully logs the
`SHOW ... INFO` results (plus OS counters) at each interval for us to analyze. It never writes
to your database and never changes any configuration.

It is **multi-tenant aware**: it polls every database from `SHOW DATABASES` (not just the
default `memgraph`), so data living in a non-default database is captured correctly.

---

## 1. Requirements
- Python 3.9+
- The Neo4j driver: `pip install neo4j`
- Network access to the instance's Bolt port (default 7687). Runs fine from your laptop, a
  jump box, or inside the pod.

## 2. Quick start (run this during a normal ingestion window)
```bash
# from anywhere that can reach Bolt:
python3 diag.py \
  --bolt-address <HOST>:7687 \
  --username <user> --password <pass> \
  --output-dir ./diag-out
```
Leave it running through the memory-growth period and, ideally, across a restart. To run it
unattended in the background:
```bash
nohup python3 diag.py --bolt-address <HOST>:7687 --username <user> --password <pass> \
      --output-dir ./diag-out > diag-out/console.log 2>&1 &
```
Stop it any time with `Ctrl-C` (or `kill -INT <pid>`); it flushes and exits cleanly.

Sanity-check the connection first with a single snapshot (prints clean JSON to stdout):
```bash
python3 diag.py --bolt-address <HOST>:7687 --username <user> --password <pass> --once | jq .
```

## 3. Fuller signal (optional): run it inside the pod
If you can run it in the Memgraph pod/container, add `--os` to also capture process RSS,
thread/fd counts, the **container cgroup memory limit vs usage**, and OOM-killer messages —
this reconciles what the monitoring console shows against Memgraph's own memory limit.
```bash
python3 diag.py --bolt-address 127.0.0.1:7687 --username <user> --password <pass> --os --output-dir /tmp/diag-out
```
(If Memgraph's Enterprise metrics HTTP endpoint is enabled, you may also pass
`--metrics-url http://<host>:9091/`, but it's optional — the same counters are collected over
Bolt regardless.)

## 4. What it writes (into `--output-dir`)
| File | Contents |
|---|---|
| `diag_<ts>.jsonl` | one JSON record per sample (every 20 s) — the full raw data, for us to analyze |
| `diag_<ts>.log` | a compact human-readable line per sample + warnings |
| `diag_crashes.log` | a record each time the instance restarts, **plus the samples leading up to it** |

## 5. The human log line (a quick at-a-glance view of each sample)
The `.jsonl` file is the source of truth; the `.log` file mirrors each sample as one plain
line of values (no interpretation), e.g.:
```
ts=... conn=up mem_pct=40.0 gap_bytes=26843545600 unreleased_deltas=0 active_txns=3 oldest_txn_s=812 read_rate=0 write_rate=48 delete_rate=0 tenants=[example-db:v=1.0M,idx=8]
```
These are just the recorded values per interval. What they are (for reference when graphing):
- **mem_pct** — memory used as a % of Memgraph's `--memory-limit`.
- **gap_bytes** — non-storage memory = total tracked − sum of all tenants' graph memory
  (i.e. query/plan/AST-cache memory).
- **unreleased_deltas** — un-garbage-collected version records.
- **active_txns / oldest_txn_s** — active transaction count and age of the oldest.
- **read_rate / write_rate / delete_rate** — per-second query/delete rates.
- **tenants=[…]** — per-database vertex/index counts.

## 6. Graphing the timeseries
Flatten a captured `.jsonl` into a wide CSV (one row per timestamp) for plotting in a
spreadsheet / pandas / Grafana:
```bash
python3 diag.py --to-csv diag-out/diag_<ts>.jsonl --csv-out diag-out/timeseries.csv
```
Purely the recorded numbers (no analysis), so you can chart any column over time. Columns:
- **Instance-wide**: `ts`, `total_memory_tracked_bytes`, `runtime_limit_bytes`,
  `memory_res_bytes`, `memory_vs_runtime_limit_pct`, `non_storage_gap_bytes`,
  `tenant_total_memory_bytes_sum`, `unreleased_delta_objects_total`, `active_txn_count`,
  `longest_txn_age_s`, `vertex_count_total`, `edge_count_total`.
- **Counters + rates**: `ReadQuery`, `ReadQuery_rate_per_s`, `WriteQuery`(+rate),
  `DeletedNodes`(+rate), `DeletedEdges`(+rate), commit/rollback.
- **Per tenant** (one set of columns per database): `<db>__vertex_count`, `<db>__edge_count`,
  `<db>__graph_mem`, `<db>__query_mem`, `<db>__embedding_mem`, `<db>__total_mem`,
  `<db>__index_count`, `<db>__constraint_count`, `<db>__unreleased_delta_objects`.

The raw `.jsonl` has everything (top-level keys `ts, tick, bolt, metrics_http, os, derived,
timings_s`); the CSV is just the graph-friendly numeric projection. `--csv-out` defaults to
the input path with a `.csv` suffix.

Note on `non_storage_gap_bytes`: it is `total_memory_tracked − Σ(per-tenant total memory)` —
process-global memory not attributed to any database. On v3.10.1 the AST/query-plan cache is
tracked only here (never in a per-DB bucket), but so are Bolt buffers, thread stacks, etc., so
treat it as "global overhead" and correlate with the other columns rather than reading it as a
single cause.

## 7. What to send back to us
The whole `--output-dir` folder — especially the `.jsonl` timeseries and `diag_crashes.log`.
That gives us the full memory/transaction trajectory and the exact recorded state right before
a restart, to graph and analyze.

---

### Notes
- **Credentials:** if your instance has no auth, pass empty strings (`--username "" --password ""`).
- **Restrict tenants:** by default it polls all databases; use `--databases db1,db2` to limit.
- **Version:** tuned for your Memgraph **v3.10.1** (field names are resolved per-version).
- **Overhead:** negligible — a handful of `SHOW` queries every 20 s.
- Verify the script itself is healthy any time with `python3 diag.py --selftest`.
