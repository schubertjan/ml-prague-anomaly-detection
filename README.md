# Anomaly Detection Feature Extraction — mock aggregation pipeline

Mock-up of a **kafka -> windowed aggregation -> ElasticSearch** pipeline:

- A **mock producer** pushes dummy documents into a kafka topic.
- A **preprocessing pipeline** consumes them, counts events in fixed time
  windows, and writes one metric document per (window, entity, metric,
  metric_value) to ES.
- **Each use case = one YAML config = one consumer group = one index.**
  Run as many pipelines as you want, side by side.

## Layout

```
configs/
  syslog.yaml             # group by facility; metrics: severity, tag (1m, share)
  access_log.yaml         # group by path; metric: status_code (5m, count)
  producer_syslog.yaml    # mock producer config for topic syslog-events
  producer_access.yaml    # mock producer config for topic access-log-events
agg_pipeline/
  config.py       # YAML use-case config, freq parsing (30s|1m|5m|1h)
  fields.py       # field lookup (flat dotted names: 'process.program' -> doc["process.program"])
  aggregator.py   # WindowAggregator: count per (window, entity, metric, metric_value)
  es_writer.py    # bulk writer to ES (auto-creates index, stable doc ids => idempotent upserts)
  consumer.py     # Kafka consumer per use case
  pipeline.py     # run loop: consume -> aggregate -> write; CLI: python -m agg_pipeline
mock_producer/    # dummy document generator + kafka producer; CLI: python -m mock_producer
data_generator.py # standalone streaming generator: python data_generator.py --topic syslog-events --rate 500
tests/            # pytest suite (kafka/ES are faked in tests, no infra needed)
docker-compose.yml  # kafka (KRaft) + elasticsearch for local runs
```

## Add a new use case

1. Copy a config, change what's relevant:

```yaml
use_case:
  name: my_usecase
  source: my_source                # -> output field "source"
  freq: 5m                         # window: 30s | 1m | 5m | 1h
  aggregation_level: facility      # FIELD to GROUP by -> output "aggregation_level";
                                   #   each value -> output "entity"   (e.g. daemon)
  metric_names:                    # one or more FIELDS to BREAK DOWN the group by;
    - severity                     #   each is aggregated independently -> output
    - tag                          #   "metric_name" = that field, each value ->
                                   #   "metric_value" (dotted names stay flat: 'a.b' is one key)
  value_type: share                # share -> value = n / all events for this entity and metric_name
                                   #                             in the window
                                   # count -> value = n
  index_name: syslog-5m            # exact ES index documents are written into
kafka:
  bootstrap_servers: localhost:9092
  topic: my-events
  consumer:
    group_id: agg-my-usecase       # must be unique per use case
    # optional TLS (per reference config): security_protocol, ssl_certfile,
    #   ssl_keyfile, ssl_cafile
elasticsearch:
  hosts:
    - http://localhost:9200
```

2. Run it as its own process:

```bash
uv run python -m agg_pipeline --config configs/my_usecase.yaml
```

## Output document (one per window, per entity, per metric, per metric_value)

Grouping by `facility` and breaking down by `severity` and `tag` yields,
per window, one doc per (facility-value, metric, metric-value) combination
that occurs:

| field               | meaning                                              |
|---------------------|------------------------------------------------------|
| `timestamp`         | **end** of the aggregation window (ISO 8601, UTC)    |
| `frequency`         | configured window, e.g. `1m`, `5m`                   |
| `aggregation_level` | the field grouped on, e.g. `facility`                |
| `entity`            | value of that field, e.g. `daemon`                   |
| `metric_name`       | the metric field this doc is for, e.g. `severity`    |
| `metric_value`      | value of that field, e.g. `warning`                  |
| `n`                 | events with this (entity, metric, metric_value) in the window |
| `value_type`        | `count` or `share`                                   |
| `value`             | `count` -> `n`; `share` -> `n` / all events for this entity and metric_name in the window |
| `source`            | source data set (e.g. `syslog`), from config         |

Each metric in `metric_names` is aggregated **independently**: its `share`
denominator is all events for this entity and metric_name in the window
(which equals the entity's event count, since every event has exactly one
value per metric field). So for a given entity the shares within one metric
sum to 1.

Doc ids are `window_start|entity|metric_name|metric_value`, so reprocessing
the same window overwrites the same ES document (idempotent).

Example (one doc: `facility=daemon`, `severity=warning`):

```json
{"timestamp": "2026-11-24T13:01:00Z", "frequency": "1m",
 "aggregation_level": "facility", "entity": "daemon",
 "metric_name": "severity", "metric_value": "warning",
 "n": 120, "value_type": "share", "value": 0.6,
 "source": "syslog"}
```

## Run it locally

```bash
uv sync                                   # create .venv + install deps

docker compose up -d kafka elasticsearch  # only the infra

# push dummy data (two independent producers)
uv run python -m mock_producer --config configs/producer_syslog.yaml
uv run python -m mock_producer --config configs/producer_access.yaml
# ... or generate a syslog stream at a higher rate, single file:
uv run python data_generator.py --topic syslog-events --rate 500

# run the two use-case pipelines (each = own consumer, group, index)
uv run python -m agg_pipeline --config configs/syslog.yaml
uv run python -m agg_pipeline --config configs/access_log.yaml
```

Check results:

```bash
curl -s 'localhost:9200/syslog-1m/_search?size=3&pretty'     # syslog, 1m windows
curl -s 'localhost:9200/access-5m/_search?size=3&pretty'     # access_log, 5m windows
```

Producer options: `--count N` (produce exactly N docs), `--rate R`,
`--dry-run` (print sample docs without kafka).

## Run everything with docker compose

One command runs the whole pipeline end-to-end: kafka, elasticsearch,
kibana, the syslog producer and the syslog aggregator pipeline.

```bash
docker compose up -d
```

- The python services use the `astral/uv` image (no custom build); each runs
  `uv sync --no-dev` at start, creating its own venv in a named volume
  (`UV_PROJECT_ENVIRONMENT=/venv`) — your host `.venv` is never touched.
- They reach the infra via env overrides: `KAFKA_BOOTSTRAP_SERVERS` and
  `ES_HOSTS` (comma-separated) take precedence over the YAML values.
  Compose sets them to `kafka:9092` / `http://elasticsearch:9200`.
- Kibana at http://localhost:5601 — browse the `syslog-1m` index via
  Dev Tools: `GET syslog-1m/_search`
- Producer stream volume is variable: `RATE=500 DURATION=60 docker compose up -d syslog_producer`
  (RATE = docs/s, 0 = unthrottled; DURATION = seconds, 0 = until stopped)
- Kibana is memory-hungry; remove the `kibana` service if you need the RAM.
- `docker compose down` stops everything.

## Tests

```bash
uv run pytest        # kafka and ES are faked, no infra needed
```

## Code style

All tools are in the `dev` dependency group and configured under the
`[tool.*]` tables in `pyproject.toml` (`line-length = 100`); flake8 reads
them via the `flake8-pyproject` plugin.

```bash
uv run isort agg_pipeline mock_producer tests data_generator.py
uv run black agg_pipeline mock_producer tests data_generator.py
uv run mypy agg_pipeline mock_producer tests data_generator.py
uv run flake8 agg_pipeline mock_producer tests data_generator.py
```

## Notes / simplifications of this mock-up

- In-memory state: counts live in the process. A real implementation would
  checkpoint window state (e.g. per partition) to survive restarts.
- Watermarking is `now()`: windows flush once the wall clock passes their end.
  A late event for an already-flushed window writes its own small document
  that upserts (same doc id) instead of being merged; for `value_type: share`
  its denominator is the events seen at that flush, not the full window total.
- `flush_all()` on SIGINT/SIGTERM writes in-flight windows so nothing visible
  is lost on shutdown.
- Every distinct (entity, metric, metric_value) combination that occurs gets
  its own document; there is no per-value filtering.
- For `value_type: share`, each metric's denominator is all values of *that*
  metric for the entity, so a given entity's shares within one metric sum to 1.
- One count per event per metric; extending to sums/averages of a numeric
  field is a small change in `WindowAggregator` (state already holds the buckets).
