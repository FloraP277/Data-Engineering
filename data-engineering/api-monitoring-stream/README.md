# Real-Time API Monitoring Pipeline

A streaming pipeline that watches live request logs from a fleet of microservices and flags latency spikes, error bursts and regional outages within about a minute.

**Stack:** Python · Apache Kafka (Redpanda) · Spark Structured Streaming · PostgreSQL · Grafana · Docker Compose · pytest · GitHub Actions

## Results

Simulated run: 20 minutes of traffic from 5 services in 3 regions at 10 events per second, with 1% duplicate deliveries, 1% late events and three injected incidents. Processed locally on a 2-core machine.

| Incident | Affected | Detected | Time to detect | Rules that fired |
| --- | --- | --- | --- | --- |
| Latency spike (6× slower) | payments | Yes | 60 s | p95 above limit, p95 above baseline |
| Error burst (35% errors) | checkout | Yes | 60 s | error rate above limit and baseline |
| Regional outage (90% errors) | all services in eu-west | Yes | 60 s | error rate above limit and baseline |

- **Recall:** 3 of 3 incidents detected. **Precision:** 56 of 57 alerts belonged to a real incident (1 false alarm).
- **Throughput:** about 5,000–6,000 events per second processed in batch catch-up on 2 cores.
- **Exactly-once counts:** duplicate deliveries were dropped (window totals equal the number of unique events), and re-running from the checkpoint reads nothing twice.

> **Your run:** _run with Kafka and Grafana at a higher rate and replace these numbers; add a GIF of the dashboard during an incident._

Time to detect is 60 seconds because alerts are evaluated per 1-minute window; the measurement is from the incident start to the end of the first alerting window.

## Architecture

```mermaid
flowchart LR
    S[Simulator<br/>5 services · 3 regions<br/>incidents, duplicates, late events] -->|JSON events| K[(Kafka topic<br/>api-requests)]
    K --> SS[Spark Structured Streaming<br/>parse + validate<br/>watermark 2 min<br/>drop duplicate event_id]
    SS --> W[1-minute windows per service<br/>requests · error rate<br/>avg + p95 latency]
    W --> A[Alert rules<br/>fixed limits + service baseline]
    W --> P[(Postgres<br/>service_windows)]
    A --> P2[(Postgres<br/>alerts)]
    P --> G[Grafana dashboard]
    P2 --> G
    S -.->|ground truth| I[incidents.json] --> E[Evaluation<br/>recall · precision · delay]
```

## Alert rules

| Rule | Fires when |
| --- | --- |
| `error_rate_above_limit` | More than 5% of requests in the window returned 5xx |
| `p95_above_limit` | p95 latency exceeds the service's limit (e.g. 900 ms for payments) |
| `p95_above_baseline` | p95 latency is more than 3× the service's median p95 over its last 15 windows |
| `error_rate_above_baseline` | Error rate is more than 3× the service's recent normal and above 2% |

Windows with fewer than 20 requests are skipped, so a single failed request on a quiet service doesn't page anyone.

## Run it

```bash
pip install -r requirements.txt          # needs Java 17+ for Spark
docker compose up -d                     # Redpanda (Kafka), Postgres, Grafana at http://localhost:3000

# Terminal 1: produce live traffic (incidents are scheduled across the 30 minutes)
PYTHONPATH=src python -m apimon.simulator kafka --rate 200 --minutes 30
# Terminal 2: the streaming job (downloads the Spark Kafka connector on first run)
PYTHONPATH=src python -m apimon.streaming --source kafka \
    --pg postgresql://postgres:postgres@localhost:5432/monitoring
# Afterwards: score the alerts against the incidents
PYTHONPATH=src python -m apimon.evaluate --out data/out --incidents incidents.json
```

Without Docker, the same job runs on files:

```bash
PYTHONPATH=src python -m apimon.simulator files --out data/events --minutes 20 --rate 10
PYTHONPATH=src python -m apimon.streaming --source files --path data/events --out data/out --once
PYTHONPATH=src python -m apimon.evaluate --out data/out --incidents data/incidents.json
```

**Tests:** `pytest`. The end-to-end test streams 20 simulated minutes through Spark and checks that every incident is caught, duplicates are dropped, and re-runs read nothing twice.

## Design decisions

- **Event time, not arrival time.** Windows use the time each request happened, so late events still land in the right minute. The 2-minute watermark bounds how long Spark waits and keeps state from growing forever.
- **Deduplicate by event ID within the watermark.** At-least-once delivery from Kafka means duplicates happen; counting them would inflate traffic and error rates.
- **Upserts keyed by service and window.** In update mode a window is emitted several times as data arrives. Writing it with `ON CONFLICT … DO UPDATE` keeps the latest totals without double counting.
- **Two kinds of rules.** Fixed limits encode service-level objectives; baselines catch "much worse than usual" for services whose normal is far below the limit.
- **Detection is scored against ground truth.** The simulator records exactly when and where each incident happened, so recall, precision and delay are measured, not guessed.

## Limitations

- Alerts can fire on a partially filled window early in its minute; a production version would wait for the watermark or require two consecutive bad windows.
- The baseline is held in the job's memory and resets on restart; a production version would store it in a state store or the database.
- Throughput was measured catching up on files on a 2-core machine, not on a cluster.
