# FraudStream V3 — Production Hardening Guide

> **Purpose**: Study & interview prep guide documenting every step taken to upgrade FraudStream from a local-first POC to a production-grade real-time fraud detection pipeline on Databricks.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture Evolution: Local → Cloud](#2-architecture-evolution-local--cloud)
3. [Step 1: Continuous Streaming Pipeline](#step-1-continuous-streaming-pipeline)
4. [Step 2: Databricks Job Creation](#step-2-databricks-job-creation)
5. [Step 3: Late Arriving Data Handling](#step-3-late-arriving-data-handling)
6. [Step 4: Performance Optimization](#step-4-performance-optimization)
7. [Step 5: Email Failure Alerts](#step-5-email-failure-alerts)
8. [Step 6: Structured Logging](#step-6-structured-logging)
9. [Step 7: Error Handling & Safe Wrappers](#step-7-error-handling--safe-wrappers)
10. [Step 8: Business Anomaly Alerting](#step-8-business-anomaly-alerting)
11. [Step 9: Git Version Control](#step-9-git-version-control)
12. [Step 10: CI/CD Pipeline](#step-10-cicd-pipeline)
13. [Interview Q&A](#interview-qa)
14. [Architecture Diagram](#architecture-diagram)

---

## 1. Project Overview

### What is FraudStream?

FraudStream is a fraud detection pipeline that processes transaction, login, device, and chargeback events in real-time to identify suspicious activity.

### Evolution Timeline

| Version | Mode | Processing | Infrastructure |
|---------|------|-----------|----------------|
| V1 (Local) | Batch | Python + Kafka + Pandas | Docker, local files |
| V2 (Databricks) | Batch | Spark + Delta Lake | Databricks Serverless |
| **V3 (Current)** | **Continuous Streaming** | **Structured Streaming + Auto Loader** | **Databricks Serverless + Git + CI/CD** |

### V3 Pipeline Stages

```
01_stream_generator → 02_bronze_stream → 03_silver_stream → 04_scoring_stream → 05_gold_stream
      (generates)         (ingests)         (dedup+normalize)    (fraud rules)       (analytics)
                                                                                          ↓
                                                                              06_validation_stream
                                                                                   (validates)
```

### Key Data Paths

- **UC Volume Root**: `/Volumes/masoom_central_india_dev/default/fraudstream_v3_data/`
- **Layers**: `stream/ → bronze/ → silver/ → scored/ → alerts/ → gold/ → quarantine/ → checkpoints/`
- **4 Event Streams**: `fraud_transactions`, `fraud_logins`, `fraud_devices`, `fraud_chargebacks`

---

## 2. Architecture Evolution: Local → Cloud

### Original Architecture (V1 — Local)

From the original `architecture.md`:
- Local-first using Docker Kafka and local files under `data/`
- Pandas-based processing with fallback mode
- Plotly Dash dashboard for local monitoring
- Design principles: preserve raw events, separate layers, quarantine bad records, explainable fraud rules

### Cloud Architecture (V3 — Databricks)

| Component | V1 (Local) | V3 (Databricks) |
|-----------|------------|-----------------|
| Event source | Docker Kafka | Synthetic generator → UC Volume JSON |
| Ingestion | Python consumer | **Auto Loader** (`cloudFiles` format) |
| Storage | Local Parquet | **Delta Lake** on UC Volumes |
| Processing | Pandas DataFrame | **Spark Structured Streaming** |
| Orchestration | Shell scripts | **Databricks Continuous Job** |
| Version control | Git (local) | **GitHub + Databricks Git Folders** |
| CI/CD | None | **GitHub Actions + DABs** |
| Monitoring | Dashboard | **Email alerts + SQL anomaly alerts + structured logging** |

### Platform Layers (V3)

1. **Event simulation**: `01_stream_generator` — generates 200 events per 5 seconds, infinitely
2. **Bronze**: `02_bronze_stream` — Auto Loader reads JSON, applies 8 DQ checks, splits valid → Bronze Delta, invalid → quarantine
3. **Silver**: `03_silver_stream` — reads Bronze as stream, normalizes types, deduplicates via watermark + `dropDuplicates`, writes Silver Delta
4. **Fraud scoring**: `04_scoring_stream` — enriches with velocity features (2-day window), applies 7 weighted fraud rules, computes `risk_score` (0-100) and `risk_band` (LOW/MEDIUM/HIGH)
5. **Gold**: `05_gold_stream` — recomputes 7 gold analytics tables every 60s (country/merchant summaries, customer/merchant/device profiles, chargeback summary, high-risk transactions)
6. **Validation**: `06_validation_stream` — one-shot batch validation (counts, duplicates, DQ pass rate, risk bands, gold completeness)

---

## Step 1: Continuous Streaming Pipeline

### What We Did

Changed the pipeline from **sequential batch mode** to **continuous streaming mode**:
- Changed all streaming notebook triggers from `availableNow=True` (one-shot) to `processingTime='10s'` (continuous micro-batches)
- Set `GENERATOR_DURATION_S = 0` (infinite event generation, was 30 seconds)
- Removed all task dependencies in the Databricks job — all 5 tasks run **concurrently**

### Before vs After

| Aspect | Before (Batch) | After (Continuous) |
|--------|---------------|-------------------|
| Execution | Sequential: 01→02→03→04→05→06 | Concurrent: all 5 tasks simultaneously |
| Trigger | `availableNow=True` (process all, then stop) | `processingTime='10s'` (micro-batch every 10s) |
| Generator | 30 seconds, then stops | Runs forever (`duration=0`) |
| Data latency | Minutes (wait for each stage) | ~10 seconds (real-time) |

### Advantage

- **Real-time fraud detection**: Transactions are scored within ~10 seconds of generation
- **No idle time**: All stages process data as it arrives, no waiting for upstream
- **True streaming architecture**: Matches production fraud detection requirements

### Interview Answer

> "I upgraded the pipeline from batch to continuous streaming by changing Structured Streaming triggers from `availableNow=True` to `processingTime='10s'`. This means each stage processes data in 10-second micro-batches continuously. All 5 stages run concurrently — the generator produces events while Bronze ingests, Silver normalizes, Scoring applies fraud rules, and Gold refreshes analytics tables, all at the same time. This reduced end-to-end latency from minutes to ~10 seconds."

---

## Step 2: Databricks Job Creation

### What We Did

Created a Databricks Job (ID: 50077675888111) with:
- **Continuous mode** — job runs indefinitely, auto-restarts on completion
- **`task_retry_mode: ON_FAILURE`** — failed tasks automatically retry
- **5 concurrent tasks** — no `depends_on` between tasks
- **Serverless compute** — `queue: {enabled: true}`
- **Validation notebook excluded** — runs manually, not in continuous mode

### Job Configuration

```json
{
  "continuous": {
    "pause_status": "UNPAUSED",
    "task_retry_mode": "ON_FAILURE"
  },
  "queue": {"enabled": true},
  "max_concurrent_runs": 1,
  "tasks": [
    {"task_key": "01_stream_generator", ...},
    {"task_key": "02_bronze_stream", ...},
    {"task_key": "03_silver_stream", ...},
    {"task_key": "04_scoring_stream", ...},
    {"task_key": "05_gold_stream", ...}
  ]
}
```

### Advantage

- **Auto-recovery**: Task crashes → automatic retry without manual intervention
- **No scheduling needed**: Continuous mode runs 24/7 automatically
- **Serverless**: No cluster management — Databricks handles compute scaling

### Interview Answer

> "I created a Databricks continuous job with `task_retry_mode: ON_FAILURE`. This means if any streaming task crashes (e.g., OOM, network error), it automatically retries. The job runs indefinitely in continuous mode — no need for cron schedules. All 5 tasks run concurrently on Serverless compute, which auto-scales and requires no cluster management."

---

## Step 3: Late Arriving Data Handling

### What We Did

Increased the watermark from `60 seconds` to `10 minutes` in `00_config_stream`:

```python
# Before
WATERMARK_DELAY = "60 seconds"

# After
WATERMARK_DELAY = "10 minutes"
```

### How Watermarks Work in Structured Streaming

In `03_silver_stream`, the Silver layer uses `dropDuplicates(["event_id"])` with a watermark:

```python
silver_df = (bronze_df
    .withColumn("event_ts", col("event_timestamp").cast("timestamp"))
    .withWatermark("event_ts", WATERMARK_DELAY)  # 10 minutes
    .dropDuplicates(["event_id"])  # dedup with watermark
)
```

The watermark tells Spark how long to keep state for deduplication. Events arriving within the watermark window are accepted; events older than the watermark are dropped (state cleanup).

### Before vs After

| Setting | Before | After |
|---------|--------|-------|
| Watermark | 60 seconds | 10 minutes |
| Late data tolerance | 1 minute | 10 minutes |
| State size | Small | Larger (more memory) |
| Data loss risk | Higher (tight window) | Lower (generous window) |

### Advantage

- **Handles network delays**: Events delayed by up to 10 minutes are still processed
- **Handles out-of-order events**: Events arriving in wrong order are deduplicated correctly
- **Trade-off accepted**: More memory for state, but much safer for real-world scenarios

### Interview Answer

> "I increased the Structured Streaming watermark from 60 seconds to 10 minutes. The watermark controls how long Spark keeps deduplication state. With 60 seconds, any event arriving more than 1 minute late would be dropped — risky in production where network delays or out-of-order delivery happens. With 10 minutes, we tolerate much more latency while keeping state slightly larger. The trade-off is memory vs. data completeness, and I chose completeness for a fraud pipeline where missing a transaction could mean missing a fraud."

---

## Step 4: Performance Optimization

### Problem 1: Scoring — Full Table Scan Every 10 Seconds

### What We Did

The `04_scoring_stream` notebook's `process_scoring_batch` function reads ALL historical Silver data every micro-batch to compute velocity features. Changed to a 2-day window:

```python
# Before — reads ENTIRE Silver tables every 10s
# tx_all = spark.read.format("delta").load(f"{SILVER_ROOT}/fraud_transactions")

# After — only last 2 days for velocity features
from pyspark.sql.functions import date_sub, current_date
lookback = date_sub(current_date(), 1)
tx_all = spark.read.format("delta").load(f"{SILVER_ROOT}/fraud_transactions")
    .where(col("event_date") >= lookback)
```

### Advantage

As data grows, scanning all history every 10 seconds becomes exponentially slower. With a 2-day window, the scan is bounded regardless of total data size.

### Problem 2: Gold — 7 Full Table Overwrites Every 10 Seconds

### What We Did

Changed Gold trigger from `processingTime='10s'` to `processingTime='60s'`:

```python
# Before — 7 full table overwrites every 10s = 42 overwrites/minute
.trigger(processingTime='10s')

# After — 7 full table overwrites every 60s = 7 overwrites/minute
.trigger(processingTime='60s')
```

### Advantage

6x reduction in Gold compute overhead. Gold tables are analytics — 60-second freshness is more than sufficient.

### Problem 3: Bronze — 5 Redundant Spark Jobs Per Batch

### What We Did

The `process_batch` function in `02_bronze_stream` was triggering 5 separate Spark jobs per batch per stream (20 total across 4 streams every 10s):

```python
# Before — 5 Spark jobs per batch:
# 1. df.limit(1).count()  — existence check
# 2. valid.limit(1).count()  — existence check
# 3. valid.count()  — for print statement
# 4. invalid.limit(1).count()  — existence check
# 5. invalid.count()  — for print statement

# After — 1 Spark job per batch:
counts = df.groupBy("dq_passed").count().collect()
valid_count = next((r["count"] for r in counts if r["dq_passed"]), 0)
invalid_count = next((r["count"] for r in counts if not r["dq_passed"]), 0)
```

### Advantage

~80% reduction in Spark job overhead for Bronze (from 20 jobs to 4 jobs per 10s cycle across 4 streams).

### Interview Answer

> "I optimized three bottlenecks: First, the scoring notebook was scanning the entire Silver table every 10 seconds for velocity features — I added a 2-day window filter using `date_sub(current_date(), 1)`. Second, the Gold notebook was doing 7 full table overwrites every 10 seconds — I changed the trigger to 60 seconds since analytics tables don't need real-time freshness. Third, the Bronze notebook was triggering 5 separate Spark jobs per batch for existence checks and print counts — I replaced them with a single `groupBy().count()` that gets both valid and invalid counts in one job. Overall, these changes reduced compute usage significantly while maintaining correctness."

---

## Step 5: Email Failure Alerts

### What We Did

Added email notifications at both job level and task level:

```json
{
  "email_notifications": {
    "on_failure": ["masoomgupta0715@gmail.com"],
    "on_start": false
  },
  "tasks": [
    {
      "task_key": "01_stream_generator",
      "email_notifications": {
        "on_failure": ["masoomgupta0715@gmail.com"]
      }
    }
    // ... same for all 5 tasks
  ]
}
```

### Advantage

- **Immediate notification**: Email fires when any task fails or retries are exhausted
- `on_start: false` — no spam from continuous mode restarts
- **Two levels**: Task-level (immediate task failure) + Job-level (retries exhausted)

### Interview Answer

> "I configured email notifications at both the job and individual task levels. When any task fails, an email is sent immediately. When the job's auto-retry is exhausted, a job-level email fires. I set `on_start: false` to avoid notification spam from continuous mode restarts. This ensures the team is always aware of production issues without manually monitoring dashboards."

---

## Step 6: Structured Logging

### What We Did

Added Python's `logging` module to `00_config_stream` (shared config), which all notebooks inherit via `%run`:

```python
# In 00_config_stream (cell 6):
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("FraudStreamV3")
```

Then replaced `print()` with `logger` calls in all 6 streaming notebooks:

```python
# Before
print(f"  [Bronze:{stream_name}] batch {batch_id}: {valid_count} valid → Bronze")

# After
logger.info(f"[Bronze:{stream_name}] batch {batch_id}: {valid_count} valid → Bronze")
logger.warning(f"[Bronze:{stream_name}] batch {batch_id}: {invalid_count} invalid → Quarantine")
logger.error(f"[Silver:{stream_name}] batch {batch_id}: error - {e}")
```

### Log Levels Used

| Level | Usage | Example |
|------|-------|--------|
| `INFO` | Normal batch processing | `[Bronze:fraud_transactions] batch 5: 200 valid → Bronze` |
| `WARNING` | DQ failures, skipped operations | `[Gold] chargeback_summary skipped: Path does not exist` |
| `ERROR` | Batch failures, critical errors | `[Scoring] batch 10 FAILED: Schema mismatch` |

### Advantage

- **Timestamps in every log**: `2026-10-11 12:30:45 [INFO] FraudStreamV3: ...`
- **Log levels**: Filter by severity (INFO/WARNING/ERROR) — can't do with `print()`
- **Persistent**: Logs go to cluster logs, not just notebook stdout
- **Forwardable**: Can be sent to monitoring systems (Splunk, Datadog, etc.)

### Interview Answer

> "I replaced bare `print()` statements with Python's `logging` module across all 6 streaming notebooks. The logger is defined in the shared config notebook (`00_config_stream`) and inherited by all notebooks via `%run`. I use three log levels: `INFO` for normal batch processing, `WARNING` for DQ failures and skipped operations, and `ERROR` for batch failures. This gives us timestamps, severity filtering, and the ability to forward logs to external monitoring systems — none of which is possible with `print()`."

---

## Step 7: Error Handling & Safe Wrappers

### What We Did

### 7a. Created `safe_foreachbatch` decorator

Added a reusable error-handling wrapper to `00_config_stream`:

```python
def safe_foreachbatch(fn):
    """Wraps foreachBatch functions with structured error logging."""
    def wrapper(df, batch_id):
        try:
            fn(df, batch_id)
        except Exception as e:
            logger.error(f"Batch {batch_id} FAILED in {fn.__name__}: {e}")
            raise
    return wrapper
```

Applied to all 4 foreachBatch functions:
```python
# In 02_bronze_stream
@safe_foreachbatch
def process_batch(df, batch_id): ...

# In 03_silver_stream
@safe_foreachbatch
def write_silver_batch(df, batch_id): ...

# In 04_scoring_stream
@safe_foreachbatch
def process_scoring_batch(batch_df, batch_id): ...

# In 05_gold_stream
@safe_foreachbatch
def process_gold_batch(batch_df, batch_id): ...
```

### 7b. Fixed dangerous broad `except` in Scoring

The scoring notebook had two `except Exception:` blocks that would **overwrite all existing data** on ANY error:

```python
# Before — DANGEROUS: any error → overwrite entire table → data loss!
except Exception:
    scored.write.format("delta").mode("overwrite").save(scored_path)

# After — SAFE: only overwrite if table doesn't exist, else log and re-raise
except Exception as e:
    if "Path does not exist" in str(e):
        scored.write.format("delta").mode("overwrite").save(scored_path)
    else:
        logger.error(f"[Scoring] batch {batch_id}: ERROR - {e}")
        raise  # trigger task retry
```

### 7c. Fixed silent error swallowing in Gold

```python
# Before — silent failure, nobody knows
except Exception:
    pass

# After — logs the warning
except Exception as e:
    logger.warning(f"[Gold] chargeback_summary skipped: {e}")
```

### Advantage

| Issue | Before | After |
|-------|--------|-------|
| Unhandled exceptions | Batch silently lost | Caught, logged, re-raised for retry |
| Scoring overwrite on error | **Destroys all data** | Only on first run, else retry |
| Gold chargeback errors | Silent (nobody knows) | Logged as warning |

### Error Flow

```
foreachBatch throws exception
       ↓
@safe_foreachbatch catches it
       ↓
logger.error("Batch N FAILED in process_scoring_batch: ...")
       ↓
Re-raises exception
       ↓
Structured Streaming retries the batch (checkpoint-based)
       ↓
If retries exhausted → task fails → job retry (ON_FAILURE)
       ↓
Email alert sent to masoomgupta0715@gmail.com
```

### Interview Answer

> "I created a `safe_foreachbatch` decorator in the shared config that wraps all streaming batch functions with try/except. It logs the error with the function name, batch ID, and full exception info, then re-raises so Structured Streaming can retry the batch. I also fixed a critical bug in the scoring notebook where a broad `except Exception` would fall back to `mode('overwrite')` on ANY error — not just 'table doesn't exist' — which could destroy all scored data. Now it only overwrites on first run and re-raises all other errors. Finally, I fixed a silent `except: pass` in the Gold notebook's chargeback summary that was hiding errors."

---

## Step 8: Business Anomaly Alerting

### What We Did

Created a Databricks SQL Alert (Alert V2) that monitors for spikes in HIGH risk transactions:

```sql
SELECT COUNT(*) AS high_risk_count
FROM delta.`/Volumes/masoom_central_india_dev/default/fraudstream_v3_data/scored/scored_transactions`
WHERE risk_band = 'HIGH'
  AND scored_at >= dateadd(hour, -1, current_timestamp())
```

**Alert configuration**:
- **Column**: `high_risk_count`
- **Operator**: `GREATER_THAN`
- **Threshold**: 50
- **Schedule**: Every 15 minutes (`0 0/15 * * * ?` UTC)

### How It Works

Every 15 minutes, Databricks runs the SQL query against the scored_transactions Delta table. If the count of HIGH risk transactions in the last hour exceeds 50, the alert fires and notifies the configured email/Slack/Teams destination.

### Why This Matters

This is a **business-level alert**, not an infrastructure alert. It doesn't fire when a task crashes — it fires when the business sees something unusual:
- Sudden fraud spike (coordinated attack)
- Rule misconfiguration (too many transactions classified HIGH)
- Data quality issue (bad data inflating risk scores)

### Interview Answer

> "I created a Databricks SQL Alert that fires when HIGH risk transactions exceed 50 in the last hour, evaluated every 15 minutes. This is a business anomaly alert — it catches fraud spikes, rule misconfigurations, or data quality issues that inflate risk scores. Unlike infrastructure alerts (task failure emails), this monitors the actual business metric. The query reads from the scored_transactions Delta table using `dateadd(hour, -1, current_timestamp())` for a rolling 1-hour window."

---

## Step 9: Git Version Control

### What We Did

1. **Added Git credential**: Connected GitHub account to Databricks via Linked Accounts
2. **Cloned repo**: Created a Databricks Git folder linked to `https://github.com/guptamasoom/fraudstream-v3.git`
3. **Copied all 7 notebooks**: Used Databricks SDK `workspace.export` + `workspace.import_` to copy notebooks from the workspace folder to the Git folder
4. **Created CI/CD files**: Added `databricks.yml` (DABs config) and `.github/workflows/deploy.yml` (GitHub Actions)
5. **Committed and pushed**: All 9 files pushed to GitHub

### Files in Git Repo

```
fraudstream-v3/
├── .github/
│   └── workflows/
│       └── deploy.yml          # CI/CD pipeline
├── 00_config_stream.py         # Shared config + logging + safe_foreachbatch
├── 01_stream_generator.py      # Infinite event generator
├── 02_bronze_stream.py         # Auto Loader → DQ → Bronze Delta
├── 03_silver_stream.py         # Bronze → dedup → Silver Delta
├── 04_scoring_stream.py        # Silver → fraud rules → Scored + Alerts
├── 05_gold_stream.py           # Scored → 7 gold analytics tables
├── 06_validation_stream.py     # One-shot validation
└── databricks.yml              # DABs bundle config
```

### Advantage

- **Version history**: Every change is tracked — who, what, when
- **Rollback**: Bad change? Revert to previous commit
- **Code review**: Team members can review PRs before merging
- **Branching**: Experiment on a branch without breaking production
- **Audit trail**: Required for compliance in financial services

### Interview Answer

> "I set up Git version control by connecting a GitHub credential to Databricks, cloning the repo as a Databricks Git folder, and copying all 7 notebooks plus CI/CD configuration files. I used the Databricks SDK's `workspace.export` and `workspace.import_` methods to programmatically copy the notebooks from a workspace folder to the Git folder. Everything is now version-controlled on GitHub, enabling code review, branching, rollback, and audit trails — essential for a fraud detection pipeline in financial services."

---

## Step 10: CI/CD Pipeline

### What We Did

Created two files for automated deployment:

### `databricks.yml` — Declarative Automation Bundle (DAB)

```yaml
bundle:
  name: fraudstream-v3

targets:
  dev:
    mode: development
    default: true
  prod:
    mode: production

resources:
  jobs:
    fraudstream_v3_pipeline:
      name: FraudStream V3 Pipeline
      continuous:
        pause_status: PAUSED
        task_retry_mode: ON_FAILURE
      tasks:
        - task_key: 01_stream_generator
          notebook_task:
            notebook_path: /Workspace/.../01_stream_generator
            source: WORKSPACE
        # ... 4 more tasks
```

### `.github/workflows/deploy.yml` — GitHub Actions

```yaml
name: Deploy FraudStream V3 Pipeline
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  validate-and-deploy:
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - name: Validate bundle (PR)
        run: databricks bundle validate --target dev
      - name: Deploy bundle (main push)
        run: databricks bundle deploy --target prod
```

### How CI/CD Works

```
Developer makes change → Push to feature branch → Create PR
                                                ↓
                                    GitHub Actions validates bundle
                                                ↓
                                    ✅ Valid → Merge to main
                                                ↓
                                    GitHub Actions deploys to prod
                                                ↓
                                    Databricks job updated automatically
```

### Advantage

- **Automated**: No manual job updates — push = deploy
- **Safe**: PR validation catches errors before merge
- **Dev/Prod separation**: Test in `dev` target, deploy to `prod` target
- **Reproducible**: Same config deployed every time

### Prerequisites

Add these secrets to the GitHub repository:
- `DATABRICKS_HOST`: Your workspace URL
- `DATABRICKS_TOKEN`: A Databricks PAT

### Interview Answer

> "I set up CI/CD using Databricks Declarative Automation Bundles (DABs) and GitHub Actions. The `databricks.yml` file defines the job with dev and prod targets. The GitHub Actions workflow validates the bundle on pull requests and deploys to production on merge to main. This means a developer makes a change, creates a PR, GitHub Actions validates the bundle, and on merge, automatically deploys the updated job to Databricks — no manual steps needed."

---

## Interview Q&A

### Q1: What's the difference between `availableNow=True` and `processingTime='10s'`?

**A**: `availableNow=True` processes all available data in one micro-batch and then terminates the query — it's semi-batch. `processingTime='10s'` runs the query continuously, processing new data every 10 seconds indefinitely. For a fraud detection pipeline, continuous mode is essential because new events arrive constantly.

### Q2: How does checkpointing ensure no data loss on failure?

**A**: Each streaming query has a `checkpointLocation` that stores: (1) the read offset (what data has been consumed from the source), (2) the write state (what batches have been committed to the sink), and (3) the deduplication state (for `dropDuplicates`). On restart, the query reads the checkpoint and resumes from exactly where it left off — no reprocessing, no data loss.

### Q3: Why use `foreachBatch` instead of direct streaming writes?

**A**: `foreachBatch` allows us to perform complex operations within each micro-batch that aren't possible with direct streaming writes: (1) JOIN with historical data for velocity features, (2) MERGE into Delta tables (upsert), (3) Conditional writes (e.g., only write HIGH risk to alerts table), (4) Multiple writes from the same batch (scored table + alerts table). The trade-off is that foreachBatch has at-least-once semantics, but checkpointing + MERGE (idempotent) handles duplicates.

### Q4: What is a watermark and why does it matter?

**A**: A watermark is a threshold that tells Spark how long to keep state for stateful operations like `dropDuplicates`. Events arriving within the watermark window are processed normally. Events older than the watermark are dropped, and their state is cleaned up to prevent unbounded memory growth. I set it to 10 minutes — meaning we accept events up to 10 minutes late, but clean up state for anything older. This balances data completeness (catching late events) with memory usage (bounded state).

### Q5: How did you optimize the scoring notebook's velocity feature computation?

**A**: The original code read the ENTIRE Silver transactions table every 10 seconds to compute velocity features (customer transaction count, merchant count, device customer count, etc.). I added a `date_sub(current_date(), 1)` filter to only read the last 2 days. This is sufficient for velocity features (recent activity matters more than historical) and keeps the scan bounded as data grows.

### Q6: What was the dangerous `except Exception` bug in scoring?

**A**: The scoring notebook had two try/except blocks that caught ALL exceptions and fell back to `mode('overwrite')` — which replaces the entire Delta table. If the MERGE failed for any reason other than 'table doesn't exist' (e.g., schema mismatch, OOM, permission error), it would overwrite ALL existing scored data with just the current batch — destroying history. I fixed it to only overwrite when the error message contains 'Path does not exist' (first run), and re-raise all other errors to trigger retry.

### Q7: How does the CI/CD pipeline work?

**A**: I use Databricks Declarative Automation Bundles (DABs) with GitHub Actions. The `databricks.yml` file defines the job configuration with `dev` and `prod` targets. When a PR is created, GitHub Actions runs `databricks bundle validate --target dev` to catch configuration errors. When the PR is merged to main, it runs `databricks bundle deploy --target prod` which updates the Databricks job automatically. GitHub secrets (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`) authenticate the CLI.

### Q8: What's the difference between job-level and task-level email alerts?

**A**: Task-level alerts fire immediately when a specific task fails. Job-level alerts fire when the job itself fails — which in continuous mode with `task_retry_mode: ON_FAILURE`, means after all retries are exhausted. I configured both: task-level for fast detection of individual failures, job-level for escalation when retries don't fix the problem.

### Q9: Why is the anomaly alert separate from the job failure alerts?

**A**: Job failure alerts are **infrastructure** alerts — they fire when something breaks (task crash, retry exhaustion). The anomaly alert is a **business** alert — it fires when the business metric (HIGH risk transaction count) exceeds a threshold, even if the infrastructure is healthy. For example, a sudden fraud attack would spike HIGH risk transactions without any infrastructure failure. Both are needed for complete monitoring.

### Q10: How would you scale this pipeline for production with real Kafka?

**A**: The architecture is already designed for it. I would: (1) Replace the synthetic generator with Kafka topics (MSK or Confluent Cloud), (2) Replace Auto Loader's `cloudFiles` source with Kafka source (`kafka` format), (3) Keep the same Bronze→Silver→Scoring→Gold layer architecture, (4) Add schema registry for event validation, (5) Replace rule-based scoring with a trained ML model using chargebacks as delayed labels, (6) Add data retention policies for Delta tables, (7) Send alerts to Slack/PagerDuty instead of just email.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    Databricks Serverless                     │
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    │
│  │ 01_stream_   │    │ 02_bronze_   │    │ 03_silver_   │    │
│  │ generator    │───▶│ stream       │───▶│ stream       │    │
│  │              │    │ (Auto Loader)│    │ (dedup+wmark)│    │
│  │ 200 ev/5s    │    │ DQ checks    │    │ 10min wmark  │    │
│  │ infinite     │    │ quarantine   │    │              │    │
│  └──────────────┘    └──────────────┘    └──────┬───────┘    │
│                                                  │            │
│                                                  ▼            │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    │
│  │ 05_gold_     │    │ 04_scoring_  │    │ Silver Delta │    │
│  │ stream       │◀───│ stream       │◀───│ Tables       │    │
│  │ 7 gold tables │    │ 7 fraud rules│    └──────────────┘    │
│  │ 60s trigger   │    │ velocity feat│                        │
│  │ safe wrapper  │    │ safe wrapper │                        │
│  └──────────────┘    └──────────────┘                        │
│         │                    │                               │
│         ▼                    ▼                               │
│  ┌──────────────┐    ┌──────────────┐                        │
│  │ Gold Delta   │    │ Scored Delta │                        │
│  │ Tables (7)   │    │ + Alerts     │                        │
│  └──────────────┘    └──────────────┘                        │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│  Monitoring & Alerting                                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐     │
│  │ Email    │  │ SQL Alert│  │ Structured│  │ Checkpt  │     │
│  │ (fail)   │  │ (anomaly)│  │ Logging  │  │ Recovery │     │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘     │
├──────────────────────────────────────────────────────────────┤
│  Git & CI/CD                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────────────┐   │
│  │ GitHub   │  │ DABs     │  │ GitHub Actions            │   │
│  │ Repo     │  │ Config   │  │ (validate on PR,          │   │
│  │ (9 files)│  │ (YAML)   │  │  deploy on main push)     │   │
│  └──────────┘  └──────────┘  └──────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

---

## Fraud Rules Reference

| Rule | Weight | Description |
|------|--------|-------------|
| `rule_high_amount` | 20 | Amount ≥ $600 |
| `rule_country_mismatch` | 20 | IP/billing/shipping country differ |
| `rule_new_device_high_amount` | 20 | New device + high amount |
| `rule_high_velocity` | 15 | Customer has ≥ 4 transactions |
| `rule_many_customers_on_device` | 10 | Device used by ≥ 3 customers |
| `rule_failed_login_before_purchase` | 10 | ≥ 2 failed logins before purchase |
| `rule_card_not_present_risk` | 15 | Card-not-present + country mismatch |

| Risk Band | Score Range |
|-----------|------------|
| LOW | 0-39 |
| MEDIUM | 40-69 |
| HIGH | 70-100 |

---

## DQ Checks Reference

| Check | Reason Code | Severity | Applies To |
|-------|------------|----------|------------|
| event_id present | MISSING_EVENT_ID | critical | All streams |
| event_timestamp present | MISSING_EVENT_TIMESTAMP | critical | All streams |
| customer_id present | MISSING_CUSTOMER_ID | critical | All streams |
| transaction_id present | MISSING_TRANSACTION_ID | critical | Transactions |
| amount non-negative | NEGATIVE_AMOUNT | critical | Transactions |
| valid IP country | INVALID_IP_COUNTRY | warning | Transactions |
| valid billing country | INVALID_BILLING_COUNTRY | warning | Transactions |
| chargeback_amount non-negative | NEGATIVE_CHARGEBACK | critical | Chargebacks |

---

## Key Config Values (00_config_stream)

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `WATERMARK_DELAY` | `"10 minutes"` | Late data tolerance for Silver dedup |
| `GENERATOR_DURATION_S` | `0` | Infinite event generation |
| `TRIGGER_INTERVAL_SECONDS` | `10` | Micro-batch interval |
| `BATCH_SIZE` | `200` | Events per generator batch |
| `BATCH_INTERVAL_S` | `5` | Seconds between generator batches |
| `FRAUD_RATE` | `0.08` | 8% of transactions are fraudulent |
| `BAD_DATA_RATE` | `0.04` | 4% of events have injected anomalies |
| `HIGH_AMOUNT_THRESHOLD` | `600.0` | Rule: high amount threshold |
| `HIGH_VELOCITY_THRESHOLD` | `4` | Rule: customer transaction count |
| `MANY_CUSTOMERS_THRESHOLD` | `3` | Rule: customers per device |
| `FAILED_LOGIN_THRESHOLD` | `2` | Rule: failed logins before purchase |

---

## Databricks Job Configuration

| Setting | Value |
|---------|-------|
| Job ID | 50077675888111 |
| Name | FraudStream V3 Pipeline |
| Mode | Continuous (UNPAUSED/PAUSED) |
| Task retry | ON_FAILURE |
| Compute | Serverless (queue enabled) |
| Max concurrent runs | 1 |
| Email alerts | masoomgupta0715@gmail.com |
| Tasks | 5 concurrent (01-05) |

---

## Summary: What Made This Production-Grade

| Capability | Status | How |
|------------|--------|-----|
| Real-time processing | ✅ | `processingTime='10s'` continuous triggers |
| Fault tolerance | ✅ | Checkpoints + auto-retry + safe_foreachbatch |
| Late data handling | ✅ | 10-minute watermark |
| Performance | ✅ | Windowed reads, optimized triggers, single groupBy |
| Infrastructure alerts | ✅ | Email on task/job failure |
| Business alerts | ✅ | SQL anomaly alert (HIGH risk > 50/hour) |
| Structured logging | ✅ | Python logging with levels in all notebooks |
| Error handling | ✅ | safe_foreachbatch, fixed broad excepts |
| Version control | ✅ | GitHub repo with all 9 files |
| CI/CD | ✅ | DABs + GitHub Actions (validate on PR, deploy on main) |
| Validation | ✅ | 06_validation_stream (counts, DQ, risk bands, gold) |

---

*Document generated: October 11, 2026*
*Pipeline: FraudStream V3 — Real-Time Fraud Detection*
*GitHub: https://github.com/guptamasoom/fraudstream-v3*