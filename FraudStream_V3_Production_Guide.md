# FraudStream V3 — Production Hardening Guide

> **Purpose**: Study & interview prep guide documenting every step taken to upgrade FraudStream from a local-first POC to a production-grade real-time fraud detection pipeline on Databricks.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture Evolution: Local → Cloud](#2-architecture-evolution-local--cloud)
3. [Detailed Layer-by-Layer Architecture](#detailed-layer-by-layer-architecture)
4. [Step 1: Continuous Streaming Pipeline](#step-1-continuous-streaming-pipeline)
5. [Step 2: Databricks Job Creation](#step-2-databricks-job-creation)
6. [Step 3: Late Arriving Data Handling](#step-3-late-arriving-data-handling)
7. [Step 4: Performance Optimization](#step-4-performance-optimization)
8. [Step 5: Email Failure Alerts](#step-5-email-failure-alerts)
9. [Step 6: Structured Logging](#step-6-structured-logging)
10. [Step 7: Error Handling & Safe Wrappers](#step-7-error-handling--safe-wrappers)
11. [Step 8: Business Anomaly Alerting](#step-8-business-anomaly-alerting)
12. [Step 9: Git Version Control](#step-9-git-version-control)
13. [Step 10: CI/CD Pipeline](#step-10-cicd-pipeline)
14. [Interview Q&A](#interview-qa)
15. [Architecture Diagram](#architecture-diagram)

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

1. **Event simulation**: `01_stream_generator` — generates 500 events per 5 seconds, infinitely
2. **Bronze**: `02_bronze_stream` — Auto Loader reads JSON, applies 8 DQ checks, splits valid → Bronze Delta, invalid → quarantine
3. **Silver**: `03_silver_stream` — reads Bronze as stream, normalizes types, deduplicates via watermark + `dropDuplicates`, writes Silver Delta
4. **Fraud scoring**: `04_scoring_stream` — enriches with velocity features (2-day window), applies 7 weighted fraud rules, computes `risk_score` (0-100) and `risk_band` (LOW/MEDIUM/HIGH)
5. **Gold**: `05_gold_stream` — recomputes 7 gold analytics tables every 60s (country/merchant summaries, customer/merchant/device profiles, chargeback summary, high-risk transactions)
6. **Validation**: `06_validation_stream` — one-shot batch validation (counts, duplicates, DQ pass rate, risk bands, gold completeness)

---

## Detailed Layer-by-Layer Architecture

This section explains exactly what each notebook does, cell by cell, with the technical implementation details.

### Layer 0: 00_config_stream — Shared Configuration

**Purpose**: Single source of truth for all paths, schemas, DQ checks, fraud rules, and logging. Every other notebook starts with `%run ./00_config_stream` to inherit these definitions.

**Cells (7 total)**:

1. **Markdown Title**: Describes the V3 streaming architecture and key difference from V2 (real-time micro-batches vs scheduled batches).

2. **Imports & Data Paths**: 
   - Imports PySpark types (`BooleanType`, `DecimalType`, `DoubleType`, `StringType`, `StructField`, `StructType`, `TimestampType`)
   - Defines `DATA_ROOT` = UC Volume path
   - Defines 8 layer paths: `STREAM_ROOT`, `BRONZE_ROOT`, `SILVER_ROOT`, `SCORED_ROOT`, `ALERTS_ROOT`, `QUARANTINE_ROOT`, `GOLD_ROOT`, `DQ_ROOT`, `CHECKPOINT_ROOT`
   - Defines `STREAMS` = 4 event stream names
   - Sets up Python `logging` module with format `%(asctime)s [%(levelname)s] %(name)s: %(message)s`
   - Creates `logger = logging.getLogger("FraudStreamV3")`

3. **Event Schemas**: Defines `StructType` schemas for all 4 event streams:
   - `TX_SCHEMA`: 17 fields (event_id, event_type, event_timestamp, transaction_id, customer_id, merchant_id, merchant_category, amount, currency, payment_method, device_id, ip_country, billing_country, shipping_country, is_card_present, auth_result, is_known_fraud)
   - `LOGIN_SCHEMA`: 8 fields (event_id, event_type, event_timestamp, customer_id, device_id, ip_country, user_agent, mfa_passed)
   - `DEVICE_SCHEMA`: 7 fields (event_id, event_type, event_timestamp, customer_id, device_id, device_type, ip_country, is_new_device)
   - `CHARGEBACK_SCHEMA`: 8 fields (event_id, event_type, event_timestamp, transaction_id, customer_id, merchant_id, chargeback_amount, reason_code)
   - Compiled into `SCHEMAS` dict and `DQ_CHECKS_BY_STREAM` dict

4. **Generation Constants & Fraud Rules**:
   - `COUNTRIES` = ["IN", "US", "GB", "SG", "AE", "DE", "BR"]
   - `RISK_COUNTRIES` = ["RU", "NG", "BR", "AE"] — used for fraudulent transactions
   - `MERCHANT_CATEGORIES`, `PAYMENT_METHODS`, `DEVICE_TYPES`, `AUTH_RESULTS`, `LOGIN_TYPES`, `DEVICE_EVENT_TYPES`, `CHARGEBACK_REASONS`, `BAD_DATA_MODES`
   - `FRAUD_RATE` = 0.08 (8% of transactions are fraudulent)
   - `BAD_DATA_RATE` = 0.04 (4% of events have injected anomalies)
   - `RULE_WEIGHTS` dict: 7 fraud rules with weights (20+20+20+15+10+10+15 = 110 max, clipped at 100)
   - `HIGH_AMOUNT_THRESHOLD` = 600.0, `HIGH_VELOCITY_THRESHOLD` = 4, `MANY_CUSTOMERS_THRESHOLD` = 3, `FAILED_LOGIN_THRESHOLD` = 2

5. **DQ Check Definitions**:
   - `DQ_CHECKS_BASE`: 3 checks applied to ALL streams (event_id present, event_timestamp present, customer_id present)
   - `DQ_CHECKS_TX`: 7 checks for transactions (base + transaction_id present, amount non-negative, valid IP country regex `^[A-Z]{2}$`, valid billing country)
   - `DQ_CHECKS_CB`: 4 checks for chargebacks (base + chargeback_amount non-negative)
   - Each check has: (check_name, reason_code, severity, SQL predicate)
   - Compiled into `DQ_CHECKS_BY_STREAM` dict mapping stream_name → list of checks

6. **Streaming Config**:
   - `TRIGGER_INTERVAL_SECONDS` = 10 (micro-batch interval)
   - `WATERMARK_DELAY` = "10 minutes" (late data tolerance for Silver dedup)
   - Checkpoint paths for Bronze, Silver, Scoring, Gold
   - `BATCH_SIZE` = 500 (events per generator batch, was 200)
   - `BATCH_INTERVAL_S` = 5 (seconds between generator batches)
   - `GENERATOR_DURATION_S` = 0 (infinite generation)
   - Initializes structured logger with `logging.basicConfig`

7. **Smoke Test**: Prints all layer paths to verify config loaded correctly.

**Key Design Decisions**:
- All config in ONE notebook → change once, affects all stages
- `safe_foreachbatch` decorator defined here → wraps all streaming batch functions with error handling
- `show_df` helper for debugging
- Schemas are explicit `StructType` (not schema inference) → prevents schema drift

---

### Layer 1: 01_stream_generator — Event Simulation

**Purpose**: Continuously generates synthetic fraud events (transactions, logins, devices, chargebacks) and writes them as JSON files to the UC volume. Auto Loader in Bronze picks these up in real-time.

**Cells (6 total)**:

1. **Markdown Title**: Describes the generator flow: Generate batch → Write JSON → Sleep → Repeat.

2. **Run Config**: `%run ./00_config_stream` — inherits all paths, schemas, constants.

3. **Imports**: `random`, `uuid`, `json`, `time`, `datetime`, `Row`. Prints start config.

4. **Event Generation Functions**: 4 functions + 1 batch function:
   - `gen_transaction()`: Generates a transaction event. If `is_fraud=True` (8% chance): amount $600-$5000, IP from `RISK_COUNTRIES` (RU, NG, BR, AE), billing from `COUNTRIES`. If normal: amount $10-$500, IP=billing country. If `is_bad=True` (4% chance): injects one of 4 anomalies (missing customer_id, negative amount, missing timestamp, invalid country "XX_INVALID").
   - `gen_login()`: Login event with customer_id, device_id, IP country, user_agent, mfa_passed (80% pass rate).
   - `gen_device()`: Device event with device_type, is_new_device (15% chance).
   - `gen_chargeback()`: Chargeback event with chargeback_amount $10-$1000, reason_code from 4 options.
   - `gen_batch(n=BATCH_SIZE)`: Generates BATCH_SIZE events. Distribution: 70% transactions, 15% logins, 10% devices, 5% chargebacks.

5. **Batch Writer**: `write_batch(events)` — groups events by event_type, writes each group as a single JSONL file to `{STREAM_ROOT}/{event_type}/batch_{timestamp}.json` using `dbutils.fs.put`.

6. **Start Generation**: Infinite loop:
   - Generate batch of BATCH_SIZE events
   - Write JSON files to UC volume
   - Log: `[Generator] Batch {N} | {count} events | Total: {total} | Elapsed: {seconds}s`
   - Sleep BATCH_INTERVAL_S (5 seconds)
   - Repeat forever (GENERATOR_DURATION_S=0)
   - KeyboardInterrupt handler for graceful stop

**Output**: JSONL files in `{STREAM_ROOT}/fraud_transactions/`, `{STREAM_ROOT}/fraud_logins/`, `{STREAM_ROOT}/fraud_devices/`, `{STREAM_ROOT}/fraud_chargebacks/`

**Event Distribution**: 70% transactions, 15% logins, 10% devices, 5% chargebacks

**Fraud Injection**: 8% of transactions are fraudulent (high amount + risk country + country mismatch). 4% of all events have bad data (missing fields, negative amounts, invalid countries).

---

### Layer 2: 02_bronze_stream — Auto Loader Ingestion + DQ

**Purpose**: Uses Auto Loader (`cloudFiles`) to continuously read incoming JSON files, applies 8 Data Quality checks, splits valid → Bronze Delta tables, invalid → quarantine Delta tables.

**Cells (6 total)**:

1. **Markdown Title**: Describes Auto Loader flow and that it runs indefinitely.

2. **Run Config**: `%run ./00_config_stream`

3. **Imports**: PySpark functions (`col`, `expr`, `current_timestamp`, `to_date`, `lit`, `when`), `DeltaTable`.

4. **Bronze Stream Builder** (`build_bronze_stream(stream_name)`):
   - **Auto Loader**: `spark.readStream.format("cloudFiles")` with:
     - `cloudFiles.format = "json"` — reads JSONL files
     - `cloudFiles.schemaLocation` — stores schema evolution info
     - `schema = SCHEMAS[stream_name]` — explicit StructType (no inference)
     - Source path: `{STREAM_ROOT}/{stream_name}/`
   - **DQ Checks**: Combines all predicates for the stream into a single SQL expression with AND. Adds `dq_passed` boolean column.
   - **Enrichment**: Adds `ingested_at` (current_timestamp) and `event_date` (to_date from event_timestamp).
   - **foreachBatch** (`@safe_foreachbatch`): 
     - Splits dataframe: `valid = df.where(dq_passed == True)`, `invalid = df.where(dq_passed == False)`
     - Fills null event_date with "1970-01-01" for records with missing timestamps
     - **Optimized**: Single `groupBy("dq_passed").count().collect()` to get both valid and invalid counts in ONE Spark job (was 5 separate count() calls)
     - Valid events → `mode("append")` + `partitionBy("event_date")` → Bronze Delta
     - Invalid events → add `check_name`, `reason_code="DQ_CHECK_FAILED"`, `severity="critical"` → quarantine Delta with `mergeSchema=true`
     - Handles chargeback `reason_code` column collision by renaming to `chargeback_reason_code`
     - Logs: `[Bronze:{stream}] batch {id}: {valid_count} valid → Bronze` (INFO) and `{invalid_count} invalid → Quarantine` (WARNING)
   - **Streaming query**: `foreachBatch` + `checkpointLocation` + `trigger(processingTime='10s')` + `queryName(f"bronze_{stream_name}")`

5. **Start All Bronze Streams**: Loops over 4 stream names, starts 4 concurrent Auto Loader queries.

6. **Monitor**: `awaitTermination()` + prints Bronze and quarantine counts for verification.

**DQ Checks Applied**:

| Stream | Checks | Count |
|--------|--------|-------|
| fraud_transactions | event_id present, event_timestamp present, customer_id present, transaction_id present, amount ≥ 0, valid IP country (regex `^[A-Z]{2}$`), valid billing country | 7 |
| fraud_logins | event_id present, event_timestamp present, customer_id present | 3 |
| fraud_devices | event_id present, event_timestamp present, customer_id present | 3 |
| fraud_chargebacks | event_id present, event_timestamp present, customer_id present, chargeback_amount ≥ 0 | 4 |

**Output**: 
- Bronze Delta tables: `{BRONZE_ROOT}/{stream_name}/` (partitioned by `event_date`)
- Quarantine Delta tables: `{QUARANTINE_ROOT}/{stream_name}/` (with DQ failure metadata)

**DQ Pass Rate** (from validation): 97.4% (8,938 valid / 9,177 total), 239 quarantined records

---

### Layer 3: 03_silver_stream — Normalization + Deduplication

**Purpose**: Reads Bronze Delta tables as a stream, casts data types, deduplicates by `event_id` using watermark + `dropDuplicates`, and writes clean Silver Delta tables.

**Cells (6 total)**:

1. **Markdown Title**: Describes Silver flow — Bronze stream → cast types → watermark + dedup → Silver Delta.

2. **Run Config**: `%run ./00_config_stream`

3. **Imports**: PySpark functions (`col`, `current_timestamp`, `to_date`, `expr`).

4. **Silver Stream Builder** (`build_silver_stream(stream_name)`):
   - **Read Bronze as stream**: `spark.readStream.format("delta").load(bronze_path)` — reads Bronze Delta incrementally
   - **Type casting**: `event_timestamp` → `event_ts` (cast to timestamp)
   - **Watermark + Dedup**: 
     ```python
     .withWatermark("event_ts", WATERMARK_DELAY)  # "10 minutes"
     .dropDuplicates(["event_id"])
     ```
     The watermark tells Spark to keep dedup state for 10 minutes. Events within the window are deduplicated; older events are dropped and state is cleaned up.
   - **Enrichment**: Adds `event_date` (to_date from event_ts), `processed_at` (current_timestamp). Drops intermediate `event_ts` column.
   - **foreachBatch** (`@safe_foreachbatch`):
     - `mode("append")` + `partitionBy("event_date")` → Silver Delta
     - Logs: `[Silver:{stream}] batch {id}: written to Silver` (INFO)
     - Error handling: catches, logs, and re-raises (triggering Structured Streaming retry)
   - **Streaming query**: `foreachBatch` + `checkpointLocation` + `trigger(processingTime='10s')` + `queryName(f"silver_{stream_name}")`

5. **Start All Silver Streams**: Loops over 4 stream names, starts 4 concurrent streaming queries.

6. **Monitor**: `awaitTermination()` + prints Silver counts with distinct event_id verification.

**Key Concepts**:

- **Watermark**: `withWatermark("event_ts", "10 minutes")` — Spark keeps state for 10 minutes. Events arriving within 10 minutes of the latest seen event are processed. Events older than the watermark are dropped (state cleanup to prevent unbounded memory growth).
- **dropDuplicates**: `dropDuplicates(["event_id"])` — within the watermark window, if the same `event_id` appears twice, only the first occurrence is kept. This provides exactly-once processing semantics.
- **Why not just `mode("overwrite")`?**: Silver uses `mode("append")` because it's a streaming write. Overwrite would destroy previous data on every micro-batch.

**Output**: Silver Delta tables: `{SILVER_ROOT}/{stream_name}/` (partitioned by `event_date`)

**Dedup Results** (from validation): 
- Bronze total: 8,938 → Silver total: 8,372 (566 duplicates removed, ~6.3%)
- Zero duplicates remaining in Silver (verified by validation: 0 duplicates out of 5,804 transactions)

---

### Layer 4: 04_scoring_stream — Fraud Scoring + Alerting

**Purpose**: Reads Silver transactions as a stream, enriches with velocity features (customer transaction count, device customer count, failed login count), applies 7 weighted fraud rules, computes `risk_score` (0-100) and `risk_band` (LOW/MEDIUM/HIGH), and writes to scored Delta table. HIGH risk transactions also go to a real-time alerts table.

**Cells (7 total)**:

1. **Markdown Title**: Describes the scoring approach — `foreachBatch` allows joining with historical Silver data for velocity features.

2. **Run Config**: `%run ./00_config_stream`

3. **Imports**: PySpark functions (`col`, `when`, `lit`, `sum`, `count`, `countDistinct`, `max`, `greatest`, `least`, `concat_ws`, `current_timestamp`, `to_date`), `DeltaTable`.

4. **Scoring foreachBatch** (`@safe_foreachbatch process_scoring_batch(batch_df, batch_id)`):

   **Step 4a — Read historical Silver data (2-day window)**:
   ```python
   lookback = date_sub(current_date(), 1)  # 2 days
   tx_all = spark.read.format("delta").load(SILVER/fraud_transactions).where(event_date >= lookback)
   login_all = spark.read.format("delta").load(SILVER/fraud_logins).where(event_date >= lookback)
   device_all = spark.read.format("delta").load(SILVER/fraud_devices).where(event_date >= lookback)
   ```
   **Optimization**: Only reads last 2 days instead of entire Silver table. As data grows, this keeps the scan bounded.

   **Step 4b — Compute velocity features (5 aggregations)**:
   - `customer_velocity`: GROUP BY customer_id → `customer_txn_count`, `customer_total_amount`
   - `merchant_velocity`: GROUP BY merchant_id → `merchant_txn_count`
   - `device_customers`: GROUP BY device_id → `device_customer_count` (countDistinct customer_id)
   - `failed_logins`: WHERE event_type = "login_failed" GROUP BY customer_id → `failed_login_count`
   - `device_flags`: GROUP BY device_id → `is_new_device_flag` (max of is_new_device)

   **Step 4c — Enrich micro-batch with velocity features**:
   - LEFT JOIN batch_df with all 5 velocity aggregations
   - Fill nulls from left joins (customer_txn_count=1, merchant_txn_count=1, device_customer_count=1, failed_login_count=0, is_new_device=False)

   **Step 4d — Apply 7 fraud rules**:
   | Rule | Logic | Weight |
   |------|-------|--------|
   | `rule_high_amount` | amount ≥ 600.0 | 20 |
   | `rule_country_mismatch` | ip_country ≠ billing_country OR shipping_country ≠ billing_country | 20 |
   | `rule_new_device_high_amount` | is_new_device AND rule_high_amount | 20 |
   | `rule_high_velocity` | customer_txn_count ≥ 4 | 15 |
   | `rule_many_customers_on_device` | device_customer_count ≥ 3 | 10 |
   | `rule_failed_login_before_purchase` | failed_login_count ≥ 2 | 10 |
   | `rule_card_not_present_risk` | payment_method = "card_not_present" AND rule_country_mismatch | 15 |

   **Step 4e — Compute risk_score and risk_band**:
   ```python
   risk_score = least(sum(rule_weight * rule_triggered), 100)  # clip at 100
   risk_band = if score >= 70: HIGH, if score >= 40: MEDIUM, else LOW
   ```

   **Step 4f — Generate fraud reason codes**:
   ```python
   fraud_reason_codes = concat_ws(",", when(rule_high_amount, "HIGH_AMOUNT"), 
       when(rule_country_mismatch, "COUNTRY_MISMATCH"), ...)
   ```
   Example: `"HIGH_AMOUNT,COUNTRY_MISMATCH,NEW_DEVICE_HIGH_AMOUNT,MANY_CUSTOMERS_ON_DEVICE,CARD_NOT_PRESENT_RISK"`

   **Step 4g — Write to scored_transactions (MERGE)**:
   - Try `DeltaTable.forPath` + `MERGE` (upsert by event_id) — idempotent
   - If table doesn't exist: `mode("overwrite")` + `overwriteSchema=true` + `partitionBy("event_date")` — initial creation
   - **Bug fix**: Only overwrite on "Path does not exist" error. All other errors are re-raised (was: broad `except Exception` would overwrite on ANY error → data destruction)

   **Step 4h — Write HIGH risk to alerts table (MERGE)**:
   - Filter: `risk_band == "HIGH"`
   - Same MERGE pattern as scored (upsert by event_id)
   - Same safe error handling (only overwrite on first run)

5. **Start Scoring Stream**: 
   - `spark.readStream.format("delta").load(silver_tx_path)` — reads Silver transactions
   - `foreachBatch(safe_scoring_batch)` — calls `process_scoring_batch`
   - `trigger(processingTime='10s')` — micro-batch every 10 seconds
   - `queryName("scoring_stream")`

6. **Monitor**: `awaitTermination()` + prints scored counts by risk band and alerts count.

**Output**:
- Scored Delta: `{SCORED_ROOT}/scored_transactions` (37 columns, partitioned by event_date)
- Alerts Delta: `{ALERTS_ROOT}/high_risk_alerts` (only HIGH risk transactions)

**Scoring Results** (from validation):
- Total scored: 5,804 transactions
- HIGH: 12 (0.2%) — avg score 74.6
- MEDIUM: 425 (7.3%) — avg score 49.8
- LOW: 5,367 (92.5%) — avg score 7.6
- Risk score range: 0-85
- Alerts: 12 (all HIGH, all with reason codes)

**Why foreachBatch instead of direct streaming writes?**:
- JOIN with historical Silver data for velocity features (not possible in pure streaming)
- MERGE into Delta tables (upsert, not just append)
- Conditional writes (only HIGH risk → alerts table)
- Multiple writes from same batch (scored + alerts)

---

### Layer 5: 05_gold_stream — Analytics Tables

**Purpose**: Reads scored transactions as a stream and uses `foreachBatch` to recompute 7 gold analytics tables every 60 seconds. These tables power the AI/BI dashboard.

**Cells (7 total)**:

1. **Markdown Title**: Lists all 7 gold tables and their purposes.

2. **Run Config**: `%run ./00_config_stream`

3. **Imports**: PySpark functions (`col`, `when`, `lit`, `count`, `countDistinct`, `sum`, `avg`, `max`), `DeltaTable`.

4. **Gold foreachBatch** (`@safe_foreachbatch process_gold_batch(batch_df, batch_id)`):

   **Step 4a — Skip empty batches**: `if batch_df.limit(1).count() == 0: return`

   **Step 4b — Read ALL scored transactions**: `scored_all = spark.read.format("delta").load(scored_path)` — full table read for recomputing aggregations

   **Step 4c — Recompute 7 gold tables**:

   **Table 1: fraud_summary_by_country** (OVERWRITE):
   ```sql
   GROUP BY ip_country → total_transactions, total_amount, avg_risk_score, high_risk_count
   ```
   Result: 9 rows (one per country)

   **Table 2: fraud_summary_by_merchant** (OVERWRITE):
   ```sql
   GROUP BY merchant_id, merchant_category → total_transactions, total_amount, avg_risk_score, high_risk_count
   ```
   Result: 2,828 rows (one per merchant)

   **Table 3: high_risk_transactions** (MERGE):
   - Filter: `risk_band == "HIGH"` from current batch only
   - MERGE by event_id (upsert) — only new HIGH risk transactions are added
   - Result: 12 rows (all HIGH risk transactions)

   **Table 4: customer_risk_profiles** (OVERWRITE):
   ```sql
   GROUP BY customer_id → txn_count, total_spent, avg_risk_score, max_risk_score, high_risk_count
   ```
   Result: 3,450 rows (one per customer)

   **Table 5: merchant_risk_profiles** (OVERWRITE):
   ```sql
   GROUP BY merchant_id → txn_count, total_volume, avg_risk_score, high_risk_count
   ```
   Result: 500 rows (one per merchant)

   **Table 6: device_risk_profiles** (OVERWRITE):
   ```sql
   GROUP BY device_id → unique_customers (countDistinct), txn_count, avg_risk_score, high_risk_count
   ```
   Result: 1,873 rows (one per device)

   **Table 7: chargeback_summary** (OVERWRITE):
   - Reads from Silver chargebacks (not scored)
   ```sql
   GROUP BY reason_code → chargeback_count, total_chargeback_amount
   ```
   - **Error handling**: `try/except` with `logger.warning` — if Silver chargebacks don't exist yet, logs warning and continues (was: silent `except: pass`)
   - Result: 4 rows (one per chargeback reason)

   **Step 4d — Log**: `[Gold] batch {id}: 7 gold tables refreshed`

5. **Start Gold Stream**:
   - `spark.readStream.format("delta").load(scored_path)` — reads scored transactions
   - `foreachBatch(safe_gold_batch)` — calls `process_gold_batch`
   - `trigger(processingTime='60s')` — **60-second trigger** (was 10s, increased for 6x compute saving)
   - `queryName("gold_stream")`

6. **Monitor**: `awaitTermination()` + prints all 7 gold table counts.

**Why OVERWRITE for 6 tables and MERGE for 1?**:
- Tables 1, 2, 4, 5, 6 use `mode("overwrite")` because they're aggregations — recomputing from all scored data gives correct results every time. No incremental state needed.
- Table 3 (high_risk_transactions) uses MERGE because it's a filter, not an aggregation — we only want to ADD new HIGH risk transactions, not recompute.

**Why 60-second trigger?**:
- Gold tables are analytics — 60-second freshness is more than sufficient for a dashboard
- 7 full table overwrites every 60s = 7 overwrites/minute (was 42/minute at 10s trigger)
- 6x reduction in Gold compute overhead

**Output**: 7 Gold Delta tables at `{GOLD_ROOT}/`:
1. `fraud_summary_by_country` — 9 rows
2. `fraud_summary_by_merchant` — 2,828 rows
3. `high_risk_transactions` — 12 rows
4. `customer_risk_profiles` — 3,450 rows
5. `merchant_risk_profiles` — 500 rows
6. `device_risk_profiles` — 1,873 rows
7. `chargeback_summary` — 4 rows

---

### Layer 6: 06_validation_stream — Data Validation

**Purpose**: One-shot batch notebook that validates the entire pipeline end-to-end. NOT part of the continuous job — run manually after pipeline has processed data.

**Validation Checks (37 total, 100% pass rate)**:

1. **Table Existence & Row Counts** (17 checks): Verifies all Bronze, Silver, Scored, Alerts, and Gold tables exist with expected row counts.

2. **Duplicate Checks** (5 checks): Verifies zero duplicates by `event_id` in Silver and Scored tables.

3. **Risk Band Distribution** (4 checks): Verifies HIGH, MEDIUM, LOW bands are present and risk_score is in valid range [0, 100].

4. **Data Quality Pass Rate** (1 check): Verifies DQ pass rate > 90%. Current: 97.4%.

5. **Gold Table Completeness** (8 checks): Verifies all 7 gold tables are populated and high_risk count is consistent across scored, gold, and alerts (scored=12, gold=12, alerts=12).

6. **Real-Time Alerts Verification** (2 checks): Verifies all alerts are HIGH risk and all have reason codes.

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
5. **Committed and pushed**: All 10 files pushed to GitHub

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
├── databricks.yml              # DABs bundle config
└── FraudStream_V3_Production_Guide.md  # This document
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

### Q11: Why does Gold use `mode("overwrite")` for 6 tables but MERGE for high_risk_transactions?

**A**: Tables like `fraud_summary_by_country` and `customer_risk_profiles` are aggregations — recomputing from all scored data gives correct results every time. Overwrite is simplest and guaranteed correct. But `high_risk_transactions` is a filter, not an aggregation — we only want to ADD new HIGH risk transactions as they arrive, not recompute. MERGE (upsert by event_id) ensures we don't lose existing HIGH risk records and don't duplicate them.

### Q12: What are the 7 fraud rules and how is the risk score computed?

**A**: The 7 rules are: `rule_high_amount` (amount ≥ $600, weight 20), `rule_country_mismatch` (IP/billing/shipping countries disagree, weight 20), `rule_new_device_high_amount` (new device + high amount, weight 20), `rule_high_velocity` (customer has ≥ 4 transactions, weight 15), `rule_many_customers_on_device` (device used by ≥ 3 customers, weight 10), `rule_failed_login_before_purchase` (≥ 2 failed logins before purchase, weight 10), `rule_card_not_present_risk` (card-not-present + country mismatch, weight 15). The risk score is the weighted sum of all triggered rules, clipped at 100. Risk bands: HIGH (70-100), MEDIUM (40-69), LOW (0-39).

### Q13: What is Auto Loader and why use it instead of regular file reading?

**A**: Auto Loader (`cloudFiles` format) is Databricks' incremental file ingestion system. It uses directory listing + file notification services (like AWS S3 event notifications) to efficiently detect new files as they arrive. Unlike `spark.read` which reads all files at once, Auto Loader tracks which files have been processed using a checkpoint and only reads NEW files in each micro-batch. This makes it ideal for streaming ingestion of JSON/CSV/Parquet files landing in cloud storage. I use it with an explicit `StructType` schema to prevent schema inference drift.

### Q14: What does the `safe_foreachbatch` decorator do and why is it needed?

**A**: The `safe_foreachbatch` decorator wraps any `foreachBatch` function with a try/except that logs the error with the function name and batch ID, then re-raises the exception. This is needed because without it, an unhandled exception in `foreachBatch` would crash the streaming query silently — the query would stop processing but the error might not be visible. With the decorator, errors are logged with full context, and re-raising allows Structured Streaming to retry the batch. If retries are exhausted, the task fails and the job's `ON_FAILURE` retry kicks in, followed by an email alert.

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
│  │ 500 ev/5s    │    │ DQ checks    │    │ 10min wmark  │    │
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
│  │ (10 files)│  │ (YAML)   │  │  deploy on main push)     │   │
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
| `BATCH_SIZE` | `500` | Events per generator batch |
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

## Validation Results (37/37 checks passed)

| Layer | Table | Rows |
|-------|-------|------|
| Bronze | fraud_transactions | 6,062 |
| Bronze | fraud_logins | 1,448 |
| Bronze | fraud_devices | 924 |
| Bronze | fraud_chargebacks | 504 |
| Silver | fraud_transactions | 5,804 |
| Silver | fraud_logins | 1,298 |
| Silver | fraud_devices | 826 |
| Silver | fraud_chargebacks | 444 |
| Scored | scored_transactions | 5,804 |
| Alerts | high_risk_alerts | 12 |
| Gold | fraud_summary_by_country | 9 |
| Gold | fraud_summary_by_merchant | 2,828 |
| Gold | high_risk_transactions | 12 |
| Gold | customer_risk_profiles | 3,450 |
| Gold | merchant_risk_profiles | 500 |
| Gold | device_risk_profiles | 1,873 |
| Gold | chargeback_summary | 4 |

DQ Pass Rate: 97.4% (8,938 valid / 9,177 total, 239 quarantined)
Risk Bands: HIGH=12 (0.2%), MEDIUM=425 (7.3%), LOW=5,367 (92.5%)
Cross-layer consistency: scored=12 = gold=12 = alerts=12 ✅

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
| Version control | ✅ | GitHub repo with all 10 files |
| CI/CD | ✅ | DABs + GitHub Actions (validate on PR, deploy on main) |
| Validation | ✅ | 06_validation_stream (37 checks, 100% pass rate) |

---

*Document generated: October 11, 2026*
*Pipeline: FraudStream V3 — Real-Time Fraud Detection*
*GitHub: https://github.com/guptamasoom/fraudstream-v3*