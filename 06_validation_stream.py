# Databricks notebook source
# DBTITLE 1,FraudStream V3 — Validation
# MAGIC %md
# MAGIC # FraudStream V3 — Validation
# MAGIC
# MAGIC Validates all streaming Delta tables: row counts, duplicate checks, risk band distribution, DQ pass rates, gold table completeness, and real-time alert verification.
# MAGIC
# MAGIC **Usage**: Run this as a batch notebook while the streaming pipeline is running (or after stopping it). It reads the Delta tables in batch mode.

# COMMAND ----------

# DBTITLE 1,Run Config
# MAGIC %run ./00_config_stream

# COMMAND ----------

# DBTITLE 1,Imports & Check Helper
from pyspark.sql.functions import col, count, countDistinct, sum as spark_sum, avg, max as spark_max

checks_passed = 0
checks_failed = 0
results = []

def check(name, condition, detail=""):
    global checks_passed, checks_failed
    status = "PASS" if condition else "FAIL"
    if condition:
        checks_passed += 1
    else:
        checks_failed += 1
    results.append((name, status, detail))
    print(f"  {status}  {name:45s} {detail}")

print("FraudStream V3 — Validation")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Table Existence & Row Counts
# ── 1. Check table existence & row counts ──
print("\n1. Table Existence & Row Counts")
print("-"*80)

def table_count(path, name):
    try:
        df = spark.read.format("delta").load(path)
        cnt = df.count()
        check(f"{name} exists", True, f"{cnt:,} rows")
        return cnt
    except Exception:
        check(f"{name} exists", False, "table not found")
        return 0

bronze_counts = {}
silver_counts = {}
for stream_name in STREAMS:
    bronze_counts[stream_name] = table_count(f"{BRONZE_ROOT}/{stream_name}", f"Bronze.{stream_name}")
    silver_counts[stream_name] = table_count(f"{SILVER_ROOT}/{stream_name}", f"Silver.{stream_name}")

scored_count = table_count(f"{SCORED_ROOT}/scored_transactions", "Scored.transactions")
alerts_count = table_count(f"{ALERTS_ROOT}/high_risk_alerts", "Alerts.high_risk")

# Gold tables
gold_tables = [
    "fraud_summary_by_country", "fraud_summary_by_merchant", "high_risk_transactions",
    "customer_risk_profiles", "merchant_risk_profiles", "device_risk_profiles", "chargeback_summary",
]
gold_counts = {}
for gt in gold_tables:
    gold_counts[gt] = table_count(f"{GOLD_ROOT}/{gt}", f"Gold.{gt}")

# COMMAND ----------

# DBTITLE 1,Duplicate Checks
# ── 2. Duplicate checks (event_id should be unique) ──
print("\n2. Duplicate Checks (event_id uniqueness)")
print("-"*80)

for stream_name in STREAMS:
    if silver_counts.get(stream_name, 0) > 0:
        df = spark.read.format("delta").load(f"{SILVER_ROOT}/{stream_name}")
        total = df.count()
        distinct = df.select("event_id").distinct().count()
        dups = total - distinct
        check(f"Silver.{stream_name} no duplicates", dups == 0, f"{dups} duplicates out of {total:,}")

if scored_count > 0:
    df = spark.read.format("delta").load(f"{SCORED_ROOT}/scored_transactions")
    total = df.count()
    distinct = df.select("event_id").distinct().count()
    dups = total - distinct
    check("Scored.transactions no duplicates", dups == 0, f"{dups} duplicates out of {total:,}")

# COMMAND ----------

# DBTITLE 1,Risk Band Distribution
# ── 3. Risk band distribution ──
print("\n3. Risk Band Distribution")
print("-"*80)

if scored_count > 0:
    scored = spark.read.format("delta").load(f"{SCORED_ROOT}/scored_transactions")
    bands = scored.groupBy("risk_band").count().collect()
    band_map = {row["risk_band"]: row["count"] for row in bands}
    for band in ["HIGH", "MEDIUM", "LOW"]:
        cnt = band_map.get(band, 0)
        pct = cnt / scored_count * 100 if scored_count else 0
        check(f"risk_band={band} present", cnt > 0, f"{cnt:,} ({pct:.1f}%)")

    # Check risk_score range (0-100)
    min_score = scored.agg({"risk_score": "min"}).collect()[0][0]
    max_score = scored.agg({"risk_score": "max"}).collect()[0][0]
    check("risk_score in [0,100]", min_score >= 0 and max_score <= 100, f"min={min_score}, max={max_score}")
else:
    check("Scored data exists", False, "no scored transactions")

# COMMAND ----------

# DBTITLE 1,DQ Pass Rate
# ── 4. DQ pass rate (Bronze valid vs quarantine) ──
print("\n4. Data Quality Pass Rate")
print("-"*80)

total_valid = sum(bronze_counts.values())
total_quarantine = 0
for stream_name in STREAMS:
    try:
        q_df = spark.read.format("delta").load(f"{QUARANTINE_ROOT}/{stream_name}")
        q_cnt = q_df.count()
        total_quarantine += q_cnt
    except:
        pass

total_events = total_valid + total_quarantine
dq_pass_rate = (total_valid / total_events * 100) if total_events > 0 else 0
check("DQ pass rate > 90%", dq_pass_rate > 90, f"{dq_pass_rate:.1f}% ({total_valid:,} valid / {total_events:,} total)")

# Show quarantine breakdown by stream
for stream_name in STREAMS:
    try:
        q_df = spark.read.format("delta").load(f"{QUARANTINE_ROOT}/{stream_name}")
        q_cnt = q_df.count()
        if q_cnt > 0:
            print(f"    Quarantine {stream_name}: {q_cnt} rows")
    except:
        pass

# COMMAND ----------

# DBTITLE 1,Gold Table Completeness
# ── 5. Gold table completeness ──
print("\n5. Gold Table Completeness")
print("-"*80)

for gt in gold_tables:
    cnt = gold_counts.get(gt, 0)
    # chargeback_summary might be 0 if no chargebacks yet — that's OK
    if gt == "chargeback_summary":
        check(f"Gold.{gt} populated", True, f"{cnt} rows (may be 0 if no chargebacks yet)")
    else:
        check(f"Gold.{gt} populated", cnt > 0, f"{cnt} rows")

# Spot-check: high_risk_transactions should match alerts table count
if scored_count > 0:
    scored = spark.read.format("delta").load(f"{SCORED_ROOT}/scored_transactions")
    hr_in_scored = scored.where(col("risk_band") == "HIGH").count()
    hr_in_gold = gold_counts.get("high_risk_transactions", 0)
    hr_in_alerts = alerts_count
    check("high_risk count consistent", hr_in_scored == hr_in_gold == hr_in_alerts,
          f"scored={hr_in_scored}, gold={hr_in_gold}, alerts={hr_in_alerts}")

# COMMAND ----------

# DBTITLE 1,Real-Time Alerts Verification
# ── 6. Real-time alerts verification ──
print("\n6. Real-Time Alerts Verification")
print("-"*80)

if alerts_count > 0:
    alerts = spark.read.format("delta").load(f"{ALERTS_ROOT}/high_risk_alerts")
    # All alerts should be HIGH risk
    non_high = alerts.where(col("risk_band") != "HIGH").count()
    check("All alerts are HIGH risk", non_high == 0, f"{non_high} non-HIGH alerts found")
    
    # All alerts should have fraud_reason_codes
    no_reasons = alerts.where(col("fraud_reason_codes").isNull() | (col("fraud_reason_codes") == "")).count()
    check("All alerts have reason codes", no_reasons == 0, f"{no_reasons} alerts without reason codes")
    
    # Show sample alerts
    print("\n    Sample alerts:")
    alerts.select("transaction_id", "customer_id", "amount", "risk_score", "fraud_reason_codes", "scored_at") \
        .orderBy(col("scored_at").desc()).show(5, truncate=False)
else:
    check("Alerts exist", False, "no alerts generated yet")

# COMMAND ----------

# DBTITLE 1,Validation Summary
# ── Summary ──
print("\n" + "="*80)
print(f"FraudStream V3 Validation Summary")
print("="*80)
print(f"  Total checks:  {checks_passed + checks_failed}")
print(f"  Passed:        {checks_passed}")
print(f"  Failed:        {checks_failed}")
print(f"  Pass rate:     {checks_passed/(checks_passed+checks_failed)*100:.1f}%" if (checks_passed+checks_failed) > 0 else "  No checks run.")

if checks_failed > 0:
    print("\n  Failed checks:")
    for name, status, detail in results:
        if status == "FAIL":
            print(f"    ✗ {name}: {detail}")

print("\n" + ("✅ All checks passed!" if checks_failed == 0 else "❌ Some checks failed."))
print("\nNext steps:")
print("  - Build an AI/BI dashboard over the Gold tables for real-time fraud monitoring")
print("  - Add a schedule to the streaming job for continuous operation")
print("  - Set up alerting notifications for high-risk transactions")