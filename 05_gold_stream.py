# Databricks notebook source
# DBTITLE 1,FraudStream V3 — Gold Streaming
# MAGIC %md
# MAGIC # FraudStream V3 — Gold Streaming
# MAGIC
# MAGIC Reads scored transactions as a stream and uses `foreachBatch` to MERGE into 7 gold analytics tables in real-time:
# MAGIC
# MAGIC 1. `fraud_summary_by_country` — risk metrics by country
# MAGIC 2. `fraud_summary_by_merchant` — risk metrics by merchant
# MAGIC 3. `high_risk_transactions` — all HIGH risk transactions
# MAGIC 4. `customer_risk_profiles` — per-customer risk aggregation
# MAGIC 5. `merchant_risk_profiles` — per-merchant risk aggregation
# MAGIC 6. `device_risk_profiles` — per-device risk aggregation
# MAGIC 7. `chargeback_summary` — chargeback stats by reason
# MAGIC
# MAGIC **Usage**: Start after Scoring streaming is running.

# COMMAND ----------

# DBTITLE 1,Run Config
# MAGIC %run ./00_config_stream

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import (
    col, when, lit, count, countDistinct, sum as spark_sum, avg, max as spark_max
)
from delta.tables import DeltaTable

print("Gold streaming starting...")
print(f"  Scored root: {SCORED_ROOT}")
print(f"  Gold root:   {GOLD_ROOT}")

# COMMAND ----------

# DBTITLE 1,Gold foreachBatch Function
# ── Gold streaming: read scored transactions stream, MERGE/recompute 7 gold tables via foreachBatch ──

scored_path = f"{SCORED_ROOT}/scored_transactions"
ckpt_path  = f"{GOLD_CHECKPOINT}/gold"

scored_stream = (spark.readStream
    .format("delta")
    .load(scored_path)
)

@safe_foreachbatch
def process_gold_batch(batch_df, batch_id):
    if batch_df.limit(1).count() == 0:
        return

    # Read ALL scored transactions for recomputing aggregations
    scored_all = spark.read.format("delta").load(scored_path)

    # 1. fraud_summary_by_country
    country_summary = scored_all.groupBy("ip_country").agg(
        count("transaction_id").alias("total_transactions"),
        spark_sum("amount").alias("total_amount"),
        avg("risk_score").alias("avg_risk_score"),
        count(when(col("risk_band") == "HIGH", True)).alias("high_risk_count"),
    )
    (country_summary.write.format("delta").mode("overwrite")
        .save(f"{GOLD_ROOT}/fraud_summary_by_country"))

    # 2. fraud_summary_by_merchant
    merchant_summary = scored_all.groupBy("merchant_id", "merchant_category").agg(
        count("transaction_id").alias("total_transactions"),
        spark_sum("amount").alias("total_amount"),
        avg("risk_score").alias("avg_risk_score"),
        count(when(col("risk_band") == "HIGH", True)).alias("high_risk_count"),
    )
    (merchant_summary.write.format("delta").mode("overwrite")
        .save(f"{GOLD_ROOT}/fraud_summary_by_merchant"))

    # 3. high_risk_transactions (MERGE new high-risk from this batch)
    high_risk = batch_df.where(col("risk_band") == "HIGH")
    if high_risk.limit(1).count() > 0:
        hr_path = f"{GOLD_ROOT}/high_risk_transactions"
        try:
            dt = DeltaTable.forPath(spark, hr_path)
            dt.alias("t").merge(high_risk.alias("s"), "t.event_id = s.event_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        except Exception:
            high_risk.write.format("delta").mode("overwrite").save(hr_path)

    # 4. customer_risk_profiles
    customer_profiles = scored_all.groupBy("customer_id").agg(
        count("transaction_id").alias("txn_count"),
        spark_sum("amount").alias("total_spent"),
        avg("risk_score").alias("avg_risk_score"),
        spark_max("risk_score").alias("max_risk_score"),
        count(when(col("risk_band") == "HIGH", True)).alias("high_risk_count"),
    )
    (customer_profiles.write.format("delta").mode("overwrite")
        .save(f"{GOLD_ROOT}/customer_risk_profiles"))

    # 5. merchant_risk_profiles
    merchant_profiles = scored_all.groupBy("merchant_id").agg(
        count("transaction_id").alias("txn_count"),
        spark_sum("amount").alias("total_volume"),
        avg("risk_score").alias("avg_risk_score"),
        count(when(col("risk_band") == "HIGH", True)).alias("high_risk_count"),
    )
    (merchant_profiles.write.format("delta").mode("overwrite")
        .save(f"{GOLD_ROOT}/merchant_risk_profiles"))

    # 6. device_risk_profiles
    device_profiles = scored_all.groupBy("device_id").agg(
        countDistinct("customer_id").alias("unique_customers"),
        count("transaction_id").alias("txn_count"),
        avg("risk_score").alias("avg_risk_score"),
        count(when(col("risk_band") == "HIGH", True)).alias("high_risk_count"),
    )
    (device_profiles.write.format("delta").mode("overwrite")
        .save(f"{GOLD_ROOT}/device_risk_profiles"))

    # 7. chargeback_summary (from Silver chargebacks)
    try:
        cb_all = spark.read.format("delta").load(f"{SILVER_ROOT}/fraud_chargebacks")
        cb_summary = cb_all.groupBy("reason_code").agg(
            count("event_id").alias("chargeback_count"),
            spark_sum("chargeback_amount").alias("total_chargeback_amount"),
        )
        (cb_summary.write.format("delta").mode("overwrite")
            .save(f"{GOLD_ROOT}/chargeback_summary"))
    except Exception as e:
        logger.warning(f"[Gold] chargeback_summary skipped: {e}")

    logger.info(f"[Gold] batch {batch_id}: 7 gold tables refreshed")

print("Gold foreachBatch function defined.")

# COMMAND ----------

# DBTITLE 1,Start Gold Stream
# ── Start the Gold streaming query ──
print("Starting Gold streaming...")
print("="*60)
print("This stream reads scored transactions and refreshes 7 gold tables.")
print("Ensure 04_scoring_stream is running.\n")

def safe_gold_batch(batch_df, batch_id):
    try:
        process_gold_batch(batch_df, batch_id)
    except Exception as e:
        logger.error(f"[Gold] batch {batch_id} FAILED: {e}", exc_info=True)

gold_query = (scored_stream.writeStream
    .foreachBatch(safe_gold_batch)
    .option("checkpointLocation", ckpt_path)
    .trigger(processingTime='60s')
    .queryName("gold_stream")
    .start()
)

print(f"Gold stream started: {gold_query.name}")
print("   Waiting for scored transactions...")

# COMMAND ----------

# DBTITLE 1,Monitor Gold Stream
# ── Wait for Gold stream to finish + verify results ──
import time

print("Waiting for Gold stream to process all available data...")
try:
    gold_query.awaitTermination()
    print("  gold_stream: completed")
except Exception as e:
    print(f"  gold_stream: {e}")

print("\n✅ Gold stream finished.\n")
gold_tables = [
    "fraud_summary_by_country", "fraud_summary_by_merchant", "high_risk_transactions",
    "customer_risk_profiles", "merchant_risk_profiles", "device_risk_profiles", "chargeback_summary",
]
print("Gold table counts:")
for gt in gold_tables:
    try:
        cnt = spark.read.format("delta").load(f"{GOLD_ROOT}/{gt}").count()
        print(f"  Gold.{gt}: {cnt:,} rows")
    except:
        print(f"  Gold.{gt}: not yet created")

# COMMAND ----------

