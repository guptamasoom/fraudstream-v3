# Databricks notebook source
# DBTITLE 1,FraudStream V3 — Scoring Streaming
# MAGIC %md
# MAGIC # FraudStream V3 — Real-Time Fraud Scoring
# MAGIC
# MAGIC Reads Silver transactions as a stream, enriches with velocity features (customer txn count, device customer count, failed logins), applies 7 weighted fraud rules, computes `risk_score` (0-100) and `risk_band` (LOW/MEDIUM/HIGH), and writes to the scored Delta table. High-risk transactions are also written to a real-time **alerts** table.
# MAGIC
# MAGIC **Key**: Uses `foreachBatch` to compute velocity features against historical Silver data within each micro-batch. This allows complex joins and aggregations that aren't possible in pure streaming.
# MAGIC
# MAGIC **Usage**: Start after Silver streaming is running.

# COMMAND ----------

# DBTITLE 1,Run Config
# MAGIC %run ./00_config_stream

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import (
    col, when, lit, sum as spark_sum, count, countDistinct, max as spark_max,
    greatest, least, concat_ws, current_timestamp, to_date
)
from delta.tables import DeltaTable

print("Real-time fraud scoring starting...")
print(f"  Silver root: {SILVER_ROOT}")
print(f"  Scored root: {SCORED_ROOT}")
print(f"  Alerts root: {ALERTS_ROOT}")

# COMMAND ----------

# DBTITLE 1,Scoring foreachBatch Function
# ── Real-time scoring: read Silver transactions stream, apply fraud rules via foreachBatch ──
# The foreachBatch approach lets us join with ALL historical Silver data for velocity features

silver_tx_path = f"{SILVER_ROOT}/fraud_transactions"
scored_path   = f"{SCORED_ROOT}/scored_transactions"
alerts_path  = f"{ALERTS_ROOT}/high_risk_alerts"
ckpt_path    = f"{SCORING_CHECKPOINT}/scored_transactions"

# Read Silver transactions as a stream
silver_tx_stream = (spark.readStream
    .format("delta")
    .load(silver_tx_path)
)

@safe_foreachbatch
def process_scoring_batch(batch_df, batch_id):
    # Process batch directly (no pre-count to avoid extra Spark jobs)

    # ── Read recent Silver data (last 2 days) for velocity features ──
    from pyspark.sql.functions import date_sub, current_date
    lookback = date_sub(current_date(), 1)
    tx_all     = spark.read.format("delta").load(f"{SILVER_ROOT}/fraud_transactions").where(col("event_date") >= lookback)
    login_all  = spark.read.format("delta").load(f"{SILVER_ROOT}/fraud_logins").where(col("event_date") >= lookback)
    device_all = spark.read.format("delta").load(f"{SILVER_ROOT}/fraud_devices").where(col("event_date") >= lookback)

    # Customer velocity (total txns + total amount per customer)
    customer_velocity = tx_all.groupBy("customer_id").agg(
        count("transaction_id").alias("customer_txn_count"),
        spark_sum("amount").alias("customer_total_amount"),
    )

    # Merchant velocity
    merchant_velocity = tx_all.groupBy("merchant_id").agg(
        count("transaction_id").alias("merchant_txn_count"),
    )

    # Device → distinct customer count
    device_customers = tx_all.groupBy("device_id").agg(
        countDistinct("customer_id").alias("device_customer_count"),
    )

    # Failed logins per customer
    failed_logins = login_all.where(col("event_type") == "login_failed").groupBy("customer_id").agg(
        count("event_id").alias("failed_login_count"),
    )

    # Device new-device flag
    device_flags = device_all.groupBy("device_id").agg(
        spark_max("is_new_device").alias("is_new_device_flag"),
    )

    # ── Enrich the micro-batch with velocity features ──
    scored = (batch_df
        .join(customer_velocity, on="customer_id", how="left")
        .join(merchant_velocity, on="merchant_id", how="left")
        .join(device_customers, on="device_id", how="left")
        .join(failed_logins, on="customer_id", how="left")
        .join(device_flags, on="device_id", how="left")
    )

    # Fill nulls from left joins
    scored = (scored
        .withColumn("customer_txn_count", when(col("customer_txn_count").isNull(), lit(1)).otherwise(col("customer_txn_count")))
        .withColumn("merchant_txn_count", when(col("merchant_txn_count").isNull(), lit(1)).otherwise(col("merchant_txn_count")))
        .withColumn("device_customer_count", when(col("device_customer_count").isNull(), lit(1)).otherwise(col("device_customer_count")))
        .withColumn("failed_login_count", when(col("failed_login_count").isNull(), lit(0)).otherwise(col("failed_login_count")))
        .withColumn("is_new_device", when(col("is_new_device_flag").isNull(), lit(False)).otherwise(col("is_new_device_flag").cast("boolean")))
        .drop("is_new_device_flag")
    )

    # ── Apply 7 fraud rules ──
    scored = scored.withColumn("rule_high_amount", col("amount") >= HIGH_AMOUNT_THRESHOLD)
    scored = scored.withColumn("rule_country_mismatch",
        (col("ip_country").isNull() | (col("ip_country") != col("billing_country"))) |
        (col("shipping_country").isNull() | (col("shipping_country") != col("billing_country")))
    )
    scored = scored.withColumn("rule_new_device_high_amount", col("is_new_device") & col("rule_high_amount"))
    scored = scored.withColumn("rule_high_velocity", col("customer_txn_count") >= HIGH_VELOCITY_THRESHOLD)
    scored = scored.withColumn("rule_many_customers_on_device", col("device_customer_count") >= MANY_CUSTOMERS_THRESHOLD)
    scored = scored.withColumn("rule_failed_login_before_purchase", col("failed_login_count") >= FAILED_LOGIN_THRESHOLD)
    scored = scored.withColumn("rule_card_not_present_risk", (col("payment_method") == "card_not_present") & col("rule_country_mismatch"))

    # ── Risk score (weighted sum, clipped at 100) ──
    risk_expr = lit(0)
    for rule_name, weight in RULE_WEIGHTS.items():
        risk_expr = risk_expr + (col(rule_name).cast("int") * lit(weight))
    scored = scored.withColumn("risk_score", least(risk_expr, lit(100)))
    scored = scored.withColumn("risk_band",
        when(col("risk_score") >= 70, lit("HIGH"))
        .when(col("risk_score") >= 40, lit("MEDIUM"))
        .otherwise(lit("LOW"))
    )

    # ── Fraud reason codes ──
    reason_cols = []
    for rule_name in RULE_WEIGHTS.keys():
        label = rule_name.replace("rule_", "").upper()
        reason_cols.append(when(col(rule_name), lit(label)).otherwise(lit(None)))
    scored = scored.withColumn("fraud_reason_codes", concat_ws(",", *reason_cols))
    scored = scored.withColumn("scored_at", current_timestamp())
    scored = scored.withColumn("event_date", to_date(col("event_timestamp")))

    # ── MERGE into scored_transactions Delta ──
    try:
        dt = DeltaTable.forPath(spark, scored_path)
        dt.alias("t").merge(
            scored.alias("s"), "t.event_id = s.event_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.info(f"[Scoring] batch {batch_id}: MERGE -> scored_transactions")
    except Exception as e:
        if "Path does not exist" in str(e):
            scored.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
                .partitionBy("event_date").save(scored_path)
            logger.info(f"[Scoring] batch {batch_id}: initial write -> scored_transactions")
        else:
            print(f"  [Scoring] batch {batch_id}: ERROR in scored write - {e}")
            raise

    # ── Write HIGH risk to real-time alerts table ──
    high_risk = scored.where(col("risk_band") == "HIGH")
    try:
        dt_alerts = DeltaTable.forPath(spark, alerts_path)
        dt_alerts.alias("t").merge(
            high_risk.alias("s"), "t.event_id = s.event_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.info(f"[Scoring] batch {batch_id}: HIGH risk -> alerts")
    except Exception as e:
        if "Path does not exist" in str(e):
            high_risk.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(alerts_path)
            logger.info(f"[Scoring] batch {batch_id}: HIGH risk -> alerts (initial)")
        else:
            print(f"  [Scoring] batch {batch_id}: ERROR in alerts write - {e}")
            raise

print("Scoring foreachBatch function defined.")

# COMMAND ----------

# DBTITLE 1,Start Scoring Stream
# ── Start the real-time scoring stream ──
print("Starting real-time fraud scoring stream...")
print("="*60)
print("This stream reads Silver transactions, applies 7 fraud rules,")
print("computes risk_score/band, and writes to scored_transactions + alerts.")
print("Ensure 03_silver_stream is running.\n")

def safe_scoring_batch(batch_df, batch_id):
    try:
        process_scoring_batch(batch_df, batch_id)
    except Exception as e:
        logger.error(f"[Scoring] batch {batch_id} FAILED: {e}", exc_info=True)

scoring_query = (silver_tx_stream.writeStream
    .foreachBatch(safe_scoring_batch)
    .option("checkpointLocation", ckpt_path)
    .trigger(processingTime='10s')
    .queryName("scoring_stream")
    .start()
)

print(f"✅ Scoring stream started: {scoring_query.name}")
print("   Waiting for Silver transactions...")

# COMMAND ----------

# DBTITLE 1,Monitor Scoring Stream
# ── Wait for scoring stream to finish + verify results ──
import time

print("Waiting for scoring stream to process all available data...")
try:
    scoring_query.awaitTermination()
    print("  scoring_stream: completed")
except Exception as e:
    print(f"  scoring_stream: {e}")

print("\n✅ Scoring stream finished.\n")
try:
    scored = spark.read.format("delta").load(f"{SCORED_ROOT}/scored_transactions")
    total = scored.count()
    high = scored.where(col("risk_band") == "HIGH").count()
    med = scored.where(col("risk_band") == "MEDIUM").count()
    low = scored.where(col("risk_band") == "LOW").count()
    print(f"Scored transactions: {total:,}")
    print(f"  HIGH:   {high:,} ({high/total*100:.1f}%)")
    print(f"  MEDIUM: {med:,} ({med/total*100:.1f}%)")
    print(f"  LOW:    {low:,} ({low/total*100:.1f}%)")
except:
    print("Scored table not yet created")

try:
    alerts = spark.read.format("delta").load(f"{ALERTS_ROOT}/high_risk_alerts")
    print(f"\nReal-time alerts: {alerts.count():,}")
except:
    print("\nAlerts table not yet created")

# COMMAND ----------

