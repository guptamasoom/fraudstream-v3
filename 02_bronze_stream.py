# Databricks notebook source
# DBTITLE 1,FraudStream V3 — Bronze Streaming
# MAGIC %md
# MAGIC # FraudStream V3 — Bronze Streaming (Auto Loader)
# MAGIC
# MAGIC Uses **Auto Loader** (`cloudFiles`) to continuously read incoming JSON files from the UC volume, applies 8 DQ checks, and writes valid events to Bronze Delta tables + invalid events to quarantine.
# MAGIC
# MAGIC **Key**: This is a Structured Streaming query that runs indefinitely. It processes new files as they arrive from the generator, with micro-batches every `TRIGGER_INTERVAL_SECONDS` seconds.
# MAGIC
# MAGIC **Usage**: Start the generator first, then run this notebook. It will run until cancelled.

# COMMAND ----------

# DBTITLE 1,Run Config
# MAGIC %run ./00_config_stream

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import col, expr, current_timestamp, to_date, lit, when
from delta.tables import DeltaTable
import os

print("Bronze streaming starting...")
print(f"  Stream root: {STREAM_ROOT}")
print(f"  Bronze root: {BRONZE_ROOT}")

# COMMAND ----------

# DBTITLE 1,Bronze Stream Builder
# ── Build a Bronze streaming query for one event stream ──
def build_bronze_stream(stream_name):
    schema = SCHEMAS[stream_name]
    source_path = f"{STREAM_ROOT}/{stream_name}"
    bronze_path = f"{BRONZE_ROOT}/{stream_name}"
    quarantine_path = f"{QUARANTINE_ROOT}/{stream_name}"
    ckpt_path = f"{BRONZE_CHECKPOINT}/{stream_name}"

    # Auto Loader: read JSON files as they arrive
    raw_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_ROOT}/schema/{stream_name}")
        .schema(schema)
        .load(source_path)
    )

    # Apply DQ checks: combine all predicates into a single boolean
    checks = DQ_CHECKS_BY_STREAM[stream_name]
    dq_expr = " AND ".join([f"({pred})" for _, _, _, pred in checks])
    bronze_df = (raw_df
        .withColumn("dq_passed", expr(dq_expr))
        .withColumn("ingested_at", current_timestamp())
        .withColumn("event_date", to_date(col("event_timestamp")))
    )

    # foreachBatch: split valid → Bronze, invalid → quarantine
    @safe_foreachbatch
    def process_batch(df, batch_id):
        valid = df.where(col("dq_passed") == True).drop("dq_passed")
        invalid = df.where(col("dq_passed") == False).drop("dq_passed")

        # Fill null event_date for records with missing timestamp
        valid = valid.withColumn("event_date", when(col("event_date").isNull(), lit("1970-01-01").cast("date")).otherwise(col("event_date")))

        # Single groupBy to get both counts (1 Spark job instead of 5)
        counts = df.groupBy("dq_passed").count().collect()
        valid_count = next((r["count"] for r in counts if r["dq_passed"]), 0)
        invalid_count = next((r["count"] for r in counts if not r["dq_passed"]), 0)

        if valid_count > 0:
            (valid.write.format("delta").mode("append")
                .partitionBy("event_date")
                .save(bronze_path))
            logger.info(f"[Bronze:{stream_name}] batch {batch_id}: {valid_count} valid → Bronze")

        if invalid_count > 0:
            # Rename collision for chargebacks
            if "reason_code" in invalid.columns:
                invalid = invalid.withColumnRenamed("reason_code", "chargeback_reason_code")
            invalid = (invalid
                .withColumn("check_name", lit("dq_failed"))
                .withColumn("reason_code", lit("DQ_CHECK_FAILED"))
                .withColumn("severity", lit("critical"))
            )
            (invalid.write.format("delta").mode("append")
                .option("mergeSchema", "true")
                .save(quarantine_path))
            logger.warning(f"[Bronze:{stream_name}] batch {batch_id}: {invalid_count} invalid → Quarantine")

    # Start the streaming query
    query = (bronze_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", ckpt_path)
        .trigger(processingTime='10s')
        .queryName(f"bronze_{stream_name}")
        .start()
    )
    print(f"  Started: bronze_{stream_name}")
    return query

print("Bronze stream builder defined.")

# COMMAND ----------

# DBTITLE 1,Start All Bronze Streams
# ── Start Bronze streaming queries for all event streams ──
print("Starting Bronze Auto Loader streams for all event types...")
print("="*60)

bronze_queries = {}
for stream_name in STREAMS:
    bronze_queries[stream_name] = build_bronze_stream(stream_name)

print(f"\n✅ {len(bronze_queries)} Bronze streams started.")
print("   Waiting for events from the generator...")
print("   Run 01_stream_generator in another tab to start producing events.")
print("   These streams will run until cancelled.")

# COMMAND ----------

# DBTITLE 1,Monitor Bronze Streams
# ── Wait for Bronze streams to finish + verify results ──
import time

print("Waiting for Bronze streams to process all available data...")
for stream_name, q in bronze_queries.items():
    try:
        q.awaitTermination()
        print(f"  bronze_{stream_name}: completed")
    except Exception as e:
        print(f"  bronze_{stream_name}: {e}")

print("\n✅ All Bronze streams finished.\n")
print("Bronze table counts:")
for stream_name in STREAMS:
    try:
        cnt = spark.read.format("delta").load(f"{BRONZE_ROOT}/{stream_name}").count()
        print(f"  Bronze.{stream_name}: {cnt:,} rows")
    except:
        print(f"  Bronze.{stream_name}: not yet created")

print("\nQuarantine counts:")
for stream_name in STREAMS:
    try:
        cnt = spark.read.format("delta").load(f"{QUARANTINE_ROOT}/{stream_name}").count()
        print(f"  Quarantine.{stream_name}: {cnt} rows")
    except:
        print(f"  Quarantine.{stream_name}: 0 rows")