# Databricks notebook source
# DBTITLE 1,FraudStream V3 — Silver Streaming
# MAGIC %md
# MAGIC # FraudStream V3 — Silver Streaming
# MAGIC
# MAGIC Reads Bronze Delta tables as a stream, normalizes data types, adds `event_date`, deduplicates by `event_id` (using watermark + dropDuplicates), and writes clean Silver Delta tables.
# MAGIC
# MAGIC **Key**: Uses `spark.readStream.format("delta")` to read Bronze incrementally. Deduplication uses Structured Streaming's `dropDuplicates` with watermark for exactly-once semantics.
# MAGIC
# MAGIC **Usage**: Start after Bronze streaming is running. This too runs indefinitely.

# COMMAND ----------

# DBTITLE 1,Run Config
# MAGIC %run ./00_config_stream

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import col, current_timestamp, to_date, expr

print("Silver streaming starting...")
print(f"  Bronze root: {BRONZE_ROOT}")
print(f"  Silver root: {SILVER_ROOT}")

# COMMAND ----------

# DBTITLE 1,Silver Stream Builder
# ── Build a Silver streaming query for one event stream ──
def build_silver_stream(stream_name):
    bronze_path = f"{BRONZE_ROOT}/{stream_name}"
    silver_path = f"{SILVER_ROOT}/{stream_name}"
    ckpt_path = f"{SILVER_CHECKPOINT}/{stream_name}"

    # Read Bronze Delta as a stream (only valid records — Bronze already filtered)
    bronze_df = (spark.readStream
        .format("delta")
        .load(bronze_path)
    )

    # Cast timestamp + deduplicate by event_id using watermark
    silver_df = (bronze_df
        .withColumn("event_ts", col("event_timestamp").cast("timestamp"))
        .withWatermark("event_ts", WATERMARK_DELAY)
        .dropDuplicates(["event_id"])
        .withColumn("event_date", to_date(col("event_ts")))
        .drop("event_ts")
        .withColumn("processed_at", current_timestamp())
    )

    # Write to Silver Delta via foreachBatch (supports partitionBy)
    @safe_foreachbatch
    def write_silver_batch(df, batch_id):
        try:
            (df.write.format("delta").mode("append")
                .partitionBy("event_date")
                .save(silver_path))
            logger.info(f"[Silver:{stream_name}] batch {batch_id}: written to Silver")
        except Exception as e:
            logger.error(f"[Silver:{stream_name}] batch {batch_id}: error - {e}")

    query = (silver_df.writeStream
        .foreachBatch(write_silver_batch)
        .option("checkpointLocation", ckpt_path)
        .trigger(processingTime='10s')
        .queryName(f"silver_{stream_name}")
        .start()
    )
    print(f"  Started: silver_{stream_name}")
    return query

print("Silver stream builder defined.")

# COMMAND ----------

# DBTITLE 1,Start All Silver Streams
# ── Start Silver streaming queries for all event streams ──
print("Starting Silver streams for all event types...")
print("="*60)

silver_queries = {}
for stream_name in STREAMS:
    silver_queries[stream_name] = build_silver_stream(stream_name)

print(f"\n✅ {len(silver_queries)} Silver streams started.")
print("   These read from Bronze Delta tables and write to Silver Delta.")
print("   Ensure 02_bronze_stream is running to produce Bronze data.")

# COMMAND ----------

# DBTITLE 1,Monitor Silver Streams
# ── Wait for Silver streams to finish + verify results ──
import time

print("Waiting for Silver streams to process all available data...")
for stream_name, q in silver_queries.items():
    try:
        q.awaitTermination()
        print(f"  silver_{stream_name}: completed")
    except Exception as e:
        print(f"  silver_{stream_name}: {e}")

print("\n✅ All Silver streams finished.\n")
print("Silver table counts:")
for stream_name in STREAMS:
    try:
        cnt = spark.read.format("delta").load(f"{SILVER_ROOT}/{stream_name}").count()
        distinct = spark.read.format("delta").load(f"{SILVER_ROOT}/{stream_name}").select("event_id").distinct().count()
        print(f"  Silver.{stream_name}: {cnt:,} rows ({distinct:,} distinct event_ids)")
    except:
        print(f"  Silver.{stream_name}: not yet created")