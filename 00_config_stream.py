# Databricks notebook source
# DBTITLE 1,FraudStream V3 — Streaming Config
# MAGIC %md
# MAGIC # FraudStream V3 — Streaming Configuration
# MAGIC
# MAGIC Shared config for the **real-time streaming** FraudStream pipeline using Structured Streaming + Auto Loader.
# MAGIC
# MAGIC **Architecture**: Stream Generator → Auto Loader Bronze → Silver Stream → Scoring Stream → Gold Stream → Validation
# MAGIC
# MAGIC **Key difference from V2**: Events are processed as they arrive (micro-batches every 10s), not in scheduled batches. Velocity features and fraud rules are applied in real-time via `foreachBatch`.

# COMMAND ----------

# DBTITLE 1,Imports & Data Paths
from pyspark.sql.types import (
    BooleanType, DecimalType, DoubleType, StringType, StructField, StructType, TimestampType,
)
from pyspark.sql.functions import col, current_timestamp
import os, json

# ── UC Volume root ──
DATA_ROOT = "/Volumes/masoom_central_india_dev/default/fraudstream_v3_data"

# ── Layer paths ──
STREAM_ROOT      = f"{DATA_ROOT}/stream"        # Auto Loader source (incoming JSON)
BRONZE_ROOT     = f"{DATA_ROOT}/bronze"
SILVER_ROOT     = f"{DATA_ROOT}/silver"
SCORED_ROOT     = f"{DATA_ROOT}/scored"
ALERTS_ROOT     = f"{DATA_ROOT}/alerts"
QUARANTINE_ROOT = f"{DATA_ROOT}/quarantine"
GOLD_ROOT       = f"{DATA_ROOT}/gold"
DQ_ROOT         = f"{DATA_ROOT}/dq_scorecard"
CHECKPOINT_ROOT = f"{DATA_ROOT}/checkpoints"

# ── Stream names ──
STREAMS = ["fraud_transactions", "fraud_logins", "fraud_devices", "fraud_chargebacks"]

# ── Display helper ──
def show_df(df, n=5, label=""):
    if label:
        print(f"\n{'='*60}\n{label}\n{'='*60}")
    df.show(n, truncate=False)

# ── Structured logging ──
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("FraudStreamV3")

# ── Error handling wrapper for foreachBatch functions ──
def safe_foreachbatch(fn):
    """Wraps foreachBatch functions with structured error logging."""
    def wrapper(df, batch_id):
        try:
            fn(df, batch_id)
        except Exception as e:
            logger.error(f"Batch {batch_id} FAILED in {fn.__name__}: {e}")
            raise
    return wrapper

# COMMAND ----------

# DBTITLE 1,Event Schemas
# ── StructType schemas for each event stream ──

TX_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_category", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("payment_method", StringType()),
    StructField("device_id", StringType()),
    StructField("ip_country", StringType()),
    StructField("billing_country", StringType()),
    StructField("shipping_country", StringType()),
    StructField("is_card_present", BooleanType()),
    StructField("auth_result", StringType()),
    StructField("is_known_fraud", BooleanType()),
])

LOGIN_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("customer_id", StringType()),
    StructField("device_id", StringType()),
    StructField("ip_country", StringType()),
    StructField("user_agent", StringType()),
    StructField("mfa_passed", BooleanType()),
])

DEVICE_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("customer_id", StringType()),
    StructField("device_id", StringType()),
    StructField("device_type", StringType()),
    StructField("ip_country", StringType()),
    StructField("is_new_device", BooleanType()),
])

CHARGEBACK_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("chargeback_amount", DoubleType()),
    StructField("reason_code", StringType()),
])

SCHEMAS = {
    "fraud_transactions": TX_SCHEMA,
    "fraud_logins": LOGIN_SCHEMA,
    "fraud_devices": DEVICE_SCHEMA,
    "fraud_chargebacks": CHARGEBACK_SCHEMA,
}

print("Schemas defined for:", list(SCHEMAS.keys()))

# COMMAND ----------

# DBTITLE 1,Generation Constants & Fraud Rules
# ── Constants for synthetic data generation ──

COUNTRIES = ["IN", "US", "GB", "SG", "AE", "DE", "BR"]
RISK_COUNTRIES = ["RU", "NG", "BR", "AE"]
MERCHANT_CATEGORIES = ["grocery", "electronics", "travel", "gaming", "fashion", "fuel", "wallet"]
PAYMENT_METHODS = ["card_present", "card_not_present", "upi", "wallet", "netbanking"]
DEVICE_TYPES = ["ios", "android", "web", "tablet"]
AUTH_RESULTS = ["approved", "approved", "approved", "declined"]
LOGIN_TYPES = ["login_success", "login_failed", "password_reset"]
DEVICE_EVENT_TYPES = ["device_seen", "new_device", "device_blocked"]
CHARGEBACK_REASONS = ["fraud", "product_not_received", "duplicate", "customer_dispute"]
BAD_DATA_MODES = ["missing_customer", "negative_amount", "missing_timestamp", "bad_country"]

FRAUD_RATE = 0.08
BAD_DATA_RATE = 0.04

# ── Fraud rule weights ──
RULE_WEIGHTS = {
    "rule_high_amount":              20,
    "rule_country_mismatch":         20,
    "rule_new_device_high_amount":  20,
    "rule_high_velocity":           15,
    "rule_many_customers_on_device": 10,
    "rule_failed_login_before_purchase": 10,
    "rule_card_not_present_risk":   15,
}

# ── Risk band thresholds ──
HIGH_AMOUNT_THRESHOLD = 600.0
HIGH_VELOCITY_THRESHOLD = 4
MANY_CUSTOMERS_THRESHOLD = 3
FAILED_LOGIN_THRESHOLD = 2

print("Generation constants loaded.")

# COMMAND ----------

# DBTITLE 1,DQ Check Definitions
# ── Data Quality checks ──

DQ_CHECKS_BASE = [
    ("event_id_present",       "MISSING_EVENT_ID",       "critical", "event_id IS NOT NULL AND length(event_id) > 0"),
    ("event_timestamp_present","MISSING_EVENT_TIMESTAMP","critical", "event_timestamp IS NOT NULL AND length(event_timestamp) > 0"),
    ("customer_id_present",    "MISSING_CUSTOMER_ID",   "critical", "customer_id IS NOT NULL AND length(customer_id) > 0"),
]

DQ_CHECKS_TX = DQ_CHECKS_BASE + [
    ("transaction_id_present", "MISSING_TRANSACTION_ID", "critical", "transaction_id IS NOT NULL AND length(transaction_id) > 0"),
    ("amount_non_negative",    "NEGATIVE_AMOUNT",        "critical", "amount IS NOT NULL AND amount >= 0"),
    ("valid_ip_country",      "INVALID_IP_COUNTRY",     "warning",  "ip_country IS NOT NULL AND rlike(ip_country, '^[A-Z]{2}$')"),
    ("valid_billing_country", "INVALID_BILLING_COUNTRY","warning",  "billing_country IS NOT NULL AND rlike(billing_country, '^[A-Z]{2}$')"),
]

DQ_CHECKS_CB = DQ_CHECKS_BASE + [
    ("chargeback_amount_non_negative", "NEGATIVE_CHARGEBACK", "critical", "chargeback_amount IS NOT NULL AND chargeback_amount >= 0"),
]

DQ_CHECKS_LOGIN = DQ_CHECKS_BASE
DQ_CHECKS_DEVICE = DQ_CHECKS_BASE

DQ_CHECKS_BY_STREAM = {
    "fraud_transactions": DQ_CHECKS_TX,
    "fraud_chargebacks":  DQ_CHECKS_CB,
    "fraud_logins":       DQ_CHECKS_LOGIN,
    "fraud_devices":      DQ_CHECKS_DEVICE,
}

print(f"DQ checks defined for {len(DQ_CHECKS_BY_STREAM)} streams")
for s, checks in DQ_CHECKS_BY_STREAM.items():
    print(f"  {s}: {len(checks)} checks")

# COMMAND ----------

# DBTITLE 1,Streaming Config
# ── Streaming configuration ──
TRIGGER_INTERVAL_SECONDS = 10
WATERMARK_DELAY          = "10 minutes"

# ── Structured Streaming checkpoint locations ──
BRONZE_CHECKPOINT  = f"{CHECKPOINT_ROOT}/bronze"
SILVER_CHECKPOINT  = f"{CHECKPOINT_ROOT}/silver"
SCORING_CHECKPOINT = f"{CHECKPOINT_ROOT}/scoring"
GOLD_CHECKPOINT    = f"{CHECKPOINT_ROOT}/gold"

# ── Generator settings ──
BATCH_SIZE           = 200
BATCH_INTERVAL_S     = 5
GENERATOR_DURATION_S = 0

print("Streaming config loaded.")
print(f"  Trigger interval:  {TRIGGER_INTERVAL_SECONDS}s")
print(f"  Watermark delay:   {WATERMARK_DELAY}")
print(f"  Generator batch:   {BATCH_SIZE} events / {BATCH_INTERVAL_S}s")

# ── Structured logging setup ──
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("FraudStreamV3")
logger.info("Structured logging initialized for FraudStream V3")

# COMMAND ----------

# DBTITLE 1,Smoke Test
# ── Smoke test: verify paths ──
for layer, path in [("STREAM", STREAM_ROOT), ("BRONZE", BRONZE_ROOT), ("SILVER", SILVER_ROOT),
                     ("SCORED", SCORED_ROOT), ("ALERTS", ALERTS_ROOT), ("QUARANTINE", QUARANTINE_ROOT),
                     ("GOLD", GOLD_ROOT), ("DQ", DQ_ROOT), ("CHECKPOINT", CHECKPOINT_ROOT)]:
    print(f"  {layer:12s} → {path}")
print("\n✅ FraudStream V3 config loaded successfully.")
print("   Run other notebooks with %run ./00_config_stream")