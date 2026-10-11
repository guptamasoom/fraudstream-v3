# Databricks notebook source
# DBTITLE 1,FraudStream V3 — Stream Generator
# MAGIC %md
# MAGIC # FraudStream V3 — Real-Time Stream Generator
# MAGIC
# MAGIC Continuously generates synthetic fraud events (transactions, logins, devices, chargebacks) in small batches and writes them as JSON files to the UC volume. Auto Loader in the Bronze layer picks these up in real-time.
# MAGIC
# MAGIC **Flow**: Generate batch → Write JSON files to `stream/<event_type>/` → Sleep `BATCH_INTERVAL_S` → Repeat
# MAGIC
# MAGIC **Usage**: Run this notebook first. It will generate events for `GENERATOR_DURATION_S` seconds (default 5 min). Meanwhile, run the Bronze/Silver/Scoring notebooks in separate tabs to process the stream.

# COMMAND ----------

# DBTITLE 1,Run Config
# MAGIC %run ./00_config_stream

# COMMAND ----------

# DBTITLE 1,Imports
import random, uuid, json, time
from datetime import datetime, timezone
from pyspark.sql import Row

print("Stream generator starting...")
print(f"  Stream root: {STREAM_ROOT}")
print(f"  Batch size:  {BATCH_SIZE} events / {BATCH_INTERVAL_S}s")

# COMMAND ----------

# DBTITLE 1,Event Generation Functions
# ── Event generation functions ──

def gen_transaction():
    is_fraud = random.random() < FRAUD_RATE
    is_bad = random.random() < BAD_DATA_RATE
    customer_id = f"cust_{random.randint(1, 5000):05d}"
    merchant_id = f"merc_{random.randint(1, 500):04d}"
    device_id = f"dev_{random.randint(1, 2000):05d}"
    if is_fraud:
        amount = round(random.uniform(600, 5000), 2)
        ip_country = random.choice(RISK_COUNTRIES)
        billing_country = random.choice(COUNTRIES)
    else:
        amount = round(random.uniform(10, 500), 2)
        ip_country = random.choice(COUNTRIES)
        billing_country = ip_country
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "fraud_transactions",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
        "customer_id": customer_id,
        "merchant_id": merchant_id,
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "amount": amount,
        "currency": "USD",
        "payment_method": random.choice(PAYMENT_METHODS),
        "device_id": device_id,
        "ip_country": ip_country,
        "billing_country": billing_country,
        "shipping_country": billing_country,
        "is_card_present": random.random() > 0.3,
        "auth_result": random.choice(AUTH_RESULTS),
        "is_known_fraud": is_fraud,
    }
    if is_bad:
        mode = random.choice(BAD_DATA_MODES)
        if mode == "missing_customer": event["customer_id"] = None
        elif mode == "negative_amount": event["amount"] = -abs(amount)
        elif mode == "missing_timestamp": event["event_timestamp"] = None
        elif mode == "bad_country": event["ip_country"] = "XX_INVALID"
    return event

def gen_login():
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "fraud_logins",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": f"cust_{random.randint(1, 5000):05d}",
        "device_id": f"dev_{random.randint(1, 2000):05d}",
        "ip_country": random.choice(COUNTRIES + RISK_COUNTRIES),
        "user_agent": random.choice(["Mozilla", "Chrome", "Safari", "Edge"]),
        "mfa_passed": random.random() > 0.2,
    }

def gen_device():
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "fraud_devices",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": f"cust_{random.randint(1, 5000):05d}",
        "device_id": f"dev_{random.randint(1, 2000):05d}",
        "device_type": random.choice(DEVICE_TYPES),
        "ip_country": random.choice(COUNTRIES + RISK_COUNTRIES),
        "is_new_device": random.random() < 0.15,
    }

def gen_chargeback():
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "fraud_chargebacks",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
        "customer_id": f"cust_{random.randint(1, 5000):05d}",
        "merchant_id": f"merc_{random.randint(1, 500):04d}",
        "chargeback_amount": round(random.uniform(10, 1000), 2),
        "reason_code": random.choice(CHARGEBACK_REASONS),
    }

def gen_batch(n=BATCH_SIZE):
    events = []
    for _ in range(n):
        r = random.random()
        if r < 0.70:   events.append(gen_transaction())
        elif r < 0.85: events.append(gen_login())
        elif r < 0.95: events.append(gen_device())
        else:          events.append(gen_chargeback())
    return events

print("Event generators defined.")

# COMMAND ----------

# DBTITLE 1,Batch Writer
# ── Write a batch of events as JSON files to UC volume ──
from collections import defaultdict

def write_batch(events):
    by_type = defaultdict(list)
    for e in events:
        by_type[e["event_type"]].append(e)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    total = 0
    for stream_name, stream_events in by_type.items():
        target_dir = f"{STREAM_ROOT}/{stream_name}"
        json_lines = "\n".join(json.dumps(e) for e in stream_events)
        file_path = f"{target_dir}/batch_{ts}.json"
        dbutils.fs.put(file_path, json_lines, overwrite=True)
        total += len(stream_events)
    return total

print("Batch writer defined.")

# COMMAND ----------

# DBTITLE 1,Start Real-Time Generation
# ── Start generating events in real-time ──
print(f"Starting real-time event generator...")
print(f"  Batch size: {BATCH_SIZE} events / {BATCH_INTERVAL_S}s")
print(f"  Duration: {GENERATOR_DURATION_S}s ({'infinite' if GENERATOR_DURATION_S == 0 else f'{GENERATOR_DURATION_S//60}min'})")
print(f"  Output: {STREAM_ROOT}/")
print("="*60)

start_time = time.time()
batch_num = 0
total_events = 0

try:
    while True:
        batch_num += 1
        events = gen_batch(BATCH_SIZE)
        written = write_batch(events)
        total_events += written
        elapsed = time.time() - start_time
        logger.info(f"[Generator] Batch {batch_num:4d} | {written:4d} events | Total: {total_events:>8,} | Elapsed: {elapsed:>6.0f}s")
        if GENERATOR_DURATION_S > 0 and elapsed >= GENERATOR_DURATION_S:
            print(f"\n✅ Generator finished after {elapsed:.0f}s. Total events: {total_events:,}")
            break
        time.sleep(BATCH_INTERVAL_S)
except KeyboardInterrupt:
    print(f"\n⏹️ Generator stopped. Total events: {total_events:,}")