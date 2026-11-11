"""
generate_upi_1b.py

Distributed synthetic UPI/eFRM generator.

Designed for Spark 3.x. It generates rows lazily with spark.range()
and writes partitioned Parquet. It does NOT create one-billion-row
Python objects in driver memory.

Example:
  spark-submit \
      --master yarn \
      --conf spark.sql.shuffle.partitions=4000 \
      generate_upi_1b.py \
      --rows 1000000000 \
      --output /data/upi_1b \
      --partitions 4000 \
      --seed 42

Local smoke test:
  spark-submit generate_upi_1b.py --rows 100000 --output ./upi_test --partitions 16
"""

import argparse
from pyspark.sql import SparkSession, functions as F
from master_data_big import (
    APPS, APP_NAMES, OS_LIST, ANDROID_VERSIONS, IOS_VERSIONS,
    DEVICE_TYPES, DEVICE_BRANDS, DEVICE_MODELS, BANKS, HANDLES, PSPS,
    PAYMENT_CHANNELS, TRANSACTION_TYPES, PAYMENT_FLOWS, INTEGRATION_MODES,
    PURPOSE_CODES, INITIATION_MODES, CURRENCIES, COUNTRIES,
    TELECOM_OPERATORS, NETWORK_TYPES, ZONES, STATES, CITIES,
    CUSTOMER_TYPES, ACCOUNT_TYPES, MERCHANT_TYPES, MERCHANT_SIZES,
    MCC_CODES, AUTH_METHODS, AUTH_STATUSES, PROCESSING_STATUSES,
    DECISIONS, RISK_LEVELS, FRAUD_TYPES, SERVER_REGIONS,
    SERVICE_NAMES, PROCESSOR_TYPES, UPI_EFRM_SCHEMA
)


def lit_array(values):
    return F.array(*[F.lit(x) for x in values])


def weighted_choice(rand_col, choices):
    """
    choices = [(upper_bound_exclusive, value), ...]
    Example probabilities: 0.70, 0.20, 0.08, 0.02
    """
    expr = None
    for upper, value in choices:
        condition = rand_col < F.lit(upper)
        expr = F.when(condition, F.lit(value)) if expr is None else expr.when(condition, F.lit(value))
    return expr.otherwise(F.lit(choices[-1][1]))


def indexed_choice(rand_col, values):
    idx = F.floor(rand_col * F.lit(len(values))).cast("int") + F.lit(1)
    return F.element_at(lit_array(values), idx)


def synthetic_ip(id_col):
    # Documentation/private-style synthetic ranges, not real customer IPs.
    a = F.pmod(id_col, F.lit(240)).cast("long") + 10
    b = F.pmod(F.floor(id_col / 240), F.lit(250)).cast("long")
    c = F.pmod(F.floor(id_col / (240 * 250)), F.lit(250)).cast("long")
    d = F.pmod(F.floor(id_col / (240 * 250 * 250)), F.lit(250)).cast("long")
    return F.concat_ws(".", a, b, c, d)


def bank_columns(prefix, rand_col):
    codes = [x[0] for x in BANKS]
    names = [x[1] for x in BANKS]
    idx = F.floor(rand_col * len(BANKS)).cast("int")
    return (
        F.element_at(lit_array(codes), idx + 1).alias(prefix + "_bank_code"),
        F.element_at(lit_array(names), idx + 1).alias(prefix + "_bank_name")
    )


def generate(args):
    spark = (
        SparkSession.builder
        .appName("Synthetic-UPI-eFRM-1B")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", args.compression)
        .getOrCreate()
    )

    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions or args.partitions))

    # spark.range is distributed/lazy; no billion-row object is held by the driver.
    df = spark.range(
        start=0,
        end=args.rows,
        step=1,
        numPartitions=args.partitions
    ).withColumnRenamed("id", "_row_id")

    r = F.rand(args.seed)
    r2 = F.rand(args.seed + 1)
    r3 = F.rand(args.seed + 2)
    r4 = F.rand(args.seed + 3)
    r5 = F.rand(args.seed + 4)
    r6 = F.rand(args.seed + 5)
    r7 = F.rand(args.seed + 6)
    r8 = F.rand(args.seed + 7)
    r9 = F.rand(args.seed + 8)
    r10 = F.rand(args.seed + 9)

    # ------------------------------------------------------------------
    # Time: clustered over a configurable date range.
    # ------------------------------------------------------------------
    start_ts = args.start_date + " 00:00:00"
    seconds = F.floor(F.rand(args.seed + 10) * F.lit(args.days * 86400)).cast("long")
    df = df.withColumn("txn_timestamp", F.expr(f"timestamp'{start_ts}'") + F.expr("INTERVAL 1 SECOND") * seconds)
    df = (
        df.withColumn("txn_date", F.to_date("txn_timestamp").cast("string"))
          .withColumn("txn_year", F.year("txn_timestamp"))
          .withColumn("txn_month", F.month("txn_timestamp"))
          .withColumn("txn_day", F.dayofmonth("txn_timestamp"))
          .withColumn("txn_hour", F.hour("txn_timestamp"))
          .withColumn("txn_minute", F.minute("txn_timestamp"))
          .withColumn("txn_second", F.second("txn_timestamp"))
          .withColumn("day_of_week", F.dayofweek("txn_timestamp"))
          .withColumn("week_of_year", F.weekofyear("txn_timestamp"))
          .withColumn("quarter", F.quarter("txn_timestamp"))
          .withColumn("is_weekend", F.dayofweek("txn_timestamp").isin([1, 7]))
          .withColumn("is_holiday", (F.rand(args.seed + 11) < 0.035))
          .withColumn("is_business_hour",
                      (F.hour("txn_timestamp").between(9, 18)) &
                      (~F.dayofweek("txn_timestamp").isin([1, 7])))
          .withColumn(
              "time_period",
              F.when(F.hour("txn_timestamp").between(5, 8), "EARLY_MORNING")
               .when(F.hour("txn_timestamp").between(9, 12), "MORNING")
               .when(F.hour("txn_timestamp").between(13, 16), "AFTERNOON")
               .when(F.hour("txn_timestamp").between(17, 20), "EVENING")
               .when(F.hour("txn_timestamp").between(21, 23), "NIGHT")
               .otherwise("LATE_NIGHT")
          )
          .withColumn("festival_flag", F.rand(args.seed + 12) < 0.06)
          .withColumn("salary_day_flag",
                      F.dayofmonth("txn_timestamp").isin([1, 2, 3, 5, 28, 29, 30, 31]))
          .withColumn("month_start_flag", F.dayofmonth("txn_timestamp") <= 3)
          .withColumn("month_end_flag", F.dayofmonth("txn_timestamp") >= 28)
    )

    # ------------------------------------------------------------------
    # IDs and customer/payee relationships.
    # Deterministic IDs mean repeatable joins across generated datasets.
    # ------------------------------------------------------------------
    idc = F.col("_row_id")
    df = (
        df.withColumn("txn_id", F.concat(F.lit("TXN"), F.lpad(idc.cast("string"), 14, "0")))
          .withColumn("txn_ref_id", F.concat(F.lit("REF"), F.lpad((idc * 13 + 7).cast("string"), 14, "0")))
          .withColumn("org_txn_id", F.concat(F.lit("ORG"), F.lpad((idc * 17 + 11).cast("string"), 14, "0")))
          .withColumn("parent_txn_id", F.when(r < 0.04, F.concat(F.lit("TXN"), F.lpad((F.pmod(idc + 1, F.lit(args.rows))).cast("string"), 14, "0"))))
          .withColumn("rrn", F.lpad(F.pmod(idc * 7919 + 12345, F.lit(10**12)).cast("string"), 12, "0"))
          .withColumn("message_id", F.concat(F.lit("MSG"), F.lpad((idc * 31).cast("string"), 16, "0")))
          .withColumn("request_id", F.concat(F.lit("REQ"), F.lpad((idc * 37).cast("string"), 16, "0")))
          .withColumn("session_id", F.concat(F.lit("SES"), F.lpad(F.floor(idc / 3).cast("string"), 14, "0")))
          .withColumn("customer_ref_id", F.concat(F.lit("CUS"), F.lpad(F.pmod(idc, F.lit(args.customers)).cast("string"), 10, "0")))
          .withColumn("merchant_ref_id", F.concat(F.lit("MER"), F.lpad(F.pmod(idc * 7, F.lit(args.merchants)).cast("string"), 9, "0")))
          .withColumn("payer_id", F.concat(F.lit("P"), F.lpad(F.pmod(idc * 11, F.lit(args.customers)).cast("string"), 10, "0")))
          .withColumn("payee_id", F.concat(F.lit("B"), F.lpad(F.pmod(idc * 17 + 3, F.lit(args.customers)).cast("string"), 10, "0")))
    )

    df = (
        df.withColumn("payer_vpa", F.concat(F.lower(F.col("payer_id")), indexed_choice(r2, HANDLES)))
          .withColumn("payee_vpa", F.concat(F.lower(F.col("payee_id")), indexed_choice(r3, HANDLES)))
          .withColumn("payer_mobile_hash", F.sha2(F.concat(F.lit("mobile-"), F.col("payer_id")), 256))
          .withColumn("payer_account_type", indexed_choice(r4, ACCOUNT_TYPES))
          .withColumn("payer_customer_type", weighted_choice(r5, [
              (0.94, "INDIVIDUAL"), (0.975, "SMALL_BUSINESS"),
              (0.995, "MERCHANT"), (1.0, "CORPORATE")
          ]))
          .withColumn("payee_account_type", indexed_choice(r6, ACCOUNT_TYPES))
          .withColumn("payee_customer_type", weighted_choice(r7, [
              (0.70, "INDIVIDUAL"), (0.96, "MERCHANT"),
              (0.99, "SMALL_BUSINESS"), (1.0, "CORPORATE")
          ]))
    )

    payer_bank_code, payer_bank_name = bank_columns("payer", r8)
    payee_bank_code, payee_bank_name = bank_columns("payee", r9)
    df = df.select("*", payer_bank_code, payer_bank_name, payee_bank_code, payee_bank_name)

    # Synthetic IFSCs: deliberately recognizable test values, not presented as live branch records.
    df = (
        df.withColumn("payer_ifsc", F.concat(F.col("payer_bank_code"), F.lit("000"), F.lpad(F.pmod(idc, 9999).cast("string"), 4, "0")))
          .withColumn("payee_ifsc", F.concat(F.col("payee_bank_code"), F.lit("000"), F.lpad(F.pmod(idc * 3, 9999).cast("string"), 4, "0")))
          .withColumn("payer_psp", indexed_choice(r, PSPS))
          .withColumn("payee_psp", indexed_choice(r2, PSPS))
    )

    # ------------------------------------------------------------------
    # Merchant attributes: mostly null for P2P, populated for P2M.
    # ------------------------------------------------------------------
    is_p2m = F.col("transaction_type").isin(["P2M"]) if "transaction_type" in df.columns else (r3 < 0.60)
    df = df.withColumn("_is_p2m", is_p2m)
    df = (
        df.withColumn("merchant_id", F.when(F.col("_is_p2m"), F.col("merchant_ref_id")))
          .withColumn("merchant_vpa", F.when(F.col("_is_p2m"), F.lower(F.concat(F.col("merchant_id"), indexed_choice(r4, HANDLES)))))
          .withColumn("merchant_type", F.when(F.col("_is_p2m"), indexed_choice(r5, MERCHANT_TYPES)))
          .withColumn("merchant_size", F.when(F.col("_is_p2m"), weighted_choice(r6, [
              (0.55, "MICRO"), (0.82, "SMALL"), (0.96, "MEDIUM"),
              (0.995, "LARGE"), (1.0, "ENTERPRISE")
          ])))
          .withColumn("merchant_mcc", F.when(F.col("_is_p2m"), indexed_choice(r7, MCC_CODES)))
          .withColumn("merchant_state", F.when(F.col("_is_p2m"), indexed_choice(r8, STATES)))
          .withColumn("merchant_city", F.when(F.col("_is_p2m"), indexed_choice(r9, CITIES)))
          .withColumn("merchant_pincode", F.when(F.col("_is_p2m"), F.lpad(F.pmod(idc * 19, 900000) + 100000, 6, "0")))
          .withColumn("merchant_account_age_days", F.when(F.col("_is_p2m"), (F.rand(args.seed + 20) * 2500 + 30).cast("int")))
    )

    # ------------------------------------------------------------------
    # Transaction semantics and amount distribution.
    # ------------------------------------------------------------------
    df = (
        df.withColumn("transaction_type",
                      weighted_choice(F.rand(args.seed + 21), [
                          (0.62, "P2P"), (0.90, "P2M"), (0.94, "P2A"),
                          (0.975, "COLLECT"), (0.99, "MANDATE"),
                          (0.997, "REFUND"), (1.0, "REVERSAL")
                      ]))
          .withColumn("payment_flow",
                      weighted_choice(F.rand(args.seed + 22), [
                          (0.82, "PAY"), (0.90, "COLLECT"), (0.95, "MANDATE"),
                          (0.98, "REFUND"), (1.0, "REVERSAL")
                      ]))
          .withColumn("payment_channel",
                      weighted_choice(F.rand(args.seed + 23), [
                          (0.45, "UPI_APP"), (0.72, "QR"), (0.84, "INTENT"),
                          (0.91, "COLLECT"), (0.97, "IN_APP"), (0.985, "BANK_APP"),
                          (1.0, "WEB")
                      ]))
          .withColumn("integration_mode",
                      weighted_choice(F.rand(args.seed + 24), [
                          (0.45, "QR"), (0.72, "INTENT"),
                          (0.90, "APP"), (1.0, "COLLECT")
                      ]))
          .withColumn("purpose_code", indexed_choice(F.rand(args.seed + 25), PURPOSE_CODES))
          .withColumn("initiation_mode", indexed_choice(F.rand(args.seed + 26), INITIATION_MODES))
    )

    # Log-normal-ish amount: heavily concentrated at small values, with a long tail.
    raw_amount = F.exp(F.randn(args.seed + 27) * 1.15 + F.lit(5.0))
    amount = F.round(F.greatest(F.lit(1.0), F.least(raw_amount, F.lit(200000.0))), 2)

    df = (
        df.withColumn("amount", amount)
          .withColumn("requested_amount", F.col("amount"))
          .withColumn("settled_amount",
                      F.when(F.rand(args.seed + 28) < 0.985, F.col("amount"))
                       .otherwise(F.lit(0.0)))
          .withColumn("refund_amount",
                      F.when(F.rand(args.seed + 29) < 0.012, F.col("amount")).otherwise(F.lit(0.0)))
          .withColumn("fee_amount", F.round(F.col("amount") * F.lit(0.0015), 2))
          .withColumn("tax_amount", F.round(F.col("fee_amount") * F.lit(0.18), 2))
          .withColumn("discount_amount", F.when(F.rand(args.seed + 30) < 0.08, F.round(F.col("amount") * 0.01, 2)).otherwise(F.lit(0.0)))
          .withColumn("cashback_amount", F.when(F.rand(args.seed + 31) < 0.04, F.round(F.col("amount") * 0.005, 2)).otherwise(F.lit(0.0)))
          .withColumn("currency", F.lit("INR"))
    )

    # ------------------------------------------------------------------
    # App/device/network.
    # ------------------------------------------------------------------
    df = (
        df.withColumn("app", indexed_choice(F.rand(args.seed + 32), APPS))
          .withColumn("app_name", indexed_choice(F.rand(args.seed + 33), APP_NAMES))
          .withColumn("app_version", F.concat(
              (F.floor(F.rand(args.seed + 34) * 8) + 5).cast("string"),
              F.lit("."), (F.floor(F.rand(args.seed + 35) * 10)).cast("string"),
              F.lit("."), (F.floor(F.rand(args.seed + 36) * 20)).cast("string")
          ))
          .withColumn("device_id", F.sha2(F.concat(F.lit("device-"), F.pmod(idc, F.lit(args.devices)).cast("string")), 256))
          .withColumn("device_fingerprint", F.sha2(F.concat(F.lit("fp-"), F.floor(idc / 2).cast("string")), 256))
          .withColumn("device_type", weighted_choice(F.rand(args.seed + 37), [
              (0.97, "ANDROID_PHONE"), (0.995, "IOS_PHONE"),
              (0.998, "ANDROID_TABLET"), (1.0, "IOS_TABLET")
          ]))
          .withColumn("device_brand", indexed_choice(F.rand(args.seed + 38), DEVICE_BRANDS))
          .withColumn("device_model", indexed_choice(F.rand(args.seed + 39), DEVICE_MODELS))
          .withColumn("device_os", F.when(F.col("device_type").isin(["IOS_PHONE", "IOS_TABLET"]), "iOS").otherwise("Android"))
          .withColumn("device_os_version",
                      F.when(F.col("device_type").isin(["IOS_PHONE", "IOS_TABLET"]),
                             indexed_choice(F.rand(args.seed + 40), IOS_VERSIONS))
                       .otherwise(indexed_choice(F.rand(args.seed + 41), ANDROID_VERSIONS)))
          .withColumn("device_language", indexed_choice(F.rand(args.seed + 42), LANGUAGES))
          .withColumn("device_timezone", F.lit("Asia/Kolkata"))
          .withColumn("device_rooted", F.rand(args.seed + 43) < 0.003)
          .withColumn("device_emulator", F.rand(args.seed + 44) < 0.001)
          .withColumn("device_first_seen_days_ago", (F.rand(args.seed + 45) * 2500).cast("int"))
          .withColumn("device_last_seen_minutes_ago", (F.rand(args.seed + 46) * 10080).cast("int"))
          .withColumn("mobile_operator", indexed_choice(F.rand(args.seed + 47), TELECOM_OPERATORS))
          .withColumn("network_type", weighted_choice(F.rand(args.seed + 48), [
              (0.47, "4G"), (0.86, "5G"), (0.995, "WIFI"), (1.0, "3G")
          ]))
          .withColumn("ip_address", synthetic_ip(idc))
          .withColumn("ip_country", F.lit("IN"))
          .withColumn("ip_region", indexed_choice(F.rand(args.seed + 49), STATES))
          .withColumn("ip_city", indexed_choice(F.rand(args.seed + 50), CITIES))
          .withColumn("ip_risk_score", F.round(F.rand(args.seed + 51) * 100, 2))
          .withColumn("vpn_flag", F.rand(args.seed + 52) < 0.008)
          .withColumn("proxy_flag", F.rand(args.seed + 53) < 0.004)
    )

    # ------------------------------------------------------------------
    # Geography. Payer/payee are usually same country; location mismatch
    # is deliberately correlated with higher risk later.
    # ------------------------------------------------------------------
    df = (
        df.withColumn("payer_country", F.lit("IN"))
          .withColumn("payer_state", indexed_choice(F.rand(args.seed + 54), STATES))
          .withColumn("payer_city", indexed_choice(F.rand(args.seed + 55), CITIES))
          .withColumn("payer_pincode", F.lpad(F.pmod(idc * 23, 900000) + 100000, 6, "0"))
          .withColumn("payee_country", F.lit("IN"))
          .withColumn("payee_state", indexed_choice(F.rand(args.seed + 56), STATES))
          .withColumn("payee_city", indexed_choice(F.rand(args.seed + 57), CITIES))
          .withColumn("payee_pincode", F.lpad(F.pmod(idc * 29, 900000) + 100000, 6, "0"))
          .withColumn("payer_latitude", F.round(F.rand(args.seed + 58) * 20 + 8, 6))
          .withColumn("payer_longitude", F.round(F.rand(args.seed + 59) * 27 + 68, 6))
          .withColumn("payee_latitude", F.round(F.rand(args.seed + 60) * 20 + 8, 6))
          .withColumn("payee_longitude", F.round(F.rand(args.seed + 61) * 27 + 68, 6))
          .withColumn("geo_distance_km",
                      F.round(F.sqrt(
                          F.pow(F.col("payer_latitude") - F.col("payee_latitude"), 2) +
                          F.pow(F.col("payer_longitude") - F.col("payee_longitude"), 2)
                      ) * 111, 2))
          .withColumn("location_mismatch", F.rand(args.seed + 62) < 0.055)
    )

    # ------------------------------------------------------------------
    # Authentication.
    # ------------------------------------------------------------------
    df = (
        df.withColumn("authentication_method", indexed_choice(F.rand(args.seed + 63), AUTH_METHODS))
          .withColumn("authentication_status",
                      weighted_choice(F.rand(args.seed + 64), [
                          (0.965, "SUCCESS"), (0.992, "FAILED"), (1.0, "NOT_REQUIRED")
                      ]))
          .withColumn("authentication_attempt_count", (F.rand(args.seed + 65) * 3 + 1).cast("int"))
          .withColumn("authentication_failure_count", (F.rand(args.seed + 66) * 2).cast("int"))
          .withColumn("pin_retry_count", (F.rand(args.seed + 67) * 3).cast("int"))
          .withColumn("device_binding_status", weighted_choice(F.rand(args.seed + 68), [
              (0.985, "BOUND"), (0.998, "REBOUND"), (1.0, "FAILED")
          ]))
          .withColumn("mobile_binding_status", weighted_choice(F.rand(args.seed + 69), [
              (0.99, "BOUND"), (1.0, "FAILED")
          ]))
    )

    # ------------------------------------------------------------------
    # Velocity and account-history features.
    # These are synthetic but correlated with transaction activity.
    # ------------------------------------------------------------------
    base_velocity = F.when(F.rand(args.seed + 70) < 0.975, F.floor(F.rand(args.seed + 71) * 4)).otherwise(F.floor(F.rand(args.seed + 72) * 30 + 10))
    df = (
        df.withColumn("txn_count_1m", base_velocity.cast("int"))
          .withColumn("txn_count_5m", (F.col("txn_count_1m") + F.floor(F.rand(args.seed + 73) * 8)).cast("int"))
          .withColumn("txn_count_15m", (F.col("txn_count_5m") + F.floor(F.rand(args.seed + 74) * 12)).cast("int"))
          .withColumn("txn_count_30m", (F.col("txn_count_15m") + F.floor(F.rand(args.seed + 75) * 18)).cast("int"))
          .withColumn("txn_count_1h", (F.col("txn_count_30m") + F.floor(F.rand(args.seed + 76) * 25)).cast("int"))
          .withColumn("txn_count_6h", (F.col("txn_count_1h") + F.floor(F.rand(args.seed + 77) * 100)).cast("int"))
          .withColumn("txn_count_24h", (F.col("txn_count_6h") + F.floor(F.rand(args.seed + 78) * 250)).cast("int"))
          .withColumn("amount_1h", F.round(F.col("amount") * F.greatest(F.lit(1.0), F.col("txn_count_1h") / 2), 2))
          .withColumn("amount_6h", F.round(F.col("amount") * F.greatest(F.lit(1.0), F.col("txn_count_6h") / 3), 2))
          .withColumn("amount_24h", F.round(F.col("amount") * F.greatest(F.lit(1.0), F.col("txn_count_24h") / 4), 2))
          .withColumn("unique_payees_1h", (F.rand(args.seed + 79) * 8 + 1).cast("int"))
          .withColumn("unique_payees_24h", (F.rand(args.seed + 80) * 35 + 1).cast("int"))
          .withColumn("unique_devices_24h", (F.rand(args.seed + 81) * 4 + 1).cast("int"))
          .withColumn("unique_ips_24h", (F.rand(args.seed + 82) * 6 + 1).cast("int"))
          .withColumn("unique_banks_24h", (F.rand(args.seed + 83) * 8 + 1).cast("int"))
          .withColumn("payer_account_age_days", (F.rand(args.seed + 84) * 3500 + 1).cast("int"))
          .withColumn("payer_txn_count_24h", (F.rand(args.seed + 85) * 300).cast("int"))
          .withColumn("payer_txn_count_7d", (F.rand(args.seed + 86) * 1500).cast("int"))
          .withColumn("payer_amount_24h", F.round(F.rand(args.seed + 87) * 200000, 2))
          .withColumn("payer_amount_7d", F.round(F.rand(args.seed + 88) * 800000, 2))
          .withColumn("payee_account_age_days", (F.rand(args.seed + 89) * 3500 + 1).cast("int"))
          .withColumn("payee_txn_count_24h", (F.rand(args.seed + 90) * 500).cast("int"))
          .withColumn("payee_amount_24h", F.round(F.rand(args.seed + 91) * 500000, 2))
          .withColumn("merchant_txn_count_24h",
                      F.when(F.col("_is_p2m"), (F.rand(args.seed + 92) * 10000).cast("int")))
          .withColumn("merchant_amount_24h",
                      F.when(F.col("_is_p2m"), F.round(F.rand(args.seed + 93) * 5000000, 2)))
          .withColumn("merchant_risk_score",
                      F.when(F.col("_is_p2m"), F.round(F.rand(args.seed + 94) * 100, 2)))
    )

    # ------------------------------------------------------------------
    # Risk flags. Keep these causally related to a hidden fraud propensity.
    # ------------------------------------------------------------------
    fraud_propensity = (
        F.lit(0.003)
        + F.when(F.col("amount") > 50000, 0.020).otherwise(0)
        + F.when(F.col("txn_count_1h") > 15, 0.030).otherwise(0)
        + F.when(F.col("txn_count_24h") > 150, 0.020).otherwise(0)
        + F.when(F.col("location_mismatch"), 0.025).otherwise(0)
        + F.when(F.col("device_rooted"), 0.020).otherwise(0)
        + F.when(F.col("vpn_flag"), 0.012).otherwise(0)
        + F.when(F.col("proxy_flag"), 0.008).otherwise(0)
        + F.when(F.col("device_first_seen_days_ago") < 7, 0.030).otherwise(0)
        + F.when(F.col("payee_account_age_days") < 14, 0.020).otherwise(0)
    )
    fraud_u = F.rand(args.seed + 95)
    df = df.withColumn("_fraud_propensity", F.least(fraud_propensity, F.lit(0.95)))

    df = (
        df.withColumn("new_device_flag", F.col("device_first_seen_days_ago") < 7)
          .withColumn("new_beneficiary_flag", F.col("payee_account_age_days") < 14)
          .withColumn("high_amount_flag", F.col("amount") > 50000)
          .withColumn("high_velocity_flag", F.col("txn_count_1h") > 15)
          .withColumn("unusual_time_flag", F.hour("txn_timestamp").isin([0, 1, 2, 3, 4]))
          .withColumn("unusual_amount_flag", F.col("amount") > 100000)
          .withColumn("multiple_accounts_device_flag", F.col("unique_devices_24h") > 2)
          .withColumn("multiple_devices_account_flag", F.col("unique_devices_24h") > 3)
          .withColumn("suspicious_ip_flag", (F.col("ip_risk_score") > 85) | F.col("vpn_flag") | F.col("proxy_flag"))
          .withColumn("is_fraud", (fraud_u < F.col("_fraud_propensity")).cast("int"))
          .withColumn("is_suspicious",
                      ((F.col("_fraud_propensity") > 0.02) | (F.col("ip_risk_score") > 75)).cast("int"))
    )

    risk_base = (
        F.col("_fraud_propensity") * 100
        + F.col("ip_risk_score") * 0.25
        + F.when(F.col("high_velocity_flag"), 18).otherwise(0)
        + F.when(F.col("location_mismatch"), 15).otherwise(0)
        + F.when(F.col("new_device_flag"), 12).otherwise(0)
        + F.when(F.col("new_beneficiary_flag"), 10).otherwise(0)
    )
    df = (
        df.withColumn("risk_score", F.round(F.least(risk_base, F.lit(100.0)), 2))
          .withColumn("rule_score", F.round(F.least(risk_base * 0.92 + F.rand(args.seed + 96) * 8, F.lit(100.0)), 2))
          .withColumn("ml_score", F.round(F.least((F.col("risk_score") / 100) * 0.95 + F.rand(args.seed + 97) * 0.05, F.lit(1.0)), 5))
          .withColumn("velocity_score", F.round(F.least(F.col("txn_count_1h") * 2.5, F.lit(100.0)), 2))
          .withColumn("device_score", F.round(
              F.least(F.col("ip_risk_score") * 0.55 +
                      F.when(F.col("device_rooted"), 35).otherwise(0) +
                      F.when(F.col("device_emulator"), 25).otherwise(0), F.lit(100.0)), 2))
          .withColumn("location_score", F.when(F.col("location_mismatch"), 85.0).otherwise(20.0))
          .withColumn("merchant_score", F.coalesce(F.col("merchant_risk_score"), F.lit(15.0)))
          .withColumn("customer_score", F.round(F.rand(args.seed + 98) * 100, 2))
          .withColumn("network_score", F.round(F.col("ip_risk_score"), 2))
          .withColumn("risk_level",
                      F.when(F.col("risk_score") >= 75, "CRITICAL")
                       .when(F.col("risk_score") >= 50, "HIGH")
                       .when(F.col("risk_score") >= 25, "MEDIUM")
                       .otherwise("LOW"))
          .withColumn("fraud_type",
                      F.when(F.col("is_fraud") == 0, "NONE")
                       .when(F.col("new_device_flag") & F.col("new_beneficiary_flag"), "ACCOUNT_TAKEOVER")
                       .when(F.col("high_velocity_flag"), "VELOCITY_ABUSE")
                       .when(F.col("location_mismatch"), "LOCATION_ANOMALY")
                       .otherwise("SOCIAL_ENGINEERING"))
          .withColumn("rule_triggered", F.col("risk_score") >= 50)
          .withColumn("rule_id",
                      F.when(F.col("risk_score") >= 75, "EFRM_RISK_075")
                       .when(F.col("risk_score") >= 50, "EFRM_RISK_050")
                       .when(F.col("high_velocity_flag"), "EFRM_VELOCITY_001")
                       .otherwise(F.lit(None).cast("string")))
          .withColumn("rule_count",
                      F.when(F.col("rule_triggered"), (F.rand(args.seed + 99) * 4 + 1).cast("int")).otherwise(F.lit(0)))
    )

    # ------------------------------------------------------------------
    # Outcome/response. These are intentionally correlated with risk,
    # not independent random labels.
    # ------------------------------------------------------------------
    df = (
        df.withColumn("processing_status",
                      F.when(F.col("is_reversed") == 1, "REVERSED") if "is_reversed" in df.columns
                      else F.when(F.rand(args.seed + 100) < 0.006, "TIMEOUT")
                           .when(F.col("risk_score") >= 75, "FAILURE")
                           .when(F.rand(args.seed + 101) < 0.004, "PENDING")
                           .otherwise("SUCCESS"))
          .withColumn("response_code",
                      F.when(F.col("risk_score") >= 75, "U30")
                       .when(F.col("processing_status") == "TIMEOUT", "91")
                       .otherwise("00"))
          .withColumn("error_code",
                      F.when(F.col("response_code") != "00", F.col("response_code")))
          .withColumn("timeout_flag", F.col("processing_status") == "TIMEOUT")
          .withColumn("retry_flag", F.col("timeout_flag") | (F.col("processing_status") == "PENDING"))
          .withColumn("retry_count", F.when(F.col("retry_flag"), (F.rand(args.seed + 102) * 3 + 1).cast("int")).otherwise(0))
          .withColumn("reprocessed_flag", F.col("retry_count") > 0)
          .withColumn("decision",
                      F.when(F.col("risk_score") >= 75, "DECLINE")
                       .when(F.col("risk_score") >= 50, "REVIEW")
                       .when(F.col("risk_score") >= 30, "CHALLENGE")
                       .otherwise("ALLOW"))
          .withColumn("decision_reason",
                      F.when(F.col("risk_score") >= 75, "HIGH_RISK")
                       .when(F.col("risk_score") >= 50, "RULE_TRIGGERED")
                       .when(F.col("risk_score") >= 30, "ADDITIONAL_AUTHENTICATION")
                       .otherwise("NORMAL"))
          .withColumn("is_declined", (F.col("decision") == "DECLINE").cast("int"))
          .withColumn("is_reversed", (F.col("processing_status") == "REVERSED").cast("int"))
          .withColumn("is_manual_review", (F.col("decision") == "REVIEW").cast("int"))
    )

    # ------------------------------------------------------------------
    # eFRM/Kafka/processing infrastructure.
    # ------------------------------------------------------------------
    df = (
        df.withColumn("online_offline_flag", weighted_choice(F.rand(args.seed + 103), [
            (0.94, "ONLINE"), (1.0, "OFFLINE")
        ]))
        .withColumn("processor_type", F.col("online_offline_flag"))
        .withColumn("processing_node", F.concat(F.lit("NODE-"), (F.pmod(idc, 64) + 1).cast("string")))
        .withColumn("server_region", indexed_choice(F.rand(args.seed + 104), SERVER_REGIONS))
        .withColumn("availability_zone", F.concat(F.col("server_region"), F.lit("-AZ"), (F.pmod(idc, 3) + 1).cast("string")))
        .withColumn("service_name", indexed_choice(F.rand(args.seed + 105), SERVICE_NAMES))
        .withColumn("service_version", F.concat(F.lit("3."), (F.pmod(idc, 5)).cast("string"), F.lit(".0")))
        .withColumn("kafka_topic",
                    F.when(F.col("online_offline_flag") == "OFFLINE", "TXN_POST_PROCESSOR_TOPIC")
                     .otherwise("UPI_TXN_EVENT_TOPIC"))
        .withColumn("kafka_partition", F.pmod(idc, 256).cast("int"))
        .withColumn("kafka_offset", idc)
        .withColumn("consumer_group", F.when(F.col("online_offline_flag") == "OFFLINE", "frm-datalake-consumer").otherwise("upi-risk-consumer"))
        .withColumn("consumer_instance", F.concat(F.lit("consumer-"), (F.pmod(idc, 64) + 1).cast("string")))
        .withColumn("event_lag_ms", (F.rand(args.seed + 106) * 5000).cast("long"))
        .withColumn("processing_latency_ms", (F.rand(args.seed + 107) * 150 + 5).cast("int"))
        .withColumn("queue_latency_ms", (F.rand(args.seed + 108) * 100).cast("int"))
        .withColumn("database_latency_ms", (F.rand(args.seed + 109) * 80 + 2).cast("int"))
        .withColumn("total_latency_ms",
                    F.col("event_lag_ms").cast("int") +
                    F.col("processing_latency_ms") +
                    F.col("queue_latency_ms") +
                    F.col("database_latency_ms"))
        .withColumn("yugabyte_flag", F.lit(True))
        .withColumn("cassandra_flag", F.rand(args.seed + 110) < 0.35)
        .withColumn("hive_flag", F.col("online_offline_flag") == "OFFLINE")
        .withColumn("elasticsearch_flag", F.rand(args.seed + 111) < 0.80)
    )

    # Clean helper columns and select the exact schema order.
    df = df.drop("_row_id", "_is_p2m", "_fraud_propensity")

    # Fill fields that can be null in a controlled way.
    df = df.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in UPI_EFRM_SCHEMA.fields])

    # Write partitioned Parquet. repartition() controls distributed output volume.
    output = args.output.rstrip("/")
    (
        df.repartition(args.partitions)
          .write
          .mode(args.mode)
          .option("compression", args.compression)
          .partitionBy("txn_year", "txn_month", "txn_date")
          .parquet(output)
    )

    if args.validate:
        print("Rows written:", spark.read.parquet(output).count())
        print("Columns:", len(df.columns))
        print("Schema:")
        df.printSchema()

    spark.stop()


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=1_000_000_000)
    p.add_argument("--output", required=True)
    p.add_argument("--partitions", type=int, default=4000)
    p.add_argument("--shuffle-partitions", type=int, default=None)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--start-date", default="2026-01-01")
    p.add_argument("--days", type=int, default=365)
    p.add_argument("--customers", type=int, default=25_000_000)
    p.add_argument("--devices", type=int, default=35_000_000)
    p.add_argument("--merchants", type=int, default=2_000_000)
    p.add_argument("--compression", default="snappy", choices=["snappy", "gzip", "zstd", "none"])
    p.add_argument("--mode", default="overwrite", choices=["overwrite", "append", "ignore", "error", "errorifexists"])
    p.add_argument("--validate", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    generate(parse_args())
