"""
========================================================================================
41. ENTERPRISE CUSTOMER 360 PIPELINE (23BTRDC002 - 17/08/2026)
========================================================================================
Problem Statement:
    Integrate CRM, transaction, payment, support, website, and mobile-app datasets
    into a single Customer 360 DataFrame while resolving conflicting customer identities.
========================================================================================
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    regexp_replace,
    concat,
    lit,
    round,
    sum as _sum,
    count,
    avg,
    max as _max,
    when,
    desc,
    row_number,
)
from pyspark.sql.window import Window


def create_spark_session() -> SparkSession:
    """Initialize local SparkSession optimized for ETL and joins."""
    return (
        SparkSession.builder.appName("EnterpriseCustomer360_23BTRDC002")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def print_banner(title: str):
    """Print high-visibility header banner."""
    print("\n" + "=" * 90)
    print(f" 🌐 {title}")
    print("=" * 90)


def print_step(step_num: int, title: str):
    """Print pipeline step divider."""
    print(f"\n[STEP {step_num}] {title}")
    print("-" * 90)


def normalize_phone_col(column_name: str):
    """Remove country codes, hyphens, spaces, keeping clean 10-digit phone."""
    return regexp_replace(regexp_replace(col(column_name), r"^\+1-?", ""), r"[- ]", "")


def main():
    print_banner("41. ENTERPRISE CUSTOMER 360 PIPELINE (23BTRDC002 - 17/08/2026)")
    print("Objective: Ingest multi-source channels, resolve identities, and compute financial metrics.\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")

    # -------------------------------------------------------------------------
    # STEP 1: Ingest All 6 Source Datasets
    # -------------------------------------------------------------------------
    print_step(1, "Ingesting Multi-Source Datasets")

    crm_raw = spark.read.option("header", "true").csv(os.path.join(data_dir, "crm.csv"))
    txn_raw = spark.read.option("header", "true").csv(os.path.join(data_dir, "transactions.csv"))
    pay_raw = spark.read.option("header", "true").csv(os.path.join(data_dir, "payments.csv"))
    sup_raw = spark.read.option("header", "true").csv(os.path.join(data_dir, "support.csv"))
    web_raw = spark.read.option("header", "true").csv(os.path.join(data_dir, "website_activity.csv"))
    mob_raw = spark.read.option("header", "true").csv(os.path.join(data_dir, "mobile_app_activity.csv"))

    # -------------------------------------------------------------------------
    # STEP 2: Standardize Identifiers (Emails & Phone Numbers)
    # -------------------------------------------------------------------------
    print_step(2, "Data Cleaning & Identifier Normalization")

    crm_clean = (
        crm_raw.withColumn("norm_email", lower(trim(col("email"))))
        .withColumn("norm_phone", normalize_phone_col("phone"))
        .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
    )

    sup_clean = sup_raw.withColumn(
        "norm_email", lower(trim(col("contact_email")))
    ).withColumn("norm_phone", normalize_phone_col("contact_phone"))

    web_clean = web_raw.withColumn("norm_email", lower(trim(col("user_email"))))
    mob_clean = mob_raw.withColumn("norm_phone", normalize_phone_col("user_phone"))

    txn_clean = txn_raw.withColumn(
        "norm_identifier", lower(trim(col("customer_identifier")))
    ).withColumn("amount", col("amount").cast("double"))

    pay_clean = pay_raw.withColumn("amount", col("amount").cast("double"))

    # -------------------------------------------------------------------------
    # STEP 3: Identity Resolution (Golden Customer Registry)
    # -------------------------------------------------------------------------
    print_step(3, "Identity Resolution & Conflict Resolution Engine")

    crm_identities = crm_clean.select(
        col("crm_id"),
        col("norm_email"),
        col("norm_phone"),
        col("full_name").alias("master_name"),
        col("loyalty_tier"),
        lit("CRM").alias("identity_source"),
    )

    golden_window = Window.orderBy("crm_id")
    golden_registry = (
        crm_identities.withColumn(
            "row_num", row_number().over(golden_window)
        )
        .withColumn(
            "golden_customer_id",
            concat(lit("CUST-GOLD-"), col("row_num").cast("string")),
        )
        .drop("row_num")
    )

    print("--- Resolved Golden Customer Master Registry ---")
    golden_registry.select(
        "golden_customer_id",
        "master_name",
        "norm_email",
        "norm_phone",
        "loyalty_tier",
    ).show(truncate=False)

    # -------------------------------------------------------------------------
    # STEP 4: Link Transactions & Payments to Golden Customer ID
    # -------------------------------------------------------------------------
    print_step(4, "Linking Transactions & Payment Analytics to Golden Records")

    txn_with_golden = (
        txn_clean.join(
            golden_registry,
            (txn_clean.norm_identifier == golden_registry.norm_email)
            | (txn_clean.norm_identifier == lower(golden_registry.crm_id)),
            "inner",
        )
        .select(
            col("txn_id"),
            col("golden_customer_id"),
            col("order_date"),
            col("product_name"),
            col("category"),
            col("amount"),
            col("channel"),
        )
    )

    txn_pay_joined = txn_with_golden.join(pay_clean, "txn_id", "left")

    financial_kpis = txn_pay_joined.groupBy("golden_customer_id").agg(
        round(_sum(txn_with_golden.amount), 2).alias("total_lifetime_spend"),
        count("txn_id").alias("total_transactions"),
        round(avg(txn_with_golden.amount), 2).alias("avg_order_value"),
        _max("order_date").alias("last_order_date"),
        round(
            (
                count(when(col("payment_status") == "Completed", 1))
                / count("payment_id")
            )
            * 100,
            1,
        ).alias("payment_success_rate_pct"),
    )

    print("--- Financial & Transaction KPIs ---")
    financial_kpis.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
