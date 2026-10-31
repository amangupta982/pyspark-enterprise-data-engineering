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
from pyspark.sql.functions import col, lower, trim, regexp_replace, concat, lit


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
    print("Objective: Ingest multi-source channels and standardize customer identifiers.\n")

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

    print(f" Loaded CRM records:             {crm_raw.count()}")
    print(f" Loaded Transactions:            {txn_raw.count()}")
    print(f" Loaded Payments:                {pay_raw.count()}")
    print(f" Loaded Support Tickets:         {sup_raw.count()}")
    print(f" Loaded Website Sessions:        {web_raw.count()}")
    print(f" Loaded Mobile App Sessions:     {mob_raw.count()}")

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

    print("✅ Identifiers standardized (case-insensitive emails, canonical 10-digit phones).")

    spark.stop()


if __name__ == "__main__":
    main()
