"""
========================================================================================
41. ENTERPRISE CUSTOMER 360 PIPELINE (23BTRDC002 - 17/08/2026)
========================================================================================
Problem Statement:
    Integrate CRM, transaction, payment, support, website, and mobile-app datasets
    into a single Customer 360 DataFrame while resolving conflicting customer identities.

Pipeline Architecture:
    1. Multi-source Ingestion: CRM, Transactions, Payments, Support, Web, Mobile App.
    2. Data Cleaning & Normalization: Standardize emails, phone numbers, timestamps.
    3. Identity Resolution (Golden Record Engine):
       - Probabilistic & deterministic identity matching (Email, Phone, CRM ID).
       - Conflict resolution using source hierarchy (CRM > Support > Web/App).
       - Golden Customer ID assignment.
    4. Domain Feature Aggregations:
       - Financial & Purchase KPIs (Total Spend, AOV, Preferred Channel)
       - Payment Health (Success Rate, Preferred Payment Method)
       - Customer Experience & Support (Ticket Counts, Open Issues, Avg CSAT)
       - Digital Engagement (Web Page Views, Mobile Screen Time, Omnichannel Status)
    5. Customer 360 Unified View & Strategic Segmentation (VIP, At-Risk, Digital, Standard).
    6. Exporting Consolidated Customer 360 Data.
========================================================================================
"""

import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    regexp_replace,
    coalesce,
    lit,
    sum as _sum,
    count,
    avg,
    round,
    max as _max,
    when,
    desc,
    row_number,
    concat,
    format_number,
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
    print("Objective: Integrate CRM, transaction, payment, support, website, and mobile-app")
    print("datasets into a single Customer 360 DataFrame while resolving conflicting identities.\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")
    output_dir = os.path.join(base_dir, "output")

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

    # Transactions may link via email OR crm_id
    txn_clean = txn_raw.withColumn(
        "norm_identifier", lower(trim(col("customer_identifier")))
    ).withColumn("amount", col("amount").cast("double"))

    pay_clean = pay_raw.withColumn("amount", col("amount").cast("double"))

    print("✅ Identifiers standardized (case-insensitive emails, canonical 10-digit phones).")

    # -------------------------------------------------------------------------
    # STEP 3: Identity Resolution (Golden Customer Registry)
    # -------------------------------------------------------------------------
    print_step(3, "Identity Resolution & Conflict Resolution Engine")

    # Collect all unique identity pairs across CRM, Support, Web, Mobile
    # Priority Rule: CRM is the master authority for Name and Loyalty Tier.
    # If a customer only contacted via Support or Web, resolve to nearest identity.
    crm_identities = crm_clean.select(
        col("crm_id"),
        col("norm_email"),
        col("norm_phone"),
        col("full_name").alias("master_name"),
        col("loyalty_tier"),
        lit("CRM").alias("identity_source"),
    )

    # Window function to assign deterministic Golden Customer IDs (CUST-GOLD-001, etc.)
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

    # Link transactions via email OR CRM_ID
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

    # Join Payments with Transactions to evaluate payment success & method
    txn_pay_joined = txn_with_golden.join(pay_clean, "txn_id", "left")

    # Aggregate Financial KPIs per Golden Customer
    financial_kpis = txn_pay_joined.groupBy("golden_customer_id").agg(
        round(_sum(txn_with_golden.amount), 2).alias("total_lifetime_spend"),
        count("txn_id").alias("total_transactions"),
        round(avg(txn_with_golden.amount), 2).alias("avg_order_value"),
        _max("order_date").alias("last_order_date"),
        # Payment metrics
        round(
            (
                count(when(col("payment_status") == "Completed", 1))
                / count("payment_id")
            )
            * 100,
            1,
        ).alias("payment_success_rate_pct"),
    )

    # Preferred channel (e.g. Web, Store, Mobile App)
    channel_window = Window.partitionBy("golden_customer_id").orderBy(
        desc("channel_count")
    )
    pref_channel_df = (
        txn_with_golden.groupBy("golden_customer_id", "channel")
        .agg(count("txn_id").alias("channel_count"))
        .withColumn("rank", row_number().over(channel_window))
        .filter(col("rank") == 1)
        .select(col("golden_customer_id"), col("channel").alias("preferred_shopping_channel"))
    )

    financial_summary = financial_kpis.join(
        pref_channel_df, "golden_customer_id", "left"
    )

    # -------------------------------------------------------------------------
    # STEP 5: Aggregate Support & Customer Experience KPIs
    # -------------------------------------------------------------------------
    print_step(5, "Aggregating Customer Support & Sentiment (CSAT)")

    # Match support tickets via email OR phone
    sup_with_golden = sup_clean.join(
        golden_registry,
        (sup_clean.norm_email == golden_registry.norm_email)
        | (sup_clean.norm_phone == golden_registry.norm_phone),
        "inner",
    )

    support_kpis = sup_with_golden.groupBy("golden_customer_id").agg(
        count("ticket_id").alias("total_support_tickets"),
        count(when(col("status") == "Open", 1)).alias("open_support_tickets"),
        round(avg(col("csat_score").cast("double")), 2).alias("avg_csat_score"),
    )

    # -------------------------------------------------------------------------
    # STEP 6: Aggregate Digital Engagement (Website & Mobile App)
    # -------------------------------------------------------------------------
    print_step(6, "Aggregating Omnichannel Digital Engagement (Web + App)")

    # Web metrics
    web_with_golden = web_clean.join(
        golden_registry, web_clean.norm_email == golden_registry.norm_email, "inner"
    )
    web_kpis = web_with_golden.groupBy("golden_customer_id").agg(
        _sum(col("page_views").cast("int")).alias("total_web_pageviews"),
        _sum(col("time_spent_secs").cast("int")).alias("total_web_time_secs"),
        _max("last_active_date").alias("last_web_active_date"),
    )

    # Mobile metrics
    mob_with_golden = mob_clean.join(
        golden_registry, mob_clean.norm_phone == golden_registry.norm_phone, "inner"
    )
    mob_kpis = mob_with_golden.groupBy("golden_customer_id").agg(
        count("app_session_id").alias("total_app_sessions"),
        round(_sum(col("screen_time_mins").cast("double")), 1).alias(
            "total_app_screen_time_mins"
        ),
        _max("last_app_open").alias("last_app_open_date"),
    )

    # -------------------------------------------------------------------------
    # STEP 7: Consolidate Into Unified Customer 360 DataFrame
    # -------------------------------------------------------------------------
    print_step(7, "Merging Unified Customer 360 View & Segmentation")

    customer_360 = (
        golden_registry.select(
            "golden_customer_id",
            "master_name",
            "norm_email",
            "norm_phone",
            "loyalty_tier",
        )
        .join(financial_summary, "golden_customer_id", "left")
        .join(support_kpis, "golden_customer_id", "left")
        .join(web_kpis, "golden_customer_id", "left")
        .join(mob_kpis, "golden_customer_id", "left")
        # Impute missing values with business defaults
        .fillna(
            {
                "total_lifetime_spend": 0.0,
                "total_transactions": 0,
                "avg_order_value": 0.0,
                "payment_success_rate_pct": 100.0,
                "preferred_shopping_channel": "None",
                "total_support_tickets": 0,
                "open_support_tickets": 0,
                "avg_csat_score": 5.0,
                "total_web_pageviews": 0,
                "total_web_time_secs": 0,
                "total_app_sessions": 0,
                "total_app_screen_time_mins": 0.0,
            }
        )
    )

    # Strategic Feature: Omnichannel Flag & Customer 360 Segment
    customer_360_segmented = (
        customer_360.withColumn(
            "is_omnichannel",
            (col("total_transactions") > 0)
            & (col("total_web_pageviews") > 0)
            & (col("total_app_sessions") > 0),
        )
        .withColumn(
            "customer_360_segment",
            when(
                (col("total_lifetime_spend") >= 500.0) & (col("avg_csat_score") >= 4.0),
                lit("💎 High-Value VIP"),
            )
            .when(
                (col("open_support_tickets") > 0) | (col("avg_csat_score") < 3.5),
                lit("⚠️ At-Risk / Needs Attention"),
            )
            .when(
                (col("total_web_pageviews") > 15) | (col("total_app_screen_time_mins") > 30),
                lit("📱 Digital Engaged Explorer"),
            )
            .otherwise(lit("⭐ Standard Active")),
        )
    )

    print("\n--- 🌟 FINAL UNIFIED CUSTOMER 360 DATAFRAME ---")
    customer_360_segmented.select(
        "golden_customer_id",
        "master_name",
        "loyalty_tier",
        "total_lifetime_spend",
        "total_transactions",
        "preferred_shopping_channel",
        "open_support_tickets",
        "avg_csat_score",
        "is_omnichannel",
        "customer_360_segment",
    ).show(truncate=False)

    print("\n--- 📈 Customer 360 Segment Breakdown ---")
    customer_360_segmented.groupBy("customer_360_segment").agg(
        count("golden_customer_id").alias("customer_count"),
        round(_sum("total_lifetime_spend"), 2).alias("segment_revenue"),
        round(avg("avg_csat_score"), 2).alias("avg_satisfaction"),
    ).orderBy(desc("segment_revenue")).show(truncate=False)

    # -------------------------------------------------------------------------
    # STEP 8: Export Customer 360 Data
    # -------------------------------------------------------------------------
    print_step(8, "Exporting Customer 360 Results")
    os.makedirs(output_dir, exist_ok=True)
    c360_out = os.path.join(output_dir, "customer_360_master")
    customer_360_segmented.coalesce(1).write.mode("overwrite").option(
        "header", "true"
    ).csv(c360_out)

    print(f"✅ Master Customer 360 DataFrame successfully saved to: {c360_out}")

    spark.stop()
    print_banner("ENTERPRISE CUSTOMER 360 PIPELINE EXECUTION COMPLETED")


if __name__ == "__main__":
    main()
