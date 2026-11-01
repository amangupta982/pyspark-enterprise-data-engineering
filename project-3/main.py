"""
========================================================================================
27. BOOTSTRAP CONFIDENCE INTERVAL ESTIMATION USING SPARK (23BTRDC002)
========================================================================================
Aim & Problem Statement:
    1. Implement distributed bootstrap resampling on a dataset using PySpark.
    2. Generate repeated bootstrap samples and estimate confidence intervals for
       selected statistics (Mean and Median).
    3. Compare bootstrap confidence intervals with conventional parametric intervals.
    4. Discuss the usefulness of bootstrap methods when theoretical distributional
       assumptions are uncertain (skewed / non-normal data).
========================================================================================
"""

import os
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    stddev,
    count,
    expr,
)


def create_spark_session() -> SparkSession:
    """Initialize local PySpark session."""
    return (
        SparkSession.builder.appName("BootstrapConfidenceIntervals_23BTRDC002")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def print_banner(title: str):
    """Formatted header banner."""
    print("\n" + "=" * 85)
    print(f" 📊 {title}")
    print("=" * 85)


def print_section(title: str):
    """Section header."""
    print(f"\n>>> {title}")
    print("-" * 85)


def main():
    print_banner(
        "27. BOOTSTRAP CONFIDENCE INTERVAL ESTIMATION USING SPARK (23BTRDC002)"
    )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(base_dir, "data", "delivery_times.csv")

    # -------------------------------------------------------------------------
    # STEP 1: Load Data & Inspect Distribution
    # -------------------------------------------------------------------------
    print_section("STEP 1: Load Dataset & Compute Baseline Sample Metrics")
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(input_file)
        .cache()
    )

    print(f"Loaded records count: {df.count()}")
    print("\nSample Preview (Delivery Times in Hours):")
    df.select("order_id", "delivery_time_hours", "carrier", "package_weight_kg").show(
        5, truncate=False
    )

    stats = df.select(
        count("delivery_time_hours").alias("n"),
        avg("delivery_time_hours").alias("sample_mean"),
        stddev("delivery_time_hours").alias("sample_std"),
        expr("percentile_approx(delivery_time_hours, 0.5)").alias("sample_median"),
    ).collect()[0]

    n = stats["n"]
    sample_mean = stats["sample_mean"]
    sample_std = stats["sample_std"]
    sample_median = stats["sample_median"]
    standard_error = sample_std / math.sqrt(n)

    print(f"Sample Size (n)       : {n}")
    print(f"Sample Mean (x̄)       : {sample_mean:.2f} hours")
    print(f"Sample Std Dev (s)    : {sample_std:.2f} hours")
    print(f"Standard Error (SE)   : {standard_error:.2f} hours")
    print(f"Sample Median         : {sample_median:.2f} hours")

    # -------------------------------------------------------------------------
    # STEP 2: Conventional Parametric 95% Confidence Interval for Mean
    # Formula: Mean ± Z * (s / sqrt(n)), where Z = 1.96 for 95%
    # -------------------------------------------------------------------------
    print_section("STEP 2: Compute Conventional Parametric 95% Confidence Interval")
    z_critical = 1.96
    param_ci_lower = sample_mean - (z_critical * standard_error)
    param_ci_upper = sample_mean + (z_critical * standard_error)

    print("Parametric Method (Assumes Normal Distribution):")
    print(f"  • 95% CI for Mean: [{param_ci_lower:.2f}, {param_ci_upper:.2f}] hours")
    print("  • Note: No simple parametric formula exists for Median of skewed data.\n")

    spark.stop()


if __name__ == "__main__":
    main()
