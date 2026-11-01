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
    # -------------------------------------------------------------------------
    print_section("STEP 2: Compute Conventional Parametric 95% Confidence Interval")
    z_critical = 1.96
    param_ci_lower = sample_mean - (z_critical * standard_error)
    param_ci_upper = sample_mean + (z_critical * standard_error)

    print("Parametric Method (Assumes Normal Distribution):")
    print(f"  • 95% CI for Mean: [{param_ci_lower:.2f}, {param_ci_upper:.2f}] hours")
    print("  • Note: No simple parametric formula exists for Median of skewed data.\n")

    # -------------------------------------------------------------------------
    # STEP 3: Distributed Bootstrap Resampling
    # -------------------------------------------------------------------------
    NUM_BOOTSTRAP_SAMPLES = 200
    print_section(
        f"STEP 3: Execute Distributed Bootstrap Resampling (B = {NUM_BOOTSTRAP_SAMPLES} Iterations)"
    )

    bootstrap_estimates = []
    print(f"Resampling with replacement across {NUM_BOOTSTRAP_SAMPLES} iterations...")

    for i in range(NUM_BOOTSTRAP_SAMPLES):
        bootstrap_sample = df.sample(withReplacement=True, fraction=1.0, seed=100 + i)

        b_stats = bootstrap_sample.select(
            avg("delivery_time_hours").alias("b_mean"),
            expr("percentile_approx(delivery_time_hours, 0.5)").alias("b_median"),
        ).collect()[0]

        bootstrap_estimates.append((i + 1, float(b_stats["b_mean"]), float(b_stats["b_median"])))

    boot_df = spark.createDataFrame(
        bootstrap_estimates, ["bootstrap_iter", "boot_mean", "boot_median"]
    )

    # -------------------------------------------------------------------------
    # STEP 4: Estimate Empirical Bootstrap 95% Confidence Intervals
    # -------------------------------------------------------------------------
    print_section("STEP 4: Calculate Empirical Bootstrap 95% Confidence Intervals")

    ci_stats = boot_df.select(
        expr("percentile_approx(boot_mean, 0.025)").alias("boot_mean_ci_lower"),
        expr("percentile_approx(boot_mean, 0.975)").alias("boot_mean_ci_upper"),
        expr("percentile_approx(boot_median, 0.025)").alias("boot_median_ci_lower"),
        expr("percentile_approx(boot_median, 0.975)").alias("boot_median_ci_upper"),
    ).collect()[0]

    boot_mean_lower = ci_stats["boot_mean_ci_lower"]
    boot_mean_upper = ci_stats["boot_mean_ci_upper"]
    boot_med_lower = ci_stats["boot_median_ci_lower"]
    boot_med_upper = ci_stats["boot_median_ci_upper"]

    print(f"Bootstrap 95% CI for Mean  : [{boot_mean_lower:.2f}, {boot_mean_upper:.2f}]")
    print(f"Bootstrap 95% CI for Median: [{boot_med_lower:.2f}, {boot_med_upper:.2f}]")

    spark.stop()


if __name__ == "__main__":
    main()
