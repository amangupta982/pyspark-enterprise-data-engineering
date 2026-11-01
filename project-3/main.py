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

    # -------------------------------------------------------------------------
    # STEP 2: Conventional Parametric 95% Confidence Interval for Mean
    # -------------------------------------------------------------------------
    print_section("STEP 2: Compute Conventional Parametric 95% Confidence Interval")
    z_critical = 1.96
    param_ci_lower = sample_mean - (z_critical * standard_error)
    param_ci_upper = sample_mean + (z_critical * standard_error)

    # -------------------------------------------------------------------------
    # STEP 3: Distributed Bootstrap Resampling
    # -------------------------------------------------------------------------
    NUM_BOOTSTRAP_SAMPLES = 200
    print_section(
        f"STEP 3: Execute Distributed Bootstrap Resampling (B = {NUM_BOOTSTRAP_SAMPLES} Iterations)"
    )

    bootstrap_estimates = []
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

    # -------------------------------------------------------------------------
    # STEP 5: Side-by-Side Comparison
    # -------------------------------------------------------------------------
    print_section("STEP 5: Comparison - Parametric vs. Bootstrap Confidence Intervals")

    comparison_data = [
        (
            "Mean Delivery Time",
            f"{sample_mean:.2f}",
            f"[{param_ci_lower:.2f}, {param_ci_upper:.2f}]",
            f"[{boot_mean_lower:.2f}, {boot_mean_upper:.2f}]",
            f"{param_ci_upper - param_ci_lower:.2f}",
            f"{boot_mean_upper - boot_mean_lower:.2f}",
        ),
        (
            "Median Delivery Time",
            f"{sample_median:.2f}",
            "N/A (No Standard Formula)",
            f"[{boot_med_lower:.2f}, {boot_med_upper:.2f}]",
            "N/A",
            f"{boot_med_upper - boot_med_lower:.2f}",
        ),
    ]

    columns = [
        "Statistic",
        "Point_Estimate",
        "Parametric_95_CI",
        "Bootstrap_95_CI",
        "Parametric_Width",
        "Bootstrap_Width",
    ]
    comparison_df = spark.createDataFrame(comparison_data, columns)
    comparison_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
