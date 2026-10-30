"""
========================================================================================
EXPERIMENT 28: Filter Records in a DataFrame (23btrdc002)
========================================================================================
Aim:
    To filter DataFrame records using PySpark.

Problem Statement:
    Create a DataFrame with sample employee or student information and retrieve records
    satisfying a specified condition. Display both the original and filtered results.
    Explain the importance of filtering in data analytics.
========================================================================================
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def create_spark_session() -> SparkSession:
    """Initialize a local PySpark SparkSession."""
    return (
        SparkSession.builder.appName("Experiment28_FilterRecords_23btrdc002")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def create_employee_dataframe(spark: SparkSession):
    """
    Method 1: Create an in-memory Employee DataFrame with schema.
    Provides sample employee records directly in code.
    """
    employee_data = [
        (101, "John Doe", "Engineering", 85000, 5, "New York", "Excellent"),
        (102, "Jane Smith", "Human Resources", 52000, 3, "Chicago", "Good"),
        (103, "Mike Johnson", "Engineering", 92000, 7, "San Francisco", "Outstanding"),
        (104, "Emily Brown", "Marketing", 61000, 4, "New York", "Good"),
        (105, "Robert Lee", "Finance", 74000, 6, "Boston", "Excellent"),
        (106, "Sarah Wilson", "Human Resources", 48000, 2, "Austin", "Average"),
        (107, "David Clark", "Engineering", 68000, 3, "Seattle", "Good"),
        (108, "Sophia Martinez", "Finance", 89000, 8, "Chicago", "Outstanding"),
        (109, "James Taylor", "Marketing", 55000, 2, "Boston", "Average"),
        (110, "Olivia Anderson", "Engineering", 105000, 10, "San Francisco", "Outstanding"),
    ]

    columns = [
        "emp_id",
        "emp_name",
        "department",
        "salary",
        "experience_years",
        "city",
        "performance_rating",
    ]

    return spark.createDataFrame(employee_data, schema=columns)


def print_section(title: str):
    """Helper to print section headers."""
    print(f"\n>>> {title}")
    print("-" * 80)


def main():
    print("=" * 80)
    print(" EXPERIMENT 28: FILTER RECORDS IN A DATAFRAME (23btrdc002) ")
    print("=" * 80)

    # 1. Initialize SparkSession
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # 2. Create the Original Employee DataFrame
    emp_df = create_employee_dataframe(spark)

    print_section("1. ORIGINAL DATAFRAME (Total Records: {})".format(emp_df.count()))
    emp_df.show(truncate=False)

    # -------------------------------------------------------------------------
    # FILTER 1: Single Condition using col()
    # Condition: Retrieve employees with salary > 70,000
    # -------------------------------------------------------------------------
    filtered_salary_gt_70k = emp_df.filter(col("salary") > 70000)
    print_section(
        f"2. FILTER CONDITION 1: Salary > 70,000 (Matched: {filtered_salary_gt_70k.count()} records)"
    )
    filtered_salary_gt_70k.select(
        "emp_id", "emp_name", "department", "salary", "experience_years"
    ).show(truncate=False)

    # -------------------------------------------------------------------------
    # FILTER 2: Multiple Conditions with AND (&)
    # Condition: Department == 'Engineering' AND experience_years >= 5
    # -------------------------------------------------------------------------
    filtered_eng_senior = emp_df.filter(
        (col("department") == "Engineering") & (col("experience_years") >= 5)
    )
    print_section(
        f"3. FILTER CONDITION 2: Department == 'Engineering' AND Experience >= 5 Years (Matched: {filtered_eng_senior.count()} records)"
    )
    filtered_eng_senior.select(
        "emp_id", "emp_name", "department", "experience_years", "salary", "performance_rating"
    ).show(truncate=False)

    # -------------------------------------------------------------------------
    # FILTER 3: Multiple Conditions with OR (|) and isin()
    # Condition: Department in ('Engineering', 'Finance') AND Performance == 'Outstanding'
    # -------------------------------------------------------------------------
    filtered_top_talent = emp_df.filter(
        col("department").isin("Engineering", "Finance")
        & (col("performance_rating") == "Outstanding")
    )
    print_section(
        f"4. FILTER CONDITION 3: Department IN ('Engineering', 'Finance') & Rating == 'Outstanding' (Matched: {filtered_top_talent.count()} records)"
    )
    filtered_top_talent.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
