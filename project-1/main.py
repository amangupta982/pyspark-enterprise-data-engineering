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

Expected Outcome:
    Filtered records are displayed correctly.
========================================================================================
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc


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


def print_banner(title: str):
    """Helper to print formatted banners."""
    print("\n" + "=" * 80)
    print(f" {title} ")
    print("=" * 80)


def print_section(title: str):
    """Helper to print section headers."""
    print(f"\n>>> {title}")
    print("-" * 80)


def explain_importance_of_filtering():
    """Print detailed theoretical explanation for the lab experiment."""
    explanation = """
========================================================================================
💡 IMPORTANCE OF FILTERING IN DATA ANALYTICS:
========================================================================================
1. Performance Optimization & Query Pushdown (Predicate Pushdown):
   - Filtering early (row pruning) drastically reduces the data volume shuffled across
     cluster worker nodes, speeding up join, grouping, and aggregation operations.

2. Noise & Irrelevant Data Reduction:
   - Real-world enterprise datasets contain millions of unneeded records. Filtering isolates
     the exact slice of data relevant to the business hypothesis or KPI question.

3. Outlier, Anomaly & Error Detection:
   - Filtering allows analysts to quickly flag erroneous records (e.g., negative prices,
     null values, invalid dates) and remove them during ETL / data preparation pipelines.

4. Targeted Business Segmentation & Personalization:
   - Enables segmented reporting, such as high-performing employees (Salary > 80k AND Rating = Outstanding),
     active churn risks, or regional demographics.

5. Resource & Memory Efficiency:
   - In distributed engines like Apache Spark, smaller filtered DataFrames require lower memory
     footprints, avoiding Out-Of-Memory (OOM) errors and disk spills.
========================================================================================
"""
    print(explanation)


def main():
    print_banner("EXPERIMENT 28: FILTER RECORDS IN A DATAFRAME (23btrdc002)")
    print("AIM: To filter DataFrame records.")
    print("PROBLEM STATEMENT:")
    print("Create a DataFrame with sample employee or student information and retrieve records")
    print("satisfying a specified condition. Display both the original and filtered results.")
    print("Explain the importance of filtering in data analytics.\n")

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

    # -------------------------------------------------------------------------
    # FILTER 4: SQL Expression String Syntax
    # Condition: SQL style string filter: "city = 'New York' OR salary >= 90000"
    # -------------------------------------------------------------------------
    filtered_sql_style = emp_df.filter("city = 'New York' OR salary >= 90000")
    print_section(
        f"5. FILTER CONDITION 4 (SQL Syntax): city = 'New York' OR salary >= 90000 (Matched: {filtered_sql_style.count()} records)"
    )
    filtered_sql_style.select("emp_id", "emp_name", "city", "salary", "department").show(
        truncate=False
    )

    # -------------------------------------------------------------------------
    # BONUS: Student DataFrame Filter
    # -------------------------------------------------------------------------
    base_dir = os.path.dirname(os.path.abspath(__file__))
    students_csv_path = os.path.join(base_dir, "data", "students.csv")
    if os.path.exists(students_csv_path):
        student_df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(students_csv_path)
        )
        print_section("6. BONUS - ORIGINAL STUDENTS DATAFRAME")
        student_df.show(truncate=False)

        # Condition: Marks >= 80 AND Attendance >= 90%
        merit_students = student_df.filter(
            (col("marks") >= 80.0) & (col("attendance_pct") >= 90.0)
        )
        print_section(
            f"7. BONUS - FILTERED MERIT STUDENTS (Marks >= 80 & Attendance >= 90%) (Matched: {merit_students.count()} records)"
        )
        merit_students.show(truncate=False)

    # -------------------------------------------------------------------------
    # Explanation of the Importance of Filtering in Data Analytics
    # -------------------------------------------------------------------------
    explain_importance_of_filtering()

    # -------------------------------------------------------------------------
    # Save the filtered results to output folder
    # -------------------------------------------------------------------------
    output_dir = os.path.join(base_dir, "output")
    os.makedirs(output_dir, exist_ok=True)
    out_file = os.path.join(output_dir, "filtered_employees")
    filtered_salary_gt_70k.coalesce(1).write.mode("overwrite").option(
        "header", "true"
    ).csv(out_file)
    print(f"📁 Filtered output successfully saved to: {out_file}\n")

    spark.stop()
    print("=" * 80)
    print("✅ Experiment 28 Completed Successfully!")
    print("=" * 80)


if __name__ == "__main__":
    main()
