# Experiment 28: Filter Records in a DataFrame (23btrdc002)

## 📌 Experiment Details
- **Experiment No:** 28
- **Title:** Filter Records in a DataFrame
- **Student / Course ID:** `23btrdc002`
- **Topic:** PySpark DataFrame Operations & Transformations

---

## 🎯 Aim
To filter DataFrame records using Apache PySpark.

---

## 📝 Problem Statement
Create a DataFrame with sample employee or student information and retrieve records satisfying a specified condition. Display both the original and filtered results. Explain the importance of filtering in data analytics.

---

## 🏆 Expected Outcome
Filtered records are displayed correctly satisfying the defined filtering conditions.

---

## 📚 Theory & Concepts

### 1. What is DataFrame Filtering in PySpark?
In Apache Spark, filtering is a **narrow transformation** that inspects each row in a distributed DataFrame and retains only those rows that evaluate to `true` for a specified boolean expression. 

In PySpark, both `.filter()` and `.where()` are completely synonymous (`where` is an alias of `filter` inherited from SQL syntax).

```python
# Method 1: Using column expression
df.filter(col("salary") > 70000)

# Method 2: Using SQL condition string
df.filter("salary > 70000")

# Method 3: Using .where() alias
df.where(col("department") == "Engineering")
```

### 2. Logical Operators in PySpark
Unlike standard Python (`and`, `or`, `not`), PySpark bitwise operators must be used with parentheses:
- **AND**: `&` → `(col("salary") > 70000) & (col("experience_years") >= 5)`
- **OR**: `|` → `(col("city") == "New York") | (col("city") == "Boston")`
- **NOT**: `~` → `~(col("department") == "Marketing")`
- **IN**: `.isin(...)` → `col("department").isin("Engineering", "Finance")`

---

## 💡 Importance of Filtering in Data Analytics

1. **Predicate Pushdown & Performance Optimization:**
   - In distributed engines like Apache Spark, filtering early (row pruning) allows the Catalyst Optimizer to push down predicates directly to the data source (Parquet, ORC, JDBC databases). This minimizes disk I/O, network bandwidth, and the volume of data shuffled across worker nodes.

2. **Noise Reduction & Data Quality Improvement:**
   - Real-world big data lakes contain millions of irrelevant, corrupt, or duplicate records. Filtering eliminates bad records (e.g., negative prices, NULL identifiers, test accounts) before aggregations or model training.

3. **Targeted Business Segmentation:**
   - Analytics rarely runs on an entire monolithic dataset. Analysts need segmented slices—such as high-value customers, at-risk churn subscribers, or top-performing employees—to answer precise business questions.

4. **Resource & Memory Conservation:**
   - Working with trimmed datasets prevents driver and executor Out-Of-Memory (OOM) errors, minimizes garbage collection pauses, and saves cloud compute costs.

5. **Compliance & Data Privacy (GDPR / HIPAA):**
   - Filtering allows pipelines to exclude sensitive personally identifiable information (PII) or restrict processing strictly to consenting user groups or specific geographic jurisdictions.

---

## 📁 Project Directory Structure
```
project-1/
├── data/
│   ├── employees.csv        # Employee dataset
│   └── students.csv         # Student dataset (bonus)
├── output/
│   └── filtered_employees/  # Exported filtered records (CSV)
├── main.py                  # PySpark application script
├── requirements.txt         # Dependencies (pyspark)
└── README.md                # Complete lab experiment report
```

---

## 🚀 How to Run the Project

### 1. Activate Environment
Activate the project virtual environment:
```bash
source ../.venv/bin/activate
```
*(Or install PySpark directly via: `pip install -r requirements.txt`)*

### 2. Run the Main Script
```bash
python main.py
```

---

## 📊 Sample Output Preview

```
================================================================================
 EXPERIMENT 28: FILTER RECORDS IN A DATAFRAME (23btrdc002) 
================================================================================
AIM: To filter DataFrame records.
PROBLEM STATEMENT:
Create a DataFrame with sample employee or student information and retrieve records
satisfying a specified condition. Display both the original and filtered results.
Explain the importance of filtering in data analytics.

>>> 1. ORIGINAL DATAFRAME (Total Records: 10)
--------------------------------------------------------------------------------
+------+---------------+---------------+------+----------------+-------------+------------------+
|emp_id|emp_name       |department     |salary|experience_years|city         |performance_rating|
+------+---------------+---------------+------+----------------+-------------+------------------+
|101   |John Doe       |Engineering    |85000 |5               |New York     |Excellent         |
|102   |Jane Smith     |Human Resources|52000 |3               |Chicago      |Good              |
|103   |Mike Johnson   |Engineering    |92000 |7               |San Francisco|Outstanding       |
|104   |Emily Brown    |Marketing      |61000 |4               |New York     |Good              |
|105   |Robert Lee     |Finance        |74000 |6               |Boston       |Excellent         |
|106   |Sarah Wilson   |Human Resources|48000 |2               |Austin       |Average           |
|107   |David Clark    |Engineering    |68000 |3               |Seattle      |Good              |
|108   |Sophia Martinez|Finance        |89000 |8               |Chicago      |Outstanding       |
|109   |James Taylor   |Marketing      |55000 |2               |Boston       |Average           |
|110   |Olivia Anderson|Engineering    |105000|10              |San Francisco|Outstanding       |
+------+---------------+---------------+------+----------------+-------------+------------------+

>>> 2. FILTER CONDITION 1: Salary > 70,000 (Matched: 5 records)
--------------------------------------------------------------------------------
+------+---------------+-----------+------+----------------+
|emp_id|emp_name       |department |salary|experience_years|
+------+---------------+-----------+------+----------------+
|101   |John Doe       |Engineering|85000 |5               |
|103   |Mike Johnson   |Engineering|92000 |7               |
|105   |Robert Lee     |Finance    |74000 |6               |
|108   |Sophia Martinez|Finance    |89000 |8               |
|110   |Olivia Anderson|Engineering|105000|10              |
+------+---------------+-----------+------+----------------+

>>> 3. FILTER CONDITION 2: Department == 'Engineering' AND Experience >= 5 Years (Matched: 3 records)
--------------------------------------------------------------------------------
+------+---------------+-----------+----------------+------+------------------+
|emp_id|emp_name       |department |experience_years|salary|performance_rating|
+------+---------------+-----------+----------------+------+------------------+
|101   |John Doe       |Engineering|5               |85000 |Excellent         |
|103   |Mike Johnson   |Engineering|7               |92000 |Outstanding       |
|110   |Olivia Anderson|Engineering|10              |105000|Outstanding       |
+------+---------------+-----------+----------------+------+------------------+

>>> 4. FILTER CONDITION 3: Department IN ('Engineering', 'Finance') & Rating == 'Outstanding' (Matched: 3 records)
--------------------------------------------------------------------------------
+------+---------------+-----------+------+----------------+-------------+------------------+
|emp_id|emp_name       |department |salary|experience_years|city         |performance_rating|
+------+---------------+-----------+------+----------------+-------------+------------------+
|103   |Mike Johnson   |Engineering|92000 |7               |San Francisco|Outstanding       |
|108   |Sophia Martinez|Finance    |89000 |8               |Chicago      |Outstanding       |
|110   |Olivia Anderson|Engineering|105000|10              |San Francisco|Outstanding       |
+------+---------------+-----------+------+----------------+-------------+------------------+
```

---

## ❓ Lab Viva & Interview Questions

1. **Q: What is the difference between `.filter()` and `.where()` in PySpark?**
   - **A:** None. `.where()` is an exact alias for `.filter()`, provided to make SQL developers feel at home.

2. **Q: Is filtering in Spark a Transformation or an Action?**
   - **A:** It is a **Transformation** (specifically a Narrow Transformation). It evaluates lazily until an Action (like `.show()`, `.count()`, or `.collect()`) is called.

3. **Q: Why must bitwise `&` and `|` be used instead of Python's `and` / `or`?**
   - **A:** Python's built-in `and`/`or` evaluate expressions to short-circuit boolean primitives, whereas PySpark overrides `&` and `|` to build an Expression Tree of Column objects across distributed rows.
