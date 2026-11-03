# pyspark-enterprise-data-engineering

> **Production-grade Apache PySpark data engineering, distributed analytics, and MLlib pipelines. Features Customer 360 multi-source integration, GraphX knowledge graph traversal, distributed bootstrap estimation, and real-time fraud risk architecture.**

[![PySpark](https://img.shields.io/badge/PySpark-3.5%2B-orange.svg)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20Lakehouse-success.svg)](#)
[![Student/Course ID](https://img.shields.io/badge/ID-23BTRDC002-purple.svg)](#)

Welcome to the **`pyspark-enterprise-data-engineering`** repository, containing 5 complete, standalone, and tested PySpark projects.

---

## 🗂️ Project Catalog

| Directory | Experiment / Title | Problem Summary | Key PySpark Technologies |
|---|---|---|---|
| [**`project-1/`**](file:///Users/aman/Desktop/Spark_Projects/project-1/README.md) | **Exp 28: Filter Records in a DataFrame** | Create employee/student DataFrames, perform single/compound filtering, display original vs filtered, explain analytics importance. | `filter()`, `where()`, `col()`, `isin()`, SQL Predicates, Catalyst Optimizer |
| [**`project-2/`**](file:///Users/aman/Desktop/Spark_Projects/project-2/README.md) | **Exp 41: Enterprise Customer 360 Pipeline** | Integrate CRM, transactions, payments, support, web, and mobile datasets into a single 360 view while resolving conflicting identities. | Master Data Management (MDM), Golden ID, Window Functions, Left Outer Joins, Segmentation |
| [**`project-3/`**](file:///Users/aman/Desktop/Spark_Projects/project-3/README.md) | **Exp 27: Bootstrap Confidence Interval Estimation** | Implement distributed bootstrap resampling ($B=200$), estimate non-parametric 95% CIs for Mean and Median, compare against parametric normal CIs. | Distributed Resampling (`df.sample`), `percentile_approx`, Non-parametric CIs, In-memory Caching |
| [**`project-4/`**](file:///Users/aman/Desktop/Spark_Projects/project-4/README.md) | **Exp 14: Knowledge Graph Traversal Using GraphX** | Construct entity-relation knowledge graph, implement 1-hop, 2-hop, 3-hop traversal, analyze heterogeneous meta-paths, demonstrate Graph QA. | Property Graphs, GraphX / GraphFrames concepts, Triplet Join Traversal, Graph Question-Answering |
| [**`project-5/`**](file:///Users/aman/Desktop/Spark_Projects/project-5/README.md) | **Exp 16: Complete Data Science Architecture Design** | Design and build an enterprise real-time fraud detection engine with dual ingestion, Medallion storage, PySpark MLlib, and real-time decisioning. | Medallion Lakehouse (Bronze/Silver/Gold), PySpark MLlib (`Pipeline`, `RandomForest`), `vector_to_array`, Drift Monitoring |

---

## ⚡ Quick Start: Running Any Project

Each project includes a standalone **`run.sh`** runner script that automatically binds to the configured PySpark environment:

```bash
# Project 1: Filter Records in a DataFrame
./project-1/run.sh

# Project 2: Enterprise Customer 360 Pipeline
./project-2/run.sh

# Project 3: Bootstrap Confidence Interval Estimation
./project-3/run.sh

# Project 4: Knowledge Graph Traversal Using GraphX
./project-4/run.sh

# Project 5: Complete Data Science Project Architecture Design
./project-5/run.sh
```

---

## 📁 Repository Directory Tree

```
Spark_Projects/
├── README.md                      # Master Portfolio Overview
├── project-1/                     # Exp 28: Filter Records in a DataFrame
│   ├── data/                      # employees.csv, students.csv
│   ├── main.py                    # PySpark Filtering Engine
│   ├── run.sh                     # 1-click execution script
│   ├── requirements.txt           # Dependencies
│   └── README.md                  # Comprehensive Lab Report & Viva Q&A
├── project-2/                     # Exp 41: Enterprise Customer 360 Pipeline
│   ├── data/                      # 6 source datasets (CRM, Txn, Pay, Support, Web, App)
│   ├── main.py                    # Customer 360 ETL & Identity Resolution
│   ├── run.sh                     # 1-click execution script
│   ├── requirements.txt           # Dependencies
│   └── README.md                  # Architecture, MDM & Viva Q&A
├── project-3/                     # Exp 27: Bootstrap Confidence Interval Estimation
│   ├── data/                      # delivery_times.csv (skewed delivery logs)
│   ├── main.py                    # Distributed Bootstrap Resampling Engine
│   ├── run.sh                     # 1-click execution script
│   ├── requirements.txt           # Dependencies
│   └── README.md                  # Statistical Theory, Formulas & Viva Q&A
├── project-4/                     # Exp 14: Knowledge Graph Traversal Using GraphX
│   ├── data/                      # vertices.csv, edges.csv
│   ├── main.py                    # Multi-hop Graph Traversal & Graph QA Engine
│   ├── run.sh                     # 1-click execution script
│   ├── requirements.txt           # Dependencies
│   └── README.md                  # GraphX Architecture, Meta-paths & Viva Q&A
└── project-5/                     # Exp 16: Complete Data Science Architecture Design
    ├── data/                      # historical_transactions.csv, streaming_incoming_txns.csv
    ├── main.py                    # End-to-end Medallion MLlib Fraud Detection Engine
    ├── run.sh                     # 1-click execution script
    ├── requirements.txt           # Dependencies (pyspark, numpy)
    └── README.md                  # System Architecture Blueprint & Viva Q&A
```
