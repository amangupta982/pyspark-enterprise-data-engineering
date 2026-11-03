# Project 5: Complete Data Science Project Architecture Design (23BTRDC002)

## 📌 Experiment Header
- **Experiment No:** 16
- **Title:** Complete Data Science Project Architecture Design
- **Course / Student ID:** `23BTRDC002`
- **Topic:** Enterprise ML Systems Architecture, Medallion Lakehouse, PySpark MLlib & Real-Time Decisioning

---

## 🎯 Problem Statement
> **"Select a complex real-world data science problem and design its complete system architecture. Include data ingestion, storage, preprocessing, Spark analytics, feature engineering, model training, deployment, monitoring, and feedback mechanisms. Identify batch and streaming components where appropriate. Document data flow between all major components."**

---

## 🏛️ Selected Problem Domain
**Enterprise Real-Time Financial Fraud Detection & Risk Scoring Engine**  
Financial fraud detection requires low-latency real-time inference ($< 50$ ms) at Point-of-Sale (POS) and online checkouts, combined with deep historical batch feature engineering and automated model retraining.

---

## 📐 End-to-End System Architecture

```
                       +-----------------------------------------------------+
                       |            DUAL-PATH INGESTION LAYER                |
                       +-----------------------------------------------------+
                                  |                               |
                   [ BATCH INGESTION (S3/Lakehouse) ]   [ STREAMING INGESTION (Kafka) ]
                   - Historical Transactions            - Real-Time POS Swipes
                   - Customer KYC Profiles              - Online Checkouts & ATMs
                                  |                               |
                                  +---------------+---------------+
                                                  |
                                                  v
                       +-----------------------------------------------------+
                       |         STORAGE LAYER (MEDALLION LAKEHOUSE)         |
                       +-----------------------------------------------------+
                       | 🥉 BRONZE: Raw JSON/CSV Append-Only Immutable Logs  |
                       | 🥈 SILVER: Cleaned, Curated Features, Velocity Ratios|
                       | 🥇 GOLD: Real-Time Risk Scores & Analytical Marts   |
                       +--------------------------+--------------------------+
                                                  |
                                                  v
                       +-----------------------------------------------------+
                       |             PYSPARK MLLIB PIPELINE                  |
                       +-----------------------------------------------------+
                       | 1. StringIndexer (Merchant Category Encoding)       |
                       | 2. VectorAssembler (Multi-Feature Vectorization)    |
                       | 3. StandardScaler (Feature Normalization)           |
                       | 4. Distributed Classifier (RandomForest / GBT)      |
                       +--------------------------+--------------------------+
                                                  |
                                                  v
                       +-----------------------------------------------------+
                       |          REAL-TIME DECISIONING ENGINE               |
                       +-----------------------------------------------------+
                       | • Risk Score < 0.40  ==>  ✅ APPROVE TRANSACTION    |
                       | • 0.40 <= Score < 0.70 => ⚠️ STEP-UP OTP CHALLENGE  |
                       | • Risk Score >= 0.70 ==>  🚫 DECLINE & BLOCK        |
                       +--------------------------+--------------------------+
                                                  |
                                                  v
                       +-----------------------------------------------------+
                       |         MONITORING & CLOSED-LOOP FEEDBACK           |
                       +-----------------------------------------------------+
                       | 1. Concept Drift: Population Stability Index (PSI)  |
                       | 2. Label Ingestion: 30-Day Chargeback Reconciliation|
                       | 3. Automated Trigger: Airflow ML Retraining Pipeline|
                       +-----------------------------------------------------+
```

---

## ⚙️ Batch vs. Streaming Component Identification

| Component | Architecture Mode | Technology Stack | Purpose / Function |
|---|---|---|---|
| **Raw Transaction Stream** | **Streaming** | Apache Kafka / Spark Streaming | Ingests live swipe events at up to 50k transactions/sec. |
| **Historical Baseline Aggregations** | **Batch** | Apache Spark / Delta Lake | Computes 30-day baseline metrics (AOV, usual transaction locations). |
| **Feature Store Enrichment** | **Hybrid** | Feast / Redis / Spark Silver | Serves low-latency online features while updating offline tables. |
| **Model Retraining** | **Batch** | PySpark MLlib / MLflow | Distributed training on millions of labeled transaction records. |
| **Fraud Scoring & Decisioning** | **Streaming** | Spark Structured Streaming / REST | Evaluates real-time fraud probability in $< 50$ ms. |
| **Chargeback Reconciliation** | **Batch** | Spark SQL ETL | Ingests 30-day dispute records to update ground-truth labels. |

---

## 📁 Directory Structure
```
project-5/
├── data/
│   ├── historical_transactions.csv   # Historical training dataset (is_fraud label)
│   └── streaming_incoming_txns.csv   # Live incoming transaction stream for scoring
├── output/
│   └── real_time_fraud_decisions/    # Exported Gold decision records (CSV)
├── main.py                           # Complete PySpark Architecture Implementation
├── run.sh                            # 1-click execution script
├── requirements.txt                  # Dependencies (pyspark)
└── README.md                         # Architecture report & viva questions
```

---

## 🚀 How to Run

### Step 1: Execute Runner Script
```bash
./run.sh
# or
python3 main.py
```

---

## 📊 Sample Output Preview

```
==========================================================================================
 🛡️ 16. COMPLETE DATA SCIENCE PROJECT ARCHITECTURE DESIGN (23BTRDC002)
==========================================================================================

[STEP 1] Data Ingestion & Medallion Bronze Layer
Loaded Bronze historical training transactions: 25 records

[STEP 2] Feature Engineering & Silver Layer Transformation
Engineered Behavioral Features:
  - amount_to_avg_ratio: Transaction amount divided by customer's 30-day historical mean
  - velocity_risk_score: Transactions in last 24h scaled by distance from home location

[STEP 3] Configuring Distributed PySpark ML Pipeline
✅ Pipeline Stages defined: StringIndexer -> VectorAssembler -> StandardScaler -> RandomForest

[STEP 4] Model Training & Performance Evaluation
🎯 Model Evaluation Metric - ROC AUC: 1.0000

[STEP 5] Deploying Real-Time Decisioning Engine (Gold Layer)
--- ⚡ Real-Time Live Stream Decision Engine Output ---
+--------+-----------+-----------------+-------+----------------+-------------------------+
|txn_id  |customer_id|merchant_category|amount |fraud_risk_score|automated_action         |
+--------+-----------+-----------------+-------+----------------+-------------------------+
|LIVE_901|C101       |Grocery          |38.0   |0.0625          |✅ APPROVE               |
|LIVE_902|C102       |Jewelry          |1850.0 |0.8125          |🚫 DECLINE & BLOCK       |
|LIVE_903|C104       |Dining           |22.5   |0.0625          |✅ APPROVE               |
|LIVE_904|C106       |Electronics      |3400.0 |0.875           |🚫 DECLINE & BLOCK       |
|LIVE_905|C108       |Clothing         |72.0   |0.125           |✅ APPROVE               |
+--------+-----------+-----------------+-------+----------------+-------------------------+

[STEP 6] Continuous Monitoring & Feedback Loop Design
[FEEDBACK & RE-TRAINING ARCHITECTURE]
1. Concept Drift Monitoring (PSI / KS statistic)
2. Ground-Truth Chargeback Ingestion
3. Automated Retraining Trigger & Canary Deployment

✅ Gold Decision Records successfully saved to: project-5/output/real_time_fraud_decisions
```

---

## ❓ Lab Viva & Architecture Questions

1. **Q: What is the Medallion Architecture and why is it used?**
   - **A:** The Medallion Architecture organizes lakehouse data into Bronze (raw immutable ingestion), Silver (validated, normalized, feature-enriched), and Gold (business-level aggregate marts and risk decision tables). It guarantees data lineage, auditability, and clean separation between raw ingestion and ML feature consumption.

2. **Q: How does this architecture handle Concept Drift?**
   - **A:** Fraudster tactics evolve over time (e.g., new merchant laundering schemes). The monitoring layer calculates the Population Stability Index (PSI) between live inference feature vectors and training baselines. When PSI $> 0.25$ or model ROC AUC drops below SLA, an automated retraining pipeline is triggered.

3. **Q: Why is PySpark MLlib used instead of single-node Scikit-Learn?**
   - **A:** Large enterprise banks process tens of millions of daily card swipes totaling terabytes of data. PySpark MLlib trains models in parallel across distributed cluster workers without single-machine memory constraints.
