"""
========================================================================================
16. COMPLETE DATA SCIENCE PROJECT ARCHITECTURE DESIGN (23BTRDC002)
========================================================================================
Real-World Problem:
    Enterprise Real-Time Financial Fraud Detection & Risk Scoring Engine

End-to-End System Architecture:
    1. Ingestion: Dual-Path (Batch: S3 / Lakehouse; Streaming: Kafka / EventStreams)
    2. Storage Layer: Medallion Architecture (Bronze -> Silver -> Gold)
    3. Preprocessing & Feature Engineering: Ratio deviations, velocity metrics
    4. Distributed Model Pipeline (PySpark MLlib): StringIndexer, VectorAssembler,
       StandardScaler, RandomForestClassifier
    5. Evaluation & Validation: ROC AUC, Precision-Recall, Confusion Matrix
    6. Deployment & Real-Time Decision Engine: APPROVE, STEP-UP OTP, DECLINE
    7. Continuous Monitoring & Feedback Loop: Drift detection & chargeback retraining
========================================================================================
"""

import os
import sys

# Ensure driver and workers use the exact same Python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    round as _round,
    count,
    avg,
    sum as _sum,
    lit,
)
from pyspark.ml import Pipeline
from pyspark.ml.functions import vector_to_array
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator


def create_spark_session() -> SparkSession:
    """Initialize local SparkSession optimized for MLlib pipelines."""
    return (
        SparkSession.builder.appName("CompleteDataScienceArchitecture_23BTRDC002")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def print_banner(title: str):
    """Header banner."""
    print("\n" + "=" * 90)
    print(f" 🛡️ {title}")
    print("=" * 90)


def print_step(step_num: int, title: str):
    """Pipeline step divider."""
    print(f"\n[STEP {step_num}] {title}")
    print("-" * 90)


def print_architecture_blueprint():
    """Display complete system architecture blueprint."""
    diagram = """
==================================================================================================
🏛️ SYSTEM ARCHITECTURE BLUEPRINT: ENTERPRISE REAL-TIME FRAUD DETECTION & RISK SCORING
==================================================================================================
           [ INGESTION LAYER ]                     [ STORAGE & FEATURE STORE ]
  +--------------------------------+          +------------------------------------+
  | Batch Sources (S3/Data Lake)   | -------->| Bronze Layer: Raw Transaction Logs|
  | Historical Txns, Profiles, KYC |          +-----------------+------------------+
  +--------------------------------+                            |
                                              +-----------------v------------------+
  +--------------------------------+          | Silver Layer: Cleaned, Normalized  |
  | Streaming Sources (Kafka)      | -------->| Curated Features & Velocity Ratios |
  | Live POS, Mobile Swipes, ATM   |          +-----------------+------------------+
  +--------------------------------+                            |
                                              +-----------------v------------------+
                                              | Gold Layer: Real-Time Risk Scores  |
                                              | Aggregated Behavioral Feature Store|
                                              +-----------------+------------------+
                                                                |
           [ ML PIPELINE & SERVING ]                            |
  +--------------------------------+                            |
  | PySpark MLlib Pipeline:        |<---------------------------+
  | StringIndexer -> Assembler ->  |
  | Scaler -> RandomForestModel    |
  +----------------+---------------+
                   |
                   v
  +--------------------------------+          +------------------------------------+
  | Real-Time Decisioning Engine:  | -------->| Monitoring & Feedback Loop:        |
  |  - Score < 0.40  => APPROVE    |          |  - Chargebacks Ground Truth Influx |
  |  - 0.40-0.75     => OTP REVIEW |          |  - Model Drift & PSI Detection     |
  |  - Score >= 0.75 => DECLINE    |          |  - Automated Retraining Pipeline   |
  +--------------------------------+          +------------------------------------+
==================================================================================================
"""
    print(diagram)


def main():
    print_banner(
        "16. COMPLETE DATA SCIENCE PROJECT ARCHITECTURE DESIGN (23BTRDC002)"
    )
    print_architecture_blueprint()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    train_file = os.path.join(base_dir, "data", "historical_transactions.csv")
    live_file = os.path.join(base_dir, "data", "streaming_incoming_txns.csv")
    output_dir = os.path.join(base_dir, "output")

    # -------------------------------------------------------------------------
    # STEP 1: Data Ingestion (Bronze Layer)
    # -------------------------------------------------------------------------
    print_step(1, "Data Ingestion & Medallion Bronze Layer")

    bronze_train = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(train_file)
    )

    print(f"Loaded Bronze historical training transactions: {bronze_train.count()} records")
    bronze_train.show(5, truncate=False)

    # -------------------------------------------------------------------------
    # STEP 2: Spark Preprocessing & Feature Engineering (Silver Layer)
    # -------------------------------------------------------------------------
    print_step(2, "Feature Engineering & Silver Layer Transformation")

    # Engineered Features:
    # 1. amount_to_avg_ratio: How much higher is this txn compared to customer's 30-day baseline?
    # 2. velocity_risk_score: Interaction between rapid transactions and abnormal distance
    silver_df = (
        bronze_train.withColumn(
            "amount_to_avg_ratio",
            _round(col("amount") / col("avg_amount_30d"), 2),
        )
        .withColumn(
            "velocity_risk_score",
            _round(col("txns_last_24h") * (col("distance_from_home_km") / 100.0), 2),
        )
    )

    print("Engineered Behavioral Feature Samples (Silver Layer):")
    silver_df.select(
        "txn_id",
        "amount",
        "avg_amount_30d",
        "amount_to_avg_ratio",
        "distance_from_home_km",
        "velocity_risk_score",
        "is_fraud",
    ).show(5, truncate=False)

    # -------------------------------------------------------------------------
    # STEP 3: Distributed MLlib Pipeline Setup
    # -------------------------------------------------------------------------
    print_step(3, "Configuring Distributed PySpark ML Pipeline")

    # 1. Encode Categorical Variable
    indexer = StringIndexer(
        inputCol="merchant_category",
        outputCol="merchant_cat_index",
        handleInvalid="keep",
    )

    # 2. Assemble Features into Dense Vector
    feature_cols = [
        "amount",
        "merchant_cat_index",
        "distance_from_home_km",
        "online_order",
        "txns_last_24h",
        "amount_to_avg_ratio",
        "velocity_risk_score",
    ]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")

    # 3. Standardize Features
    scaler = StandardScaler(
        inputCol="raw_features", outputCol="features", withStd=True, withMean=True
    )

    # 4. Distributed Classifier
    rf_classifier = RandomForestClassifier(
        labelCol="is_fraud",
        featuresCol="features",
        numTrees=10,
        maxDepth=5,
        seed=42,
    )

    pipeline = Pipeline(stages=[indexer, assembler, scaler, rf_classifier])
    print("✅ Pipeline Stages defined: StringIndexer -> VectorAssembler -> StandardScaler -> RandomForest")

    # -------------------------------------------------------------------------
    # STEP 4: Model Training & Evaluation
    # -------------------------------------------------------------------------
    print_step(4, "Model Training & Performance Evaluation")

    train_data, test_data = silver_df.randomSplit([0.8, 0.2], seed=42)
    print(f"Training Partition Size: {train_data.count()} | Test Partition Size: {test_data.count()}")

    trained_model = pipeline.fit(train_data)
    test_predictions = trained_model.transform(test_data)

    evaluator = BinaryClassificationEvaluator(
        labelCol="is_fraud",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    roc_auc = evaluator.evaluate(test_predictions)
    print(f"\n🎯 Model Evaluation Metric - ROC AUC: {roc_auc:.4f}")

    print("\nSample Test Set Predictions & Probabilities:")
    # Extract probability of fraud (class 1) using native vector_to_array
    test_eval_view = test_predictions.withColumn(
        "fraud_probability", _round(vector_to_array(col("probability"))[1], 4)
    )

    test_eval_view.select(
        "txn_id", "amount", "is_fraud", "prediction", "fraud_probability"
    ).show(truncate=False)

    # -------------------------------------------------------------------------
    # STEP 5: Real-Time Serving & Decision Engine (Gold Layer)
    # -------------------------------------------------------------------------
    print_step(5, "Deploying Real-Time Decisioning Engine (Gold Layer)")

    # Ingest streaming incoming live transactions
    streaming_raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(live_file)
    )

    # Apply silver transformations to live stream
    streaming_silver = (
        streaming_raw.withColumn(
            "amount_to_avg_ratio",
            _round(col("amount") / col("avg_amount_30d"), 2),
        )
        .withColumn(
            "velocity_risk_score",
            _round(col("txns_last_24h") * (col("distance_from_home_km") / 100.0), 2),
        )
    )

    # Score live stream with the trained model
    live_scores = trained_model.transform(streaming_silver)
    live_scored_view = live_scores.withColumn(
        "fraud_risk_score", _round(vector_to_array(col("probability"))[1], 4)
    ).withColumn(
        "automated_action",
        when(col("fraud_risk_score") >= 0.70, lit("🚫 DECLINE & BLOCK"))
        .when(col("fraud_risk_score") >= 0.40, lit("⚠️ STEP-UP OTP CHALLENGE"))
        .otherwise(lit("✅ APPROVE")),
    )

    print("--- ⚡ Real-Time Live Stream Decision Engine Output ---")
    live_scored_view.select(
        "txn_id",
        "customer_id",
        "merchant_category",
        "amount",
        "fraud_risk_score",
        "automated_action",
    ).show(truncate=False)

    # -------------------------------------------------------------------------
    # STEP 6: Continuous Monitoring, Drift & Closed-Loop Feedback
    # -------------------------------------------------------------------------
    print_step(6, "Continuous Monitoring & Feedback Loop Design")

    monitoring_summary = """
[FEEDBACK & RE-TRAINING ARCHITECTURE]
1. Concept Drift Monitoring:
   - Evaluates Population Stability Index (PSI) and Kolmogorov-Smirnov (KS) statistic
     daily between production inference features vs training feature distributions.
2. Ground-Truth Chargeback Ingestion:
   - When banks receive 30-day customer dispute chargebacks, labels are automatically
     reconciled with original transaction records.
3. Automated Retraining Trigger:
   - When Model F1-Score or ROC AUC drops below 0.85 OR PSI > 0.25, Airflow/Kubeflow
     automatically triggers the PySpark re-training pipeline and registers a candidate
     model in MLflow Model Registry for canary deployment.
"""
    print(monitoring_summary)

    # -------------------------------------------------------------------------
    # STEP 7: Export Gold Decision Output
    # -------------------------------------------------------------------------
    print_step(7, "Exporting Gold Layer Risk Scores & Reports")
    os.makedirs(output_dir, exist_ok=True)
    gold_out = os.path.join(output_dir, "real_time_fraud_decisions")
    live_scored_view.select(
        "txn_id",
        "customer_id",
        "merchant_category",
        "amount",
        "fraud_risk_score",
        "automated_action",
    ).coalesce(1).write.mode("overwrite").option("header", "true").csv(gold_out)

    print(f"✅ Gold Decision Records successfully saved to: {gold_out}\n")

    spark.stop()
    print_banner("COMPLETE DATA SCIENCE PROJECT ARCHITECTURE EXECUTION SUCCESSFUL")


if __name__ == "__main__":
    main()
