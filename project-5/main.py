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
from pyspark.sql.functions import col, round as _round
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

    # -------------------------------------------------------------------------
    # STEP 2: Spark Preprocessing & Feature Engineering (Silver Layer)
    # -------------------------------------------------------------------------
    print_step(2, "Feature Engineering & Silver Layer Transformation")

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

    # -------------------------------------------------------------------------
    # STEP 3: Distributed MLlib Pipeline Setup
    # -------------------------------------------------------------------------
    print_step(3, "Configuring Distributed PySpark ML Pipeline")

    indexer = StringIndexer(
        inputCol="merchant_category",
        outputCol="merchant_cat_index",
        handleInvalid="keep",
    )

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

    scaler = StandardScaler(
        inputCol="raw_features", outputCol="features", withStd=True, withMean=True
    )

    rf_classifier = RandomForestClassifier(
        labelCol="is_fraud",
        featuresCol="features",
        numTrees=10,
        maxDepth=5,
        seed=42,
    )

    pipeline = Pipeline(stages=[indexer, assembler, scaler, rf_classifier])

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
    test_eval_view = test_predictions.withColumn(
        "fraud_probability", _round(vector_to_array(col("probability"))[1], 4)
    )

    test_eval_view.select(
        "txn_id", "amount", "is_fraud", "prediction", "fraud_probability"
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
