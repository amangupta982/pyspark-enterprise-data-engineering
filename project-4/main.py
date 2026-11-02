"""
========================================================================================
14. KNOWLEDGE GRAPH TRAVERSAL USING GRAPHX / PYSPARK (23BTRDC002)
========================================================================================
Aim & Problem Statement:
    1. Construct a knowledge graph containing entities and semantic relationships.
    2. Implement graph traversal operations to discover multi-hop relationships
       between selected entities.
    3. Analyze paths connecting different entity types (Person -> Org -> Tool -> Field).
    4. Investigate how graph traversal supports knowledge discovery and
       question-answering (Graph QA / Graph RAG) applications.
========================================================================================
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def create_spark_session() -> SparkSession:
    """Initialize local PySpark session for graph processing."""
    return (
        SparkSession.builder.appName("KnowledgeGraphTraversal_23BTRDC002")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def print_banner(title: str):
    """Formatted header banner."""
    print("\n" + "=" * 90)
    print(f" 🕸️ {title}")
    print("=" * 90)


def print_step(step_num: int, title: str):
    """Pipeline step divider."""
    print(f"\n[STEP {step_num}] {title}")
    print("-" * 90)


def main():
    print_banner("14. KNOWLEDGE GRAPH TRAVERSAL USING GRAPHX / PYSPARK (23BTRDC002)")
    print("Objective: Construct knowledge graph and perform semantic graph traversal.\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    v_path = os.path.join(base_dir, "data", "vertices.csv")
    e_path = os.path.join(base_dir, "data", "edges.csv")

    # -------------------------------------------------------------------------
    # STEP 1: Construct the Knowledge Graph
    # -------------------------------------------------------------------------
    print_step(1, "Constructing Knowledge Graph (Entities & Semantic Relations)")

    vertices = spark.read.option("header", "true").csv(v_path)
    edges = spark.read.option("header", "true").csv(e_path)

    print(f"Total Graph Entities (Vertices): {vertices.count()}")
    print(f"Total Semantic Relations (Edges): {edges.count()}\n")

    print("--- Graph Vertices (Entity Ontology) ---")
    vertices.show(truncate=False)

    print("--- Graph Edges (Semantic Triples: Subject - Predicate - Object) ---")
    edges.show(truncate=False)

    # -------------------------------------------------------------------------
    # STEP 2: 1-Hop Direct Semantic Traversal
    # -------------------------------------------------------------------------
    print_step(2, "1-Hop Direct Semantic Traversal (Triplets)")

    e = edges.alias("e")
    triplets_1hop = (
        e.join(vertices.alias("v1"), col("e.src") == col("v1.id"), "inner")
        .join(vertices.alias("v2"), col("e.dst") == col("v2.id"), "inner")
        .select(
            col("v1.name").alias("source_entity"),
            col("v1.entity_type").alias("source_type"),
            col("e.relationship").alias("relationship"),
            col("v2.name").alias("target_entity"),
            col("v2.entity_type").alias("target_type"),
        )
    )

    print("Sample 1-Hop Semantic Connections:")
    triplets_1hop.show(8, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
