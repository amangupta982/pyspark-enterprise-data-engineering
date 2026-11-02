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
from pyspark.sql.functions import col, concat, lit, count


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

    # -------------------------------------------------------------------------
    # STEP 3: 2-Hop Multi-Hop Path Traversal
    # Pattern: (A) -[R1]-> (B) -[R2]-> (C)
    # -------------------------------------------------------------------------
    print_step(3, "2-Hop Multi-Hop Relational Traversal: (A) -[R1]-> (B) -[R2]-> (C)")

    e1 = edges.alias("e1")
    e2 = edges.alias("e2")
    v1 = vertices.alias("v1")
    v2 = vertices.alias("v2")
    v3 = vertices.alias("v3")

    hop2_df = (
        e1.join(e2, col("e1.dst") == col("e2.src"), "inner")
        .join(v1, col("e1.src") == col("v1.id"), "inner")
        .join(v2, col("e1.dst") == col("v2.id"), "inner")
        .join(v3, col("e2.dst") == col("v3.id"), "inner")
        .select(
            col("v1.name").alias("start_entity"),
            col("v1.entity_type").alias("start_type"),
            col("e1.relationship").alias("rel_1"),
            col("v2.name").alias("intermediate_entity"),
            col("v2.entity_type").alias("intermediate_type"),
            col("e2.relationship").alias("rel_2"),
            col("v3.name").alias("end_entity"),
            col("v3.entity_type").alias("end_type"),
        )
        .withColumn(
            "traversal_path",
            concat(
                col("start_entity"),
                lit(" -> ["),
                col("rel_1"),
                lit("] -> "),
                col("intermediate_entity"),
                lit(" -> ["),
                col("rel_2"),
                lit("] -> "),
                col("end_entity"),
            ),
        )
    )

    print("Discovered 2-Hop Knowledge Paths:")
    hop2_df.select("start_entity", "end_entity", "traversal_path").show(
        8, truncate=False
    )

    # -------------------------------------------------------------------------
    # STEP 4: 3-Hop Deep Traversal: (A) -> (B) -> (C) -> (D)
    # -------------------------------------------------------------------------
    print_step(4, "3-Hop Deep Graph Traversal: (A) -> (B) -> (C) -> (D)")

    e3 = edges.alias("e3")
    v4 = vertices.alias("v4")

    hop3_df = (
        e1.join(e2, col("e1.dst") == col("e2.src"), "inner")
        .join(e3, col("e2.dst") == col("e3.src"), "inner")
        .join(v1, col("e1.src") == col("v1.id"), "inner")
        .join(v2, col("e1.dst") == col("v2.id"), "inner")
        .join(v3, col("e2.dst") == col("v3.id"), "inner")
        .join(v4, col("e3.dst") == col("v4.id"), "inner")
        .select(
            col("v1.name").alias("origin"),
            col("v4.name").alias("destination"),
            concat(
                col("v1.name"),
                lit(" -["),
                col("e1.relationship"),
                lit("]-> "),
                col("v2.name"),
                lit(" -["),
                col("e2.relationship"),
                lit("]-> "),
                col("v3.name"),
                lit(" -["),
                col("e3.relationship"),
                lit("]-> "),
                col("v4.name"),
            ).alias("deep_semantic_path"),
        )
    )

    print("Discovered 3-Hop Knowledge Paths:")
    hop3_df.show(truncate=False)

    # -------------------------------------------------------------------------
    # STEP 5: Heterogeneous Entity Path Analysis
    # -------------------------------------------------------------------------
    print_step(5, "Heterogeneous Entity Path Analysis Across Types")

    cross_type_paths = hop2_df.groupBy("start_type", "intermediate_type", "end_type").agg(
        count("*").alias("path_count")
    )
    print("Semantic Meta-Paths Summary:")
    cross_type_paths.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
