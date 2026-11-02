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
from pyspark.sql.functions import (
    col,
    concat,
    lit,
    count,
    collect_set,
)


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


def discuss_knowledge_graph_traversal():
    """Detailed theoretical discussion on GraphX, GraphFrames, and Graph QA."""
    discussion = """
========================================================================================
💡 DISCUSSION: KNOWLEDGE GRAPH TRAVERSAL IN KNOWLEDGE DISCOVERY & QUESTION ANSWERING
========================================================================================
1. GraphX & GraphFrames Architecture in Apache Spark:
   - GraphX extends Spark RDDs with a Resilient Distributed Property Graph (Vertices RDD,
     Edge RDD, Triplet RDD) enabling distributed graph algorithms (PageRank, Shortest Path).
   - In PySpark, GraphFrames builds upon GraphX by wrapping Graph data into optimized
     DataFrames, enabling SQL-like motif pattern finding: '(a)-[e1]->(b); (b)-[e2]->(c)'.

2. Multi-Hop Relational Inference (Beyond Direct Keywords):
   - Traditional text search only finds direct keyword co-occurrences.
   - Graph traversal discovers non-obvious multi-step pathways, e.g.:
     'Google DeepMind' -> [BUILT] -> 'AlphaFold' -> [POWERED_BY] -> 'Transformer' -> [UTILIZES] -> 'Attention'
   - This enables inferring that DeepMind's biology breakthroughs fundamentally depend on Attention mechanisms.

3. Graph Question-Answering (Graph QA & Graph RAG):
   - In modern AI systems, LLMs struggle with hallucinations and complex relational reasoning.
   - Traversing Knowledge Graphs provides deterministic ground truth facts, enabling:
     a) Exact Entity Resolution
     b) Path-constrained reasoning for complex natural language questions
     c) Subgraph retrieval to augment generative models (Graph Retrieval-Augmented Generation).

4. Scalability Across Distributed Clusters:
   - When knowledge graphs scale to billions of triples (Wikidata, enterprise data meshes),
     distributed iterative joins and Pregel graph computing allow traversal without memory bottlenecks.
========================================================================================
"""
    print(discussion)


def main():
    print_banner("14. KNOWLEDGE GRAPH TRAVERSAL USING GRAPHX / PYSPARK (23BTRDC002)")
    print("Objective: Construct knowledge graph, perform multi-hop traversals,")
    print("analyze heterogeneous entity paths, and power graph question-answering.\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    v_path = os.path.join(base_dir, "data", "vertices.csv")
    e_path = os.path.join(base_dir, "data", "edges.csv")
    output_dir = os.path.join(base_dir, "output")

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

    # Join vertices with edges to form (Entity A) -> [REL] -> (Entity B)
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

    # Join e1 and e2 where e1.dst == e2.src
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
    # Paths connecting Person -> Organization -> Software Framework -> Field
    # -------------------------------------------------------------------------
    print_step(5, "Heterogeneous Entity Path Analysis Across Types")

    cross_type_paths = hop2_df.groupBy("start_type", "intermediate_type", "end_type").agg(
        count("*").alias("path_count")
    )
    print("Semantic Meta-Paths Summary:")
    cross_type_paths.show(truncate=False)

    # -------------------------------------------------------------------------
    # STEP 6: Graph Question-Answering & Knowledge Discovery Applications
    # -------------------------------------------------------------------------
    print_step(6, "Demonstrating Graph Question-Answering (Graph QA)")

    print("\n🔍 QA Query 1: 'What technological and algorithmic path connects Yann LeCun to Deep Learning?'")
    yann_paths = hop2_df.filter(
        (col("start_entity") == "Yann LeCun") & (col("end_entity") == "Deep Learning")
    ).select("start_entity", "end_entity", "traversal_path")
    yann_paths.show(truncate=False)

    print("🔍 QA Query 2: 'Which breakthrough system was built by Google DeepMind and what architecture powers it?'")
    deepmind_breakthroughs = hop2_df.filter(
        col("start_entity") == "Google DeepMind"
    ).select("start_entity", "intermediate_entity", "end_entity", "traversal_path")
    deepmind_breakthroughs.show(truncate=False)

    print("🔍 QA Query 3: 'Find all pioneer scientists who directly or indirectly founded Deep Learning:'")
    pioneers = (
        hop2_df.filter(col("end_entity") == "Deep Learning")
        .select("start_entity", "start_type", "traversal_path")
    )
    pioneers.show(truncate=False)

    # -------------------------------------------------------------------------
    # STEP 7: Theoretical Discussion
    # -------------------------------------------------------------------------
    discuss_knowledge_graph_traversal()

    # -------------------------------------------------------------------------
    # STEP 8: Export Results
    # -------------------------------------------------------------------------
    os.makedirs(output_dir, exist_ok=True)
    out_csv = os.path.join(output_dir, "multihop_paths")
    hop2_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        out_csv
    )
    print(f"📁 Multi-hop traversal paths exported to: {out_csv}\n")

    spark.stop()
    print_banner("KNOWLEDGE GRAPH TRAVERSAL EXECUTION COMPLETED")


if __name__ == "__main__":
    main()
