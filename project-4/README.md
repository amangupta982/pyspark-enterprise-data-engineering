# Project 4: Knowledge Graph Traversal Using GraphX / PySpark (23BTRDC002)

## 📌 Experiment Header
- **Experiment No:** 14
- **Title:** Knowledge Graph Traversal Using GraphX / PySpark
- **Course / Student ID:** `23BTRDC002`
- **Topic:** Distributed Graph Processing, Multi-Hop Traversal & Semantic Graph QA

---

## 🎯 Problem Statement
> **"Construct a knowledge graph containing entities and semantic relationships. Implement graph traversal operations to discover multi-hop relationships between selected entities. Analyze paths connecting different entity types. Investigate how graph traversal can support knowledge discovery and question-answering applications."**

---

## 🧠 Theory & Core Architecture

### 1. Property Graph Abstraction (GraphX & PySpark)
In Apache Spark, a Knowledge Graph is represented as a directed **Property Graph**:
- **Vertices (V):** Entities with unique identifiers and attributes (e.g., `Person`, `Organization`, `Algorithm`, `Architecture`).
- **Edges (E):** Semantic, typed, directed relationships linking two vertices (e.g., `INVENTED`, `WORKS_AT`, `BUILT`, `POWERED_BY`).
- **Triplets:** A composite view combining source vertex attributes, edge relationship, and destination vertex attributes:
  $$\langle \text{Subject}, \; \text{Predicate}, \; \text{Object} \rangle$$

```
 (Yann LeCun) --[WORKS_AT]--> (Meta AI) --[DEVELOPED]--> (PyTorch) --[IMPLEMENTS]--> (Deep Learning)
    [Person]                  [Organization]             [Software]                     [Field]
```

### 2. Multi-Hop Traversal Mechanisms
- **1-Hop Traversal:** Identifies direct dependencies and immediate semantic neighbors.
- **2-Hop Traversal:** Self-joining Edge tables ($E_1 \bowtie E_2$ where $E_1.dst = E_2.src$) to discover indirect paths:
  $$(v_1) \xrightarrow{r_1} (v_2) \xrightarrow{r_2} (v_3)$$
- **3-Hop Traversal:** Discovers deep transitive inferences and cross-domain influence.

### 3. Graph Traversal in Knowledge Discovery & Question Answering (Graph QA / Graph RAG)
1. **Deterministic Reasoning:** Traditional semantic search relies on keyword matching. Graph traversal walks verified ontological edges to discover causal and structural connections.
2. **Path-Constrained Question Answering:** Answers complex questions such as *"How is Google DeepMind connected to Attention Mechanisms?"* by navigating:
   $$\text{Google DeepMind} \xrightarrow{\text{BUILT}} \text{AlphaFold} \xrightarrow{\text{POWERED\_BY}} \text{Transformer} \xrightarrow{\text{UTILIZES}} \text{Attention Mechanism}$$
3. **Graph RAG (Retrieval-Augmented Generation):** Knowledge Graph subgraphs are retrieved and fed into LLMs to eliminate hallucinations and ground responses in verifiable facts.

---

## 📁 Directory Structure
```
project-4/
├── data/
│   ├── vertices.csv             # Graph entities (id, name, entity_type, domain)
│   └── edges.csv                # Semantic relations (src, dst, relationship, weight)
├── output/
│   └── multihop_paths/          # Exported multi-hop path discoveries (CSV)
├── main.py                      # PySpark Knowledge Graph Traversal Engine
├── run.sh                       # 1-click execution script
├── requirements.txt             # Dependencies (pyspark)
└── README.md                    # Complete lab experiment report
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
 🕸️ 14. KNOWLEDGE GRAPH TRAVERSAL USING GRAPHX / PYSPARK (23BTRDC002)
==========================================================================================

[STEP 1] Constructing Knowledge Graph (Entities & Semantic Relations)
Total Graph Entities (Vertices): 14
Total Semantic Relations (Edges): 14

[STEP 2] 1-Hop Direct Semantic Traversal (Triplets)
+-----------------------+-------------+-------------+-----------------------+-------------+
|source_entity          |source_type  |relationship |target_entity          |target_type  |
+-----------------------+-------------+-------------+-----------------------+-------------+
|Alan Turing            |Person       |INVENTED     |Turing Machine         |Concept      |
|Geoffrey Hinton        |Person       |CO_DEVELOPED |Backpropagation        |Algorithm    |
|Backpropagation        |Algorithm    |FOUNDATION_OF|Deep Learning          |Field        |
|Yann LeCun             |Person       |WORKS_AT     |Meta AI                |Organization |
|Google DeepMind        |Organization |BUILT        |AlphaFold              |AI System    |
+-----------------------+-------------+-------------+-----------------------+-------------+

[STEP 3] 2-Hop Multi-Hop Relational Traversal: (A) -[R1]-> (B) -[R2]-> (C)
+---------------+-------------+---------------------------------------------------------------------------------+
|start_entity   |end_entity   |traversal_path                                                                   |
+---------------+-------------+---------------------------------------------------------------------------------+
|Geoffrey Hinton|Deep Learning|Geoffrey Hinton -> [CO_DEVELOPED] -> Backpropagation -> [FOUNDATION_OF] -> Deep Learning|
|Yann LeCun     |Deep Learning|Yann LeCun -> [PIONEERED] -> Convolutional Neural Network -> [SUBFIELD_OF] -> Deep Learning|
|Yann LeCun     |PyTorch      |Yann LeCun -> [WORKS_AT] -> Meta AI -> [DEVELOPED] -> PyTorch                    |
|Google DeepMind|Transformer  |Google DeepMind -> [BUILT] -> AlphaFold -> [POWERED_BY] -> Transformer Architecture|
+---------------+-------------+---------------------------------------------------------------------------------+

[STEP 4] 3-Hop Deep Graph Traversal: (A) -> (B) -> (C) -> (D)
+---------------+-------------------+-------------------------------------------------------------------------------------------------------------------------+
|origin         |destination        |deep_semantic_path                                                                                                       |
+---------------+-------------------+-------------------------------------------------------------------------------------------------------------------------+
|Yann LeCun     |Deep Learning      |Yann LeCun -[WORKS_AT]-> Meta AI -[DEVELOPED]-> PyTorch -[IMPLEMENTS]-> Deep Learning                                    |
|Google DeepMind|Attention Mechanism|Google DeepMind -[BUILT]-> AlphaFold -[POWERED_BY]-> Transformer Architecture -[UTILIZES]-> Attention Mechanism          |
|Google DeepMind|Deep Learning      |Google DeepMind -[BUILT]-> AlphaFold -[POWERED_BY]-> Transformer Architecture -[PILLAR_OF]-> Deep Learning                |
+---------------+-------------------+-------------------------------------------------------------------------------------------------------------------------+

[STEP 6] Demonstrating Graph Question-Answering (Graph QA)
🔍 QA Query 1: 'What technological and algorithmic path connects Yann LeCun to Deep Learning?'
Yann LeCun -> [PIONEERED] -> Convolutional Neural Network -> [SUBFIELD_OF] -> Deep Learning

🔍 QA Query 2: 'Which breakthrough system was built by Google DeepMind and what architecture powers it?'
Google DeepMind -> [BUILT] -> AlphaFold -> [POWERED_BY] -> Transformer Architecture
```

---

## ❓ Lab Viva & Interview Questions

1. **Q: What is the difference between GraphX and GraphFrames?**
   - **A:** GraphX is the low-level RDD-based graph computing API in Apache Spark (implemented in Scala). GraphFrames is the modern DataFrame-based graph library that allows declarative motif searching, SQL queries, and Python (PySpark) integration.

2. **Q: How does distributed self-joining simulate graph traversal in Spark?**
   - **A:** By joining edge table $E_1$ with edge table $E_2$ on condition $E_1.\text{dst} = E_2.\text{src}$, Spark discovers all 2-hop connected vertices in parallel across cluster partitions.

3. **Q: What is a heterogeneous meta-path in a Knowledge Graph?**
   - **A:** A sequence of relations connecting different entity types (e.g., `Person` $\to$ `Organization` $\to$ `Software Tool` $\to$ `Research Domain`). Meta-paths are used to compute structural similarity and recommendation rankings.
