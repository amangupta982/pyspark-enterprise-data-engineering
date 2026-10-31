# Project 2: Enterprise Customer 360 Pipeline (23BTRDC002)

## 📌 Project Header
- **Project No:** 41
- **Title:** Enterprise Customer 360 Pipeline
- **Course / Student ID:** `23BTRDC002`
- **Submission Date:** `17/08/2026`
- **Technologies:** Apache Spark, PySpark, Master Data Management (MDM), Spark SQL

---

## 🎯 Problem Statement
> **"Integrate CRM, transaction, payment, support, website, and mobile-app datasets into a single Customer 360 DataFrame while resolving conflicting customer identities."**

---

## 🏗️ Architecture & Pipeline Overview

```
                      +---------------------------+
                      | Multi-Source Data Ingress |
                      +-------------+-------------+
                                    |
     +------------+------------+----+----+------------+------------+
     |            |            |         |            |            |
   [ CRM ]   [ Transactions ] [ Payments ] [ Support ] [ Website ] [ Mobile App ]
     |            |            |         |            |            |
     +------------+------------+----+----+------------+------------+
                                    |
                    +---------------v---------------+
                    |  1. Ingestion & Normalization |
                    | (Canonical Email & Phone Fmt) |
                    +---------------^---------------+
                                    |
                    +---------------v---------------+
                    | 2. Identity Resolution Engine |
                    |  (Golden Customer ID Registry)|
                    +---------------^---------------+
                                    |
                    +---------------v---------------+
                    | 3. Domain Feature Extraction  |
                    |   - Total Spend, AOV, LTV     |
                    |   - Payment Success Rate      |
                    |   - Open Tickets & Avg CSAT   |
                    |   - Web Pageviews & App Time  |
                    +---------------^---------------+
                                    |
                    +---------------v---------------+
                    | 4. Customer 360 Consolidation |
                    |  & Behavioral Segmentation    |
                    |  (VIP, At-Risk, Omnichannel)  |
                    +---------------+---------------+
                                    |
                    +---------------v---------------+
                    | 5. Master Output Data Export  |
                    +-------------------------------+
```

---

## 🧠 Core Engineering Principles Implemented

### 1. Identity Resolution & Conflict Handling
- In modern enterprise environments, customers interact across distinct channels without a unified primary key:
  - **CRM**: Authoritative source for official names, sign-up date, and loyalty tiers.
  - **Transactions**: Linked via email or CRM ID.
  - **Payments**: Linked to transaction IDs with failure/success logs.
  - **Support**: Initiated via phone or email (sometimes alternative work emails).
  - **Website**: Tracked via sessions and logged-in email.
  - **Mobile App**: Tracked via registered phone number and device UUID.
- **Resolution Strategy**:
  - Normalizes emails (lowercase, trimmed).
  - Strips international dial prefixes and punctuation to 10-digit canonical phone numbers.
  - Matches across identity vectors to assign a deterministic `golden_customer_id` (`CUST-GOLD-001`).
  - Resolves name conflicts by prioritizing CRM authority over conversational support or app inputs.

### 2. Multi-Table Joins & Spark Optimization
- Uses broadcast joins where applicable for dimension datasets.
- Left outer joins against the Golden Registry ensure zero customer loss.
- Uses PySpark Window functions (`Window.partitionBy().orderBy(desc("channel_count"))`) to calculate each customer's preferred channel and deterministic rankings.

### 3. Business Segmentation Engine
- **💎 High-Value VIP**: Lifetime Spend $\ge \$500$ and CSAT $\ge 4.0$.
- **⚠️ At-Risk / Needs Attention**: Customers with open support tickets or CSAT $< 3.5$.
- **📱 Digital Engaged Explorer**: High web pageviews ($> 15$) or app screen time ($> 30$ mins) with active digital adoption.
- **⭐ Standard Active**: Regular satisfied customers.

---

## 📁 Project Directory Structure
```
project-2/
├── data/
│   ├── crm.csv                  # Master CRM customer accounts
│   ├── transactions.csv         # Multi-channel purchases
│   ├── payments.csv             # Payment gateway logs
│   ├── support.csv              # Customer service & CSAT scores
│   ├── website_activity.csv     # Web browsing & page views
│   └── mobile_app_activity.csv  # App screen time & notifications
├── output/
│   └── customer_360_master/     # Final unified 360 CSV output
├── main.py                      # Complete PySpark 360 Pipeline
├── run.sh                       # 1-click execution script
├── requirements.txt             # PySpark dependencies
└── README.md                    # Project report & documentation
```

---

## 🚀 How to Run the Project

### 1. Activate Environment
```bash
source ../.venv/bin/activate
```
*(Or install via `pip install -r requirements.txt`)*

### 2. Execute Pipeline
```bash
./run.sh
# or
python3 main.py
```

---

## 📊 Expected Output Preview

```
==========================================================================================
 🌐 41. ENTERPRISE CUSTOMER 360 PIPELINE (23BTRDC002 - 17/08/2026)
==========================================================================================

[STEP 3] Identity Resolution & Conflict Resolution Engine
+------------------+---------------+-------------------------+----------+------------+
|golden_customer_id|master_name    |norm_email               |norm_phone|loyalty_tier|
+------------------+---------------+-------------------------+----------+------------+
|CUST-GOLD-1       |Alice Johnson  |alice.johnson@gmail.com  |5550101   |Gold        |
|CUST-GOLD-2       |Bob Smith      |bob.smith@techcorp.io    |5550202   |Silver      |
|CUST-GOLD-3       |Charles Brown  |charlie.brown@yahoo.com  |5550303   |Platinum    |
|CUST-GOLD-4       |David Wilson   |david.w@outlook.com      |5550404   |Bronze      |
|CUST-GOLD-5       |Emma Davis     |emma.davis@icloud.com    |5550505   |Gold        |
|CUST-GOLD-6       |Frank Miller   |frank.miller@gmail.com   |5550606   |Bronze      |
+------------------+---------------+-------------------------+----------+------------+

--- 🌟 FINAL UNIFIED CUSTOMER 360 DATAFRAME ---
+------------------+---------------+------------+--------------------+------------------+--------------------------+--------------------+-------------+--------------+---------------------------+
|golden_customer_id|master_name    |loyalty_tier|total_lifetime_spend|total_transactions|preferred_shopping_channel|open_support_tickets|avg_csat_score|is_omnichannel|customer_360_segment       |
+------------------+---------------+------------+--------------------+------------------+--------------------------+--------------------+-------------+--------------+---------------------------+
|CUST-GOLD-1       |Alice Johnson  |Gold        |1024.99             |3                 |Web                       |0                   |4.5          |true          |💎 High-Value VIP          |
|CUST-GOLD-2       |Bob Smith      |Silver      |335.0               |2                 |Furniture                 |0                   |4.0          |true          |⭐ Standard Active         |
|CUST-GOLD-3       |Charles Brown  |Platinum    |439.5               |2                 |Mobile App                |0                   |5.0          |true          |💎 High-Value VIP          |
|CUST-GOLD-4       |David Wilson   |Bronze      |120.0               |1                 |Web                       |1                   |3.0          |false         |⚠️ At-Risk / Needs Attention|
|CUST-GOLD-5       |Emma Davis     |Gold        |515.0               |2                 |Store                     |0                   |5.0          |true          |💎 High-Value VIP          |
|CUST-GOLD-6       |Frank Miller   |Bronze      |35.0                |1                 |Store                     |1                   |2.0          |false         |⚠️ At-Risk / Needs Attention|
+------------------+---------------+------------+--------------------+------------------+--------------------------+--------------------+-------------+--------------+---------------------------+
```

---

## ❓ Viva & Interview Questions

1. **Q: What is Customer 360 and why is Apache Spark ideal for it?**
   - **A:** Customer 360 is the practice of consolidating fragmented customer touchpoints (CRM, sales, support, digital web/app) into a single holistic customer profile. Apache Spark is ideal because it scales horizontally across petabytes of disparate structured, semi-structured, and streaming datasets with high-performance distributed joins.

2. **Q: How does identity resolution handle conflicting data across systems?**
   - **A:** Identity resolution establishes a Source-of-Truth Hierarchy. In our pipeline, CRM holds authority for legal names and loyalty tier, while transaction timestamp windows determine channel preference and latest activity.

3. **Q: Why are Left Outer Joins used when building the Customer 360 DataFrame?**
   - **A:** Because not every registered customer has an open support ticket, mobile session, or completed transaction. Inner joins would drop customers with zero tickets or zero app sessions, whereas left outer joins retain all golden customers while imputing appropriate baseline metrics (e.g., $0 spend, 0 tickets).
