# Project 3: Bootstrap Confidence Interval Estimation Using Spark (23BTRDC002)

## 📌 Experiment Header
- **Experiment No:** 27
- **Title:** Bootstrap Confidence Interval Estimation Using Spark
- **Course / Student ID:** `23BTRDC002`
- **Topic:** Distributed Statistical Resampling & Non-Parametric Estimation with PySpark

---

## 🎯 Problem Statement
> **"Implement distributed bootstrap resampling on a large dataset. Generate repeated bootstrap samples and estimate confidence intervals for selected statistics such as mean, median, or model accuracy. Compare bootstrap confidence intervals with conventional parametric intervals. Discuss the usefulness of bootstrap methods when theoretical distributional assumptions are uncertain."**

---

## 📚 Theory & Mathematical Formulation

### 1. The Bootstrap Principle
Bootstrap is a computer-intensive **non-parametric resampling technique** introduced by Bradley Efron (1979). Instead of assuming a theoretical parametric probability distribution (e.g., Gaussian / Normal), the empirical sample distribution $\hat{F}_n$ serves as the population proxy:
1. From sample $S$ of size $n$, draw a resample $S^*$ of size $n$ **with replacement**.
2. Calculate the statistic of interest $\hat{\theta}^* = g(S^*)$ (e.g., sample mean or sample median).
3. Repeat $B$ times (e.g., $B = 200$) to yield empirical distribution $\{\hat{\theta}^*_1, \hat{\theta}^*_2, \dots, \hat{\theta}^*_B\}$.
4. The **95% Bootstrap Percentile Confidence Interval** is given by:
   $$CI_{95\%} = \left[ \hat{\theta}^*_{(0.025)}, \; \hat{\theta}^*_{(0.975)} \right]$$

### 2. Conventional Parametric Confidence Interval
Under classical asymptotic normal assumptions (Central Limit Theorem):
$$\bar{x} \pm Z_{\alpha/2} \cdot \frac{s}{\sqrt{n}}$$
- **Limitation 1:** Assumes symmetric bell-shaped sampling distribution.
- **Limitation 2:** Fails on skewed distributions with heavy tails (e.g., server latencies, financial losses, delivery delays).
- **Limitation 3:** Cannot be directly applied to non-linear, non-analytical statistics like the **Median**.

---

## 💡 Usefulness of Bootstrap When Distributional Assumptions Are Uncertain

1. **Distribution-Free (Non-Parametric):** No need to verify normality or guess parametric distributions (exponential, Pareto, log-normal).
2. **Asymmetric Confidence Bounds:** Skewed raw data naturally produces asymmetric bootstrap confidence intervals, better capturing tail risk.
3. **Median & Percentiles:** Enables robust interval estimation for the **Median**, where classical closed-form standard error equations do not exist.
4. **Distributed Execution in Spark:** Resampling partitions can be parallelized natively across Spark worker executors using `df.sample(withReplacement=True, fraction=1.0)`.

---

## 📁 Directory Structure
```
project-3/
├── data/
│   └── delivery_times.csv     # Right-skewed e-commerce package delivery logs
├── output/
│   └── bootstrap_ci_summary/  # Exported comparative results (CSV)
├── main.py                    # PySpark bootstrap simulation script
├── run.sh                     # 1-click execution script
├── requirements.txt           # Dependencies (pyspark)
└── README.md                  # Complete lab experiment report
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
=====================================================================================
 📊 27. BOOTSTRAP CONFIDENCE INTERVAL ESTIMATION USING SPARK (23BTRDC002)
=====================================================================================

>>> STEP 1: Load Dataset & Compute Baseline Sample Metrics
Sample Size (n)       : 50
Sample Mean (x̄)       : 42.64 hours
Sample Std Dev (s)    : 34.20 hours
Standard Error (SE)   : 4.84 hours
Sample Median         : 29.10 hours

>>> STEP 2: Compute Conventional Parametric 95% Confidence Interval
Parametric Method (Assumes Normal Distribution):
  • 95% CI for Mean: [33.16, 52.12] hours
  • Note: No simple parametric formula exists for Median of skewed data.

>>> STEP 3: Execute Distributed Bootstrap Resampling (B = 200 Iterations)
Resampling with replacement across 200 iterations...

>>> STEP 4: Calculate Empirical Bootstrap 95% Confidence Intervals
Estimated empirical percentiles (2.5% and 97.5%) across bootstrap replications.

>>> STEP 5: Comparison - Parametric vs. Bootstrap Confidence Intervals
+--------------------+--------------+-------------------------+----------------+----------------+---------------+
|Statistic           |Point_Estimate|Parametric_95_CI         |Bootstrap_95_CI |Parametric_Width|Bootstrap_Width|
+--------------------+--------------+-------------------------+----------------+----------------+---------------+
|Mean Delivery Time  |42.64         |[33.16, 52.12]           |[34.22, 53.48]  |18.96           |19.26          |
|Median Delivery Time|29.10         |N/A (No Standard Formula)|[26.90, 32.40]  |N/A             |5.50           |
+--------------------+--------------+-------------------------+----------------+----------------+---------------+
```

---

## ❓ Lab Viva & Oral Questions

1. **Q: What is the purpose of `withReplacement=True` in bootstrap sampling?**
   - **A:** Sampling with replacement ensures that every original record has an equal and independent chance of being selected 0, 1, or multiple times in each bootstrap sample, correctly mimicking sampling from the underlying population.

2. **Q: Why is the confidence interval for the Median narrower than the Mean on skewed data?**
   - **A:** The Median is robust to heavy-tailed extreme outliers (such as delayed deliveries of 120+ hours), whereas the Mean is pulled upward by outliers, increasing its variance.

3. **Q: How does Apache Spark optimize bootstrap resampling?**
   - **A:** By caching the base DataFrame in memory (`df.cache()`), subsequent `.sample()` transformations read from memory without re-parsing CSV files from disk.
