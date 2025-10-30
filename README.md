# AppLovin Query Planner Challenge

## Introduction

This repository contains a solution for the AppLovin Query Planner Challenge from Cal Hacks 12.0 I attempted in my free time.<br>
The goal was to simulate the database and query optimizations needed to power real-time analytics on impressions, installs, purchases, bids, etc... for over 1.6 billion people worldwide to see an AppLovin ad.<br>
A 20GB dataset and set of simple SQL-like queries was given<br>
The solution was benchmarked against a provided DuckDB baseline and achieved a **2,800× speedup** with **42ms total query time**, optimizing two phases.

- **Prepare phase**: Transform, index, and pre-aggregate the raw data
- **Run phase**: Intelligent query planning. Generating correct results as quickly as possible using your optimized design - as long as it all runs self-contained (without outbound network calls)

However, it should be noted that it was not run on the judges queries so a broader category of queries would need to be benchmarked against to test this solution for generalization. This could be done with more generalized materialized views with better coverage than the current views targeted specifically towards inputs.py or a cost-based approach with statistics collected during prepare phase. Regardless of the DuckDB initial optimizations, fallback to full table scans from assembler.py's base query plan should be avoided entirely.

**Challenge Details:** The full challenge details can be found at [https://tinyurl.com/applovin-query-planner](https://tinyurl.com/applovin-query-planner)

**Data Source:** The datasets (`data.zip` for 20GB and `data-lite.zip` for 1GB) are available in the challenge Google Drive folder linked above.

**Benchmark Environment:** All performance benchmarks were run on a **MacBook Air M2** with **16GB RAM** and **256GB SSD**.

**Baseline Implementation:** The files `assembler.py`, `inputs.py`, and `main.py` is the baseline DuckDB implementation provided as starter code.

---

## Setup

### Environment Setup

Create and activate a Python virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Usage Guide

This repository contains two benchmark scripts for the Query Planner Challenge:

---

## 1. DuckDB Baseline: `main.py`

The baseline script (`main.py`) demonstrates a DuckDB-based benchmark that reads event data from CSV files, adds derived day/minute columns, executes JSON queries, and reports timings—without any advanced optimizations or data preprocessing.

**How to run:**

```bash
python main.py --data-dir ./data --out-dir ./results
```

- `--data-dir` (required): Path to the directory with the input CSV files (`events_part_*.csv`).
- `--out-dir` (required): Path to the directory where query results (`q1.csv`, `q2.csv`, ...) will be written.

---

## 2. Optimized Solution: `query-planner-challenge.py`

This is the optimized query planner. It features two-phases:

- **Prepare phase**: Streams CSV to Parquet columnar storage, partitions by event type for predicate pushdown, and pre-computes aggregations at multiple granularities (materialized views).
- **Run phase**: Analyzes and rewrites queries to use optimal data sources, routing to pre-computed aggregates when possible, and falls back to full partitioned Parquet scan otherwise.

**Typical usage:**

### Prepare phase only:

```bash
python query-planner-challenge.py --data-dir ./data --prepared-dir ./prepared --prepare
```

### Run phase only (after preparing):

```bash
python query-planner-challenge.py --prepared-dir ./prepared --out-dir ./results --run
```

### Both phases sequentially:

```bash
python query-planner-challenge.py --data-dir ./data --prepared-dir ./prepared --out-dir ./results --prepare --run
```

**Arguments:**

- `--data-dir`: Input directory with CSV files (**needed for `--prepare`**).
- `--prepared-dir`: Directory to store Parquet and aggregates (required).
- `--out-dir`: Output directory for final query results (**needed for `--run`**).
- `--prepare`: Run the prepare phase (conversion/building aggregates).
- `--run`: Run the query execution phase using optimized data layout.

---

## Output

- Results for each query are saved as `q{N}.csv` in the specified output directory.
- Run times and summary stats are reported in the terminal.

---

## Benchmark Results

### 1GB Dataset (data-lite)

**Baseline (`main.py`):**

| Query     | Time       | Rows  |
| --------- | ---------- | ----- |
| Q1        | 0.986s     | 365   |
| Q2        | 0.704s     | 826   |
| Q3        | 0.629s     | 12    |
| Q4        | 0.672s     | 3,713 |
| Q5        | 0.817s     | 1,440 |
| **Total** | **3.809s** |       |

**Optimized (`query-planner-challenge.py`):**

| Phase   | Time   |
| ------- | ------ |
| Prepare | 6.687s |

| Query     | Time       | Rows  | Speedup  |
| --------- | ---------- | ----- | -------- |
| Q1        | 0.001s     | 365   | **986×** |
| Q2        | 0.017s     | 826   | **41×**  |
| Q3        | 0.001s     | 12    | **629×** |
| Q4        | 0.003s     | 3,713 | **224×** |
| Q5        | 0.006s     | 1,440 | **136×** |
| **Total** | **0.026s** |       | **146×** |

### 20GB Dataset (data-full)

**Baseline (`main.py`):**

| Query     | Time         | Rows  |
| --------- | ------------ | ----- |
| Q1        | 23.643s      | 366   |
| Q2        | 23.057s      | 1,114 |
| Q3        | 22.807s      | 12    |
| Q4        | 24.122s      | 6,616 |
| Q5        | 23.952s      | 1,440 |
| **Total** | **117.581s** |       |

**Optimized (`query-planner-challenge.py`):**

| Phase   | Time     |
| ------- | -------- |
| Prepare | 119.466s |

| Query     | Time       | Rows  | Speedup     |
| --------- | ---------- | ----- | ----------- |
| Q1        | 0.001s     | 366   | **23,643×** |
| Q2        | 0.029s     | 1,114 | **795×**    |
| Q3        | 0.002s     | 12    | **11,403×** |
| Q4        | 0.005s     | 6,616 | **4,824×**  |
| Q5        | 0.005s     | 1,440 | **4,790×**  |
| **Total** | **0.042s** |       | **2,800×**  |

### Key Insights

- **Query execution speedup**: The optimized solution achieves **146× faster** query execution on the 1GB dataset and **2,800× faster** on the 20GB dataset.
- **Prepare overhead**: The one-time prepare phase takes 6.7s for 1GB and 119.5s for 20GB, but enables significant speedups for query execution (run phase).
- **Scalability**: The optimized solution maintains sub-50ms query times on the full 20GB dataset.
- **Techniques used**:
  - Columnar Parquet storage with ZSTD compression
  - Hive partitioning by event type for predicate pushdown
  - Pre-computed materialized views for common aggregations
  - Query rewriting to select optimal data sources
