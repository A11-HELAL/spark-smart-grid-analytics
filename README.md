# Distributed Smart-Grid Demand & Tariff Analytics

> **4th-Year Big Data Analytics Course Project**  
> **Course Instructors & Academic Examination Checkpoint**

---

## 👥 Project Team

- **Ahmed Saad**
- **Mohamed Abu-elhajjaj**
- **Shrief Abdelmoati**
- **Moustafa Mahmoud**
- **Sameh Esam**

---

## 📌 Project Overview

This project implements a scalable, distributed Big Data analytics pipeline using **Apache Spark (PySpark)** and **Hadoop HDFS** to analyze large-scale electricity consumption patterns from the **Smart Meters in London** dataset (~2.5 GB / 30M+ rows). 

The primary business objective is to evaluate the impact of **Dynamic Time-of-Use (ToU) tariffs** versus **Standard (Std) flat-rate tariffs** on household peak load shifting, correlate demand with London meteorological patterns, and demonstrate high-performance distributed data engineering best practices.

---

## 🏛️ System Architecture: Three-Layer HDFS Storage

In accordance with **Constitutional Principle I**, all distributed data across Hadoop HDFS is strictly separated into three isolated tiers:

```
hdfs://localhost:9000/
├── /raw/                    # Tier 1: Immutable Ingested CSV Files
│   ├── halfhourly_dataset/  # London smart meter half-hourly logs (block_0.csv ... block_N.csv)
│   ├── informations_households.csv  # Metadata, tariff designations (Std vs. ToU), Acorn groups
│   └── weather_hourly_darksky.csv   # Hourly temperature, humidity, wind speed, precipitation
│
├── /cleaned/                # Tier 2: Validated Parquet Datasets (Spec 02)
│   └── (Snappy-compressed, missing values resolved, approxQuantile IQR outliers removed)
│
└── /analytics/              # Tier 3: Spark SQL Aggregated Analytical Marts (Spec 03)
    └── (KPI tables, hourly load curves, weather correlation summaries, ToU shift metrics)
```

> [!IMPORTANT]
> **Immutability of `/raw`**: Raw source CSV files in `/raw` are treated as strictly immutable and are never overwritten or mutated in place.

---

## 📊 Dataset Schema Overview

The project processes three core relational tables:

1. **`halfhourly_logs`** (`halfhourly_dataset/*.csv`):
   - `LCLid`: Household unique identifier (e.g., `MAC000002`).
   - `tstp` / `DateTime`: Half-hourly timestamp string.
   - `energy(kWh/hh)`: Electricity consumption in kilowatt-hours per half hour.
2. **`informations_households`** (`informations_households.csv`):
   - `LCLid`: Household identifier.
   - `stdorToU`: Tariff classification (`Std` = Standard flat-rate, `ToU` = Dynamic Time-of-Use).
   - `Acorn` / `Acorn_grouped`: Demographic classifications (`Affluent`, `Comfortable`, `Adversity`).
   - `file`: Ingestion batch identifier.
3. **`weather_hourly`** (`weather_hourly_darksky.csv`):
   - `time`: Observation timestamp.
   - `temperature`: Ambient temperature in Celsius.
   - `humidity`: Relative humidity $[0.0, 1.0]$.
   - `windSpeed`: Wind speed in m/s.
   - `summary`: Weather condition text.

---

## 🛡️ Engineering Guardrails & Hybrid Execution Model

To guarantee system stability on development workstations with constrained resources (e.g., Intel Core i5-7200U, 8GB RAM), the project enforces strict constitutional guardrails:

- **Local Development (8GB RAM Guardrail)**:
  - Local workstation runs **only** modular Python source authoring (`src/`), Git version tracking, and rapid unit tests with mock data (`tests/sample_data/` $\le$ 100 rows).
  - Local tests run with `local[2]`, `512m` driver heap, 2 shuffle partitions, and Spark UI disabled, executing in $< 3$ seconds.
- **Remote Cluster Execution (Google Colab)**:
  - Full-scale distributed jobs (~2.5 GB / 30M+ rows) execute in Google Colab Linux runtimes with single-node pseudo-distributed Hadoop 3.4.1 (`hdfs://localhost:9000`, `dfs.replication=1`) and PySpark `local[*]`.
- **Zero `inferSchema` Contract**:
  - `inferSchema=True` is strictly forbidden to prevent double-scanning multi-gigabyte disk blocks. All tables use explicit `StructType` schemas defined centrally in `src/spark_config.py`.
- **Driver Memory Protection**:
  - Unaggregated `.collect()` or `.toPandas()` calls are prohibited. All transformations run in worker partitions; only aggregated KPI tables ($\le$ 100 rows) are retrieved to the Driver.

---

## 🚀 Incremental Lab Checkpoint Roadmap

1. **Spec 01 (`001-hdfs-and-rdd-ops`)**:
   - Single-node HDFS pseudo-distributed cluster setup and block ingestion into `/raw`.
   - The 7 core low-level PySpark RDD operations (`filter`, `map`, `flatMap`, `reduce`, `groupByKey`, `reduceByKey`, `join`).
   - Explicit `StructType` contracts and mock test suite.
2. **Spec 02 (`02-cleaning-and-iqr`)**:
   - High-level DataFrame data cleansing, handling missing values and `"Null"` string literals.
   - Distributed IQR outlier filtering using `approxQuantile` across Spark workers.
   - CSV-to-Parquet columnar conversion with Snappy compression benchmark into `/cleaned`.
3. **Spec 03 (`03-spark-sql-and-eda`)**:
   - Optimized Spark SQL queries and Broadcast Joins across consumption, tariff, and weather tables.
   - Exploratory Data Analytics (EDA) evaluating Dynamic Time-of-Use peak load shifting.

---

## 🧪 Quickstart: Running Local Tests

```powershell
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run unit tests locally (< 3 seconds)
pytest tests/ -v
```
