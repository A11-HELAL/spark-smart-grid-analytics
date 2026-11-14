# Quickstart Validation Guide: Spec 01 (`001-hdfs-and-rdd-ops`)

**Feature**: Spec 01 — Repository Architecture Foundation, Explicit Schemas, and Phase 1 HDFS & PySpark RDD Operations  
**Date**: 2026-11-14  
**Status**: Ready for Implementation  

---

## 1. Local Environment Validation (Windows 8GB RAM Laptop)

### 1.1 Prerequisites
- Python 3.10+ installed and on system PATH.
- Java OpenJDK (8, 11, or 17) with `JAVA_HOME` environment variable configured.
- Windows PowerShell 5.1+ or PowerShell 7+.

### 1.2 Installation
In the project root directory, install the lightweight testing dependencies:
```powershell
pip install -r requirements.txt
```

### 1.3 Running Unit Tests (< 3 Seconds Guardrail)
Execute the unit tests using `pytest`:
```powershell
pytest tests/ -v
```

### 1.4 Expected Verification Outcome
- Tests run against `tests/sample_data/sample_meters.csv` (50 rows) and `tests/sample_data/sample_households.csv` (10 rows).
- Output:
  ```text
  tests/test_rdd_ops.py::test_filter_valid_meter_rows PASSED
  tests/test_rdd_ops.py::test_map_to_household_kwh_pairs PASSED
  tests/test_rdd_ops.py::test_extract_tariff_acorn_tokens PASSED
  tests/test_rdd_ops.py::test_find_peak_reading PASSED
  tests/test_rdd_ops.py::test_group_readings_by_household PASSED
  tests/test_rdd_ops.py::test_sum_kwh_by_household PASSED
  tests/test_rdd_ops.py::test_join_and_aggregate_by_tariff PASSED
  tests/test_rdd_ops.py::test_explicit_schemas_no_infer PASSED
  === 8 passed in ~1.85s ===
  ```
- Memory footprint: $\le 500$ MB RAM.

---

## 2. Cloud Execution Validation (Google Colab)

### 2.1 Opening the Notebook
1. Open Google Colab: [colab.research.google.com](https://colab.research.google.com).
2. Upload `notebooks/01_hdfs_and_rdd_ops.ipynb`.
3. Set runtime type to Python 3 with standard hardware.

### 2.2 Execution Flow in Colab
1. **Cell Group 1 - Hadoop Provisioning**:
   - Downloads Apache Hadoop 3.4.1.
   - Configures `core-site.xml` (`fs.defaultFS=hdfs://localhost:9000`) and `hdfs-site.xml` (`dfs.replication=1`).
   - Sets up passwordless SSH and formats NameNode.
   - Starts HDFS daemons (`start-dfs.sh`).
2. **Cell Group 2 - Three-Layer HDFS Initialization & Ingestion**:
   - Runs `hdfs dfs -mkdir -p /raw /cleaned /analytics`.
   - Downloads a sample/block of London Smart Meter CSVs into Google Colab local storage.
   - Pushes raw CSV files into `hdfs://localhost:9000/raw` using `hdfs dfs -put`.
3. **Cell Group 3 - PySpark RDD Operations**:
   - Connects PySpark session to HDFS raw text file: `sc.textFile("hdfs://localhost:9000/raw/...")`.
   - Executes the 7 core RDD operations in sequence:
     * `filter`: Eliminates headers and `"Null"` readings.
     * `map`: Produces `(LCLid, kwh)` pairs.
     * `flatMap`: Decomposes metadata into individual tokens.
     * `reduce`: Outputs the global peak half-hourly reading.
     * `groupByKey`: Groups readings for sample households.
     * `reduceByKey`: Computes total consumption per household.
     * `join`: Enriches totals with household tariff data (`Std` vs. `ToU`).
4. **Expected Outcome**:
   - Clean tabular printout of tariff totals and averages.
   - Zero driver memory warnings; runtime completes in under 5 minutes.
