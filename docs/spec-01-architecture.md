# Component Architecture & Execution Runbook: Spec 01 (`001-hdfs-and-rdd-ops`)

**Project**: Distributed Smart-Grid Demand & Tariff Analytics  
**Course**: 4th-Year Big Data Analytics Course Project  
**Team**: Ahmed Saad, Mohamed Abu-elhajjaj, Shrief Abdelmoati, Moustafa Mahmoud, Sameh Esam  
**Status**: 100% Implemented & Verified  

---

## 1. Architectural Highlights

### 1.1 Dual-Mode Hybrid Execution (Constitutional Principle II)
To adhere strictly to the **8GB RAM workstation guardrail** without compromising academic rigour:
1. **Local Windows Workstation**:
   - Intel Core i5-7200U, 8GB RAM.
   - `tests/conftest.py` implements an automatic zero-overhead fallback: if local Java/JVM initialization fails or is absent, it seamlessly provides a lightweight pure-Python `MockSparkSession` and `MockRDD`.
   - The complete test suite (12 tests) executes in **1.25 seconds** with **< 500 MB RAM** footprint.
2. **Google Colab Cloud Execution**:
   - Single-Node Pseudo-Distributed **Hadoop 3.4.1** running on Linux VM (`hdfs://localhost:9000`, `dfs.replication=1`).
   - PySpark `local[*]` with 4GB heap and HDFS integration.
   - Complete execution demonstrated in `notebooks/01_hdfs_and_rdd_ops.ipynb`.

### 1.2 The Three-Layer HDFS Storage (Constitutional Principle I)
- `/raw`: Immutable smart meter half-hourly readings and household metadata in CSV format.
- `/cleaned`: Parquet columnar tier (Spec 02 target).
- `/analytics`: Aggregated analytical tables and KPI summaries (Spec 03 target).

### 1.3 Explicit Schema Contract (Constitutional Principle III)
- Strictly avoids `inferSchema=True` on CSV blocks.
- Schemas declared in `src/spark_config.py`:
  - `METER_SCHEMA`: `LCLid` (String), `day` (String), `energy(kWh/hh)` (String).
  - `HOUSEHOLD_SCHEMA`: `LCLid` (String), `stdorToU` (String), `Acorn` (String), `Acorn_grouped` (String), `file` (String).
  - `WEATHER_SCHEMA`: `temperature` (Double), `humidity` (Double), `windSpeed` (Double), `time` (String), `summary` (String).

---

## 2. Implemented RDD Operations (`src/rdd_ops.py`)

All 7 core operations taught in Big Data Lab 1 are implemented as pure, testable worker functions:
1. **`filter_valid_meter_rows` (`filter`)**: Eliminates CSV headers, `"Null"` strings, negative values ($kwh < 0.0$), and malformed rows.
2. **`map_to_household_kwh_pairs` (`map`)**: Extracts `(LCLid, float(kwh))` key-value pairs.
3. **`extract_tariff_acorn_tokens` (`flatMap`)**: Decomposes household metadata into individual normalized tags.
4. **`find_peak_reading` (`reduce`)**: Employs an associative binary comparator to find the global peak half-hourly consumption.
5. **`group_readings_by_household` (`groupByKey`)**: Groups readings per household into lists for distribution inspection.
6. **`sum_kwh_by_household` (`reduceByKey`)**: Computes household cumulative consumption with map-side combine optimization.
7. **`join_and_aggregate_by_tariff` (`join`)**: Joins consumption totals with household tariff data (`Std` vs. `ToU`) and derives summary totals, counts, and averages with Driver memory protection ($\le 100$ rows).

---

## 3. Local Verification Runbook

```powershell
# Run all unit tests locally (< 2 seconds)
pytest tests/ -v
```

**Results**:
```text
tests/test_local_guardrails.py::test_local_test_execution_time_and_memory PASSED
tests/test_local_guardrails.py::test_sample_data_edge_case_coverage PASSED
tests/test_rdd_ops.py::test_filter_valid_meter_rows PASSED
tests/test_rdd_ops.py::test_map_to_household_kwh_pairs PASSED
tests/test_rdd_ops.py::test_extract_tariff_acorn_tokens PASSED
tests/test_rdd_ops.py::test_find_peak_reading PASSED
tests/test_rdd_ops.py::test_group_readings_by_household PASSED
tests/test_rdd_ops.py::test_sum_kwh_by_household PASSED
tests/test_rdd_ops.py::test_join_and_aggregate_by_tariff PASSED
tests/test_spark_config.py::test_hdfs_path_constants PASSED
tests/test_spark_config.py::test_spark_session_config_guardrails PASSED
tests/test_spark_config.py::test_explicit_schemas_no_infer PASSED

12 passed in 1.25s
```

---

## 4. Google Colab Lab Execution Runbook

1. Open [Google Colab](https://colab.research.google.com).
2. Upload `notebooks/01_hdfs_and_rdd_ops.ipynb`.
3. Click **Runtime -> Run all**.
4. The notebook automatically:
   - Provisions Java 11 and Apache Hadoop 3.4.1.
   - Configures `core-site.xml` (`hdfs://localhost:9000`) and `hdfs-site.xml` (`dfs.replication=1`).
   - Formats the NameNode and starts HDFS daemons.
   - Creates `/raw`, `/cleaned`, and `/analytics`.
   - Ingests mock meter CSVs into `/raw`.
   - Executes all 7 RDD operations and displays formatted comparison tables for `Std` vs. `ToU` tariffs.
