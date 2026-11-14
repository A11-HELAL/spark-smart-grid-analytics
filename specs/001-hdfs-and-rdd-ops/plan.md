# Implementation Plan: Spec 01 — Repository Architecture Foundation, Explicit Schemas, and Phase 1 HDFS & PySpark RDD Operations

**Branch**: `001-hdfs-and-rdd-ops` | **Date**: 2026-11-14 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-hdfs-and-rdd-ops/spec.md`

## Summary

Establish the foundational Big Data project skeleton, centralized explicit schema definitions (`METER_SCHEMA`, `HOUSEHOLD_SCHEMA`, `WEATHER_SCHEMA`), lightweight 100-row mock test fixtures, pure RDD transformation modules implementing the 7 core lab operations (`map`, `filter`, `flatMap`, `reduce`, `groupByKey`, `reduceByKey`, `join`), and an end-to-end pseudo-distributed Hadoop 3.4.1 HDFS execution notebook on Google Colab. The architecture strictly adheres to the 8GB RAM host guardrails and the Three-Layer HDFS storage hierarchy.

## Technical Context

**Language/Version**: Python 3.10+  
**Primary Dependencies**: `pyspark>=3.4.0`, `pytest>=7.4.0`  
**Storage**: Three-layer HDFS hierarchy (`/raw`, `/cleaned`, `/analytics`) running on Apache Hadoop 3.4.1 pseudo-distributed cluster (`hdfs://localhost:9000`, `dfs.replication=1`); local mock storage in `tests/sample_data/`  
**Testing**: `pytest` running with Spark local test mode (`master="local[2]"`, `spark.driver.memory="512m"`, `spark.sql.shuffle.partitions="2"`, `spark.ui.enabled="false"`)  
**Target Platform**: Hybrid: Local development on Windows PowerShell (Intel Core i5-7200U, 8GB RAM); Distributed cloud execution on Google Colab (Linux, PySpark `local[*]`, Hadoop 3.4.1)  
**Project Type**: Distributed Big Data analytics library & practical course lab pipeline  
**Performance Goals**: Local unit test suite executes in $\le 2$ seconds on 8GB RAM laptop; peak test memory $\le 500$ MB RAM; Google Colab end-to-end HDFS setup + RDD execution completes in $\le 5$ minutes  
**Constraints**: 0 occurrences of `inferSchema=True`; 0 unaggregated rows collected to Driver; 0 bytes of large data committed to Git  
**Scale/Scope**: Local mock tests: 50 rows `sample_meters.csv`, 10 rows `sample_households.csv`; Production dataset: ~2.5 GB / 30M+ rows of London Smart Meter readings  

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle / Gate | Assessment | Status | Notes |
|---|---|---|---|
| **I. Three-Layer HDFS Storage** | Verified `/raw`, `/cleaned`, and `/analytics` directories defined in `src/spark_config.py`. Immutability of `/raw` preserved. | **PASS** | No in-place mutations permitted. |
| **II. 8GB RAM Guardrail** | Local execution restricted to `local[2]` with 512MB heap and $\le 100$ rows mock fixtures. Cloud workloads isolated to Colab. | **PASS** | Prevents OS paging and JVM OOM crashes on Intel i5-7200U host. |
| **III. Explicit Schema Contract** | Explicit `StructType` schemas defined for `halfhourly_logs`, `informations_households`, and `weather_hourly`. | **PASS** | `inferSchema=True` strictly forbidden across all loaders. |
| **IV. Driver Memory Protection** | All transformations in `src/rdd_ops.py` are pure worker functions. Only aggregated summaries ($\le 100$ rows) collected to Driver. | **PASS** | Tariff summary outputs exactly 2 records to Driver. |
| **V. Lab Checkpoint Alignment** | Implements HDFS setup and all 7 core RDD operations (`map`, `filter`, `flatMap`, `reduce`, `groupByKey`, `reduceByKey`, `join`). | **PASS** | 100% compliant with Practical Lab 1 exam scope. |

## Project Structure

### Documentation (this feature)

```text
specs/001-hdfs-and-rdd-ops/
├── spec.md              # Feature specification
├── plan.md              # This implementation plan
├── research.md          # Technical research & architectural decisions
├── data-model.md        # Ingestion & in-memory RDD data models
├── quickstart.md        # Validation guide for local tests & Colab notebook
├── contracts/           # Component contracts
│   ├── spark_config.md  # SparkSession & schema contract
│   ├── rdd_ops.md       # The 7 core RDD operations contract
│   └── hdfs_storage.md  # Three-layer HDFS storage architecture
└── checklists/
    └── requirements.md  # Spec quality validation checklist
```

### Source Code (repository root)

```text
├── .gitignore                      # Git exclusion rules for venv, data, checkpoints
├── README.md                       # Architectural documentation & project overview
├── requirements.txt                # Python dependencies (pyspark, pytest)
├── src/
│   ├── __init__.py                 # Package initializer
│   ├── spark_config.py             # SparkSession factory, StructType schemas, HDFS constants
│   └── rdd_ops.py                  # Pure functions for the 7 core RDD operations
├── notebooks/
│   └── 01_hdfs_and_rdd_ops.ipynb   # Colab notebook: Hadoop 3.4.1 setup & Lab 1 execution
├── tests/
│   ├── __init__.py                 # Test package initializer
│   ├── conftest.py                 # Pytest fixture initializing lightweight SparkSession
│   ├── test_rdd_ops.py             # Test suite verifying schemas and the 7 RDD operations
│   └── sample_data/
│       ├── sample_meters.csv       # 50 rows mock meter readings with edge cases
│       └── sample_households.csv   # 10 rows mock household demographic & tariff data
└── docs/                           # Project documentation directory
```

**Structure Decision**: Standard modular Python library pattern paired with a dedicated `notebooks/` directory for cloud-based orchestration and `tests/sample_data/` for rapid local mock testing.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|---|---|---|
| *None* | All components strictly adhere to constitutional principles | No complex patterns introduced; pure functional transformations only |
