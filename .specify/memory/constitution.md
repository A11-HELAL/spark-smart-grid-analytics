<!--
Sync Impact Report:
- Version change: Initial Template (0.0.0) → 1.0.0
- List of modified principles:
  * [PRINCIPLE_1_NAME] → I. Three-Layer HDFS Storage Separation (NON-NEGOTIABLE)
  * [PRINCIPLE_2_NAME] → II. Local Hardware Protection & Hybrid Execution (8GB RAM Guardrail)
  * [PRINCIPLE_3_NAME] → III. Explicit Schema Contract (Zero inferSchema)
  * [PRINCIPLE_4_NAME] → IV. Driver Memory Protection & Pure Functions
  * [PRINCIPLE_5_NAME] → V. Incremental Course Checkpoint Alignment
- Added sections:
  * Technical Constraints & Hardware Guardrails (replaces [SECTION_2_NAME])
  * Development Workflow & Quality Gates (replaces [SECTION_3_NAME])
  * Governance
- Removed sections:
  * None (initial scaffold populated)
- Follow-up TODOs:
  * None. All constitutional placeholders fully populated and ratified.
-->

# Distributed Smart-Grid Demand & Tariff Analytics Constitution

## Core Principles

### I. Three-Layer HDFS Storage Separation (NON-NEGOTIABLE)
All data stored on the Hadoop Distributed File System (HDFS) MUST be strictly partitioned into three isolated directory tiers:
1. `/raw`: Stores immutable, raw ingested CSV blocks as originally obtained from the Smart Meters in London dataset.
2. `/cleaned`: Stores validated, outlier-free, and cleaned datasets converted to compressed columnar Apache Parquet format.
3. `/analytics`: Stores final Spark SQL aggregated dimensional tables, key performance indicators, and analytical summary marts.

Raw source files in `/raw` MUST NEVER be overwritten, mutated in place, or appended to directly by processing jobs. All downstream transformations MUST read from `/raw` or `/cleaned` and write new immutable partitions to their corresponding tier.

*Rationale*: Enforces data immutability, auditability, and pipeline idempotency, preventing accidental data corruption and enabling reproducible batch processing across all lab runs.

### II. Local Hardware Protection & Hybrid Execution (8GB RAM Guardrail)
The local development workstation (Windows PowerShell, Intel Core i5-7200U, 8GB RAM) MUST NOT be used for running full-scale Spark analytics jobs or downloading multi-gigabyte raw datasets (~2.5 GB / 30M+ rows). Local resource usage is STRICTLY RESERVED for:
1. Authoring modular Python library source code under `src/`.
2. Executing lightweight unit tests against small mock data fixtures (<= 100 rows) located in `tests/sample_data/`.
3. Git version control, specification tracking, and documentation management.

All full-scale cluster workloads, multi-gigabyte HDFS operations, and compute-heavy distributed jobs MUST execute in Google Colab configured with PySpark `local[*]` and single-node Hadoop HDFS (`hdfs://localhost:9000`, `dfs.replication=1`).

*Rationale*: A dual-core 8GB host will encounter severe OS paging, memory thrashing, JVM Out-Of-Memory (OOM) fatal crashes, or system instability if exposed to multi-gigabyte distributed Spark executions.

### III. Explicit Schema Contract (Zero inferSchema)
Using `inferSchema=True` or relying on runtime automatic schema inference when loading CSV data is STRICTLY FORBIDDEN across all codebases and notebook scripts.

Every dataset table—specifically `halfhourly_logs`, `informations_households`, and `weather_hourly`—MUST have an explicit, strongly typed `StructType` schema defined centrally in `src/spark_config.py`. Date/time parsing, numerical coercions, and string field types MUST be explicitly validated at ingestion time.

*Rationale*: `inferSchema=True` forces a full, unindexed second pass over the multi-gigabyte raw CSV files on disk before actual job execution begins, doubling disk I/O, slowing cluster throughput, and causing unpredictable runtime type mismatches.

### IV. Driver Memory Protection & Pure Functions
Executing `.collect()`, `.toPandas()`, or `.take(N)` with large N on unaggregated RDDs, raw partitions, or unbounded DataFrames is STRICTLY FORBIDDEN. Only aggregated summary datasets containing no more than 100 rows may be collected to the Spark Driver program for final reporting or plotting.

All data cleaning operations, string normalization (including literal `"Null"` parsing and `Acorn` category extraction), and distributed outlier detection (using `approxQuantile` IQR boundaries) MUST be implemented as pure, side-effect-free, testable functions in `src/` and executed distributedly across Spark workers.

*Rationale*: Fetching millions of unaggregated rows across the network interface into the Spark Driver process instantly triggers JVM Heap `java.lang.OutOfMemoryError` failures and destabilizes the execution environment.

### V. Incremental Course Checkpoint Alignment
All engineering milestones and development workflows MUST align sequentially with the 3 practical course checkpoints and lab exam specifications:
1. **Spec 01 (`01-hdfs-and-rdd-ops`)**: Hadoop single-node HDFS initialization, ingestion verification into `/raw`, and correct implementation of the 7 core low-level RDD transformations (`map`, `filter`, `flatMap`, `reduce`, `groupByKey`, `reduceByKey`, `join`).
2. **Spec 02 (`02-cleaning-and-iqr`)**: High-level DataFrame data cleansing, handling missing values and string literal `"Null"` tokens, distributed IQR anomaly filtering via `approxQuantile`, and a comprehensive CSV-to-Parquet storage compression and read performance benchmark written to `/cleaned`.
3. **Spec 03 (`03-spark-sql-and-eda`)**: Spark SQL queries, optimized Broadcast Joins combining consumption logs with household tariff classifications (`stdorToU`) and London weather data, followed by exploratory data analytics to evaluate Dynamic Time-of-Use peak load shifting.

Each specification MUST be completed, tested, and verified independently before proceeding to the subsequent phase.

*Rationale*: Mirrors academic evaluation criteria, ensures progressive testability, and prevents technical debt from accumulating across distributed storage and processing layers.

## Technical Constraints & Hardware Guardrails

The project implements a hybrid execution architecture designed around strict hardware boundaries:
- **Local Host Workstation**: Windows PowerShell, Intel Core i5-7200U, 8 GB RAM. Limited strictly to source authoring, Git tracking, and mock unit testing.
- **Remote Cluster Environment**: Google Colab with PySpark master `local[*]`, Hadoop 3.x single-node filesystem configured at `hdfs://localhost:9000` with `dfs.replication=1`.
- **HDFS Directory Layout**:
  - `hdfs://localhost:9000/raw`: Ingested source datasets (`halfhourly_dataset/`, `informations_households.csv`, `weather_hourly_darksky.csv`).
  - `hdfs://localhost:9000/cleaned`: Outlier-free Parquet datasets with Snappy compression.
  - `hdfs://localhost:9000/analytics`: Aggregated summary marts, KPI tables, and analytical exports.
- **Dataset Scale**: ~2.5 GB compressed CSV / 30M+ rows of London Smart Meter readings.

## Development Workflow & Quality Gates

The engineering lifecycle enforces rigorous quality gates at every stage:
- **Test-Driven Local Verification**: All business logic and Spark transformation helpers in `src/` MUST be developed as pure functions and tested against 100-row mock data fixtures in `tests/sample_data/` using `pytest` before remote deployment.
- **Modular Code Architecture**: All production logic MUST reside in modular Python packages under `src/` (e.g., `src/spark_config.py`, `src/cleaning.py`, `src/rdd_ops.py`, `src/analytics.py`). Colab notebooks serve exclusively as orchestration runners that import these modules.
- **Repository Hygiene**: Multi-gigabyte datasets, runtime checkpoints, and notebook execution outputs MUST NOT be committed to Git. `.gitignore` MUST enforce exclusions for all raw data and heavy outputs.
- **Lab Checkpoint Gates**:
  - *Gate 1 (Spec 01)*: Verified HDFS block distribution in `/raw` and 100% unit/integration pass on the 7 core RDD operations.
  - *Gate 2 (Spec 02)*: Successful distributed IQR anomaly detection without Driver collection, coupled with verified Parquet compression metrics (>3x space reduction vs. raw CSV).
  - *Gate 3 (Spec 03)*: Validated Spark SQL Broadcast Join execution plans and final aggregated analytical outputs evaluating Dynamic Time-of-Use demand response.

## Governance

This constitution defines the authoritative engineering standards for the **Distributed Smart-Grid Demand & Tariff Analytics** project (4th-Year Big Data Analytics Course Project).

- **Project Team**:
  - Ahmed Saad
  - Mohamed Abu-elhajjaj
  - Shrief Abdelmoati
  - Moustafa Mahmoud
  - Sameh Esam
- **Supremacy**: This constitution supersedes all informal agreements, ad-hoc notebook workflows, and individual script practices. All code contributions MUST conform to these rules.
- **Amendment Procedure**: Amendments require unanimous consensus among all 5 team members. Any proposed change must include a documented rationale, assess the impact on hardware memory safety and course checkpoint delivery, and update this document with an incremented version and amendment date.
- **Versioning Policy**: Semantic versioning is strictly enforced:
  - *MAJOR*: Incompatible governance or principle changes (e.g., altering HDFS tier structure or removing memory limits).
  - *MINOR*: Addition of new principles, lab specifications, or materially expanded technical constraints.
  - *PATCH*: Non-semantic wording refinements, typo corrections, and formatting clarifications.
- **Compliance & Review**: Every Pull Request and code submission MUST be audited against the 5 non-negotiable principles. Commits using `inferSchema=True`, performing unaggregated `.collect()`, or attempting local multi-GB downloads MUST be rejected immediately.

**Version**: 1.0.0 | **Ratified**: 2026-11-14 | **Last Amended**: 2026-11-14