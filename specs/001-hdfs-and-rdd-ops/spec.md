# Feature Specification: Spec 01 — Repository Architecture Foundation, Explicit Schemas, and Phase 1 HDFS & PySpark RDD Operations

**Feature Branch**: `001-hdfs-and-rdd-ops`

**Created**: 2026-11-14

**Status**: Draft

**Input**: User description: "Build Spec 01 ('01-hdfs-and-rdd-ops'): Repository Architecture Foundation, Explicit Schemas, and Phase 1 HDFS & PySpark RDD Operations for the Smart Meters in London dataset."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Core Distributed RDD Processing & Transformation Pipeline (Priority: P1)

As a Big Data student and data engineer, I want to process raw energy consumption logs and household metadata using PySpark low-level Resilient Distributed Dataset (RDD) transformations so that corrupted records are cleanly filtered, key-value pairs are extracted, and consumption metrics are aggregated per household and tariff across distributed partitions.

**Why this priority**: Implements the primary computational core of Phase 1 and fulfills the fundamental Big Data course lab requirements by proving proficiency with the 7 core RDD operations on distributed smart meter data.

**Independent Test**: Can be fully tested locally against `sample_meters.csv` and `sample_households.csv` by invoking each RDD transformation in isolation and asserting that all 7 operations produce expected, mathematically verifiable outputs without runtime errors.

**Acceptance Scenarios**:

1. **Given** raw CSV text records containing header lines, valid readings, and literal `"Null"` strings, **When** the RDD filter operation is executed, **Then** all header lines and `"Null"` readings are discarded while valid numerical records are retained.
2. **Given** cleaned meter text records, **When** RDD `map` is applied, **Then** structured tuples of `(LCLid, float(kwh))` and `(LCLid, (timestamp, float(kwh)))` are emitted.
3. **Given** mapped `(LCLid, kwh)` pairs, **When** `reduceByKey` is executed with an addition operator, **Then** an RDD of `(LCLid, total_kwh)` is computed using map-side combine without driver memory bottlenecks.
4. **Given** mapped `(LCLid, kwh)` pairs, **When** `groupByKey` is executed, **Then** all readings for each household are grouped into an iterable sequence for comparative behavioral analysis.
5. **Given** all numerical consumption readings, **When** RDD `reduce` is executed with a maximum comparator, **Then** the single highest half-hourly consumption value across the entire dataset is returned to the driver.
6. **Given** household metadata RDD keyed by `LCLid` and meter consumption RDD keyed by `LCLid`, **When** RDD `join` is executed, **Then** an enriched paired RDD `(LCLid, (kwh, tariff_details))` is produced containing combined records for matching households.
7. **Given** complex multi-field string lines, **When** RDD `flatMap` is executed, **Then** decomposed individual tokens or diagnostic events are flattened into a continuous 1D stream of records.

---

### User Story 2 - Centralized Configuration & Explicit Schema Enforcement (Priority: P2)

As a Big Data engineer, I want a centralized Spark configuration module that provides standard Spark session initialization and strictly typed `StructType` schemas for all London Smart Meter tables, so that the pipeline completely avoids double-pass `inferSchema=True` disk I/O and enforces consistent types across local and cloud environments.

**Why this priority**: Guarantees adherence to Constitutional Principle III (Explicit Schema Contract) and Principle I (Three-Layer Storage Hierarchy), establishing the single source of truth for all downstream specs.

**Independent Test**: Can be verified by instantiating a Spark session via `get_spark_session()` and asserting that reading `halfhourly_logs`, `informations_households`, and `weather_hourly` using the exported schemas loads exact column names and datatypes without triggering automatic type inference.

**Acceptance Scenarios**:

1. **Given** the `spark_config` module, **When** `get_spark_session("test_app", "local[2]")` is called, **Then** an active `SparkSession` is returned with configured driver/executor memory limits and timezone settings.
2. **Given** raw CSV files for meter logs, household info, and weather data, **When** loaded into Spark DataFrames using `METER_SCHEMA`, `HOUSEHOLD_SCHEMA`, and `WEATHER_SCHEMA`, **Then** the schema matches the exact column names, nullability constraints, and data types without executing a pre-scan `inferSchema` job.
3. **Given** HDFS directory constants defined in `spark_config`, **When** referenced by pipeline jobs, **Then** paths resolve unambiguously to `/raw`, `/cleaned`, and `/analytics`.

---

### User Story 3 - Safe Local Testing & Mock Fixture Execution (Priority: P3)

As a developer working on a resource-constrained 8GB RAM Windows workstation, I want lightweight mock CSV fixtures ($\le$ 100 rows) covering normal records and dirty edge cases so that I can execute the complete unit test suite in under 2 seconds without risking system freezing, memory thrashing, or JVM Out-Of-Memory errors.

**Why this priority**: Directly enforces Constitutional Principle II (Local Hardware Protection & 8GB RAM Guardrail), enabling rapid, test-driven iteration on the local machine before cloud deployment.

**Independent Test**: Can be tested by running `pytest tests/` in Windows PowerShell and verifying that all tests execute in < 2 seconds with memory consumption remaining well below 500 MB RAM.

**Acceptance Scenarios**:

1. **Given** `tests/sample_data/sample_meters.csv`, **When** inspected, **Then** it contains exactly $\le$ 100 rows containing realistic `LCLid` identifiers, ISO timestamps, valid kWh floats, zero/negative values, and literal `"Null"` values.
2. **Given** `tests/sample_data/sample_households.csv`, **When** inspected, **Then** it contains $\le$ 100 rows with both `Std` (Standard) and `ToU` (Time-of-Use) tariffs across valid ACORN demographic categories.
3. **Given** the test suite executed locally, **When** running unit tests against these mock fixtures, **Then** all tests pass in < 2 seconds without downloading any gigabyte-scale datasets.

---

### User Story 4 - Cloud Lab Ingestion & Single-Node HDFS Setup (Priority: P4)

As a Big Data course student, I want an executable, step-by-step Google Colab notebook (`notebooks/01_hdfs_and_rdd_ops.ipynb`) that provisions a single-node pseudo-distributed Hadoop HDFS cluster and runs the complete Phase 1 RDD lab, so that full-scale execution can be demonstrated and reproduced in a cloud environment.

**Why this priority**: Fulfills the practical academic exam requirement (Spec 01 Checkpoint) by providing the reproducible demonstration artifact for course instructors and teammates.

**Independent Test**: Can be tested by executing `01_hdfs_and_rdd_ops.ipynb` end-to-end in Google Colab, verifying that Hadoop daemons start successfully, `/raw` directory is created, CSV files are uploaded, and RDD operations display correct outputs.

**Acceptance Scenarios**:

1. **Given** a fresh Google Colab Linux runtime, **When** the environment setup script runs, **Then** Hadoop 3.x is configured in pseudo-distributed mode (`localhost:9000`, `dfs.replication=1`) with NameNode and DataNode daemons running.
2. **Given** running HDFS daemons, **When** the ingestion cells execute, **Then** the three-layer HDFS directories (`/raw`, `/cleaned`, `/analytics`) are initialized and raw CSV files are uploaded to `/raw` without in-place mutation.
3. **Given** data uploaded to `hdfs://localhost:9000/raw`, **When** the notebook executes the 7 RDD operations, **Then** the results are printed cleanly and all driver collections are bounded to $\le$ 100 rows.

---

### Edge Cases

- **Literal `"Null"` string tokens**: Smart meter CSVs frequently encode missing energy readings as the literal string `"Null"` rather than empty values; these must be identified and removed during RDD filtering to avoid parse exceptions during float conversion.
- **Malformed CSV records**: Records with missing fields, unexpected delimiters, or corrupted timestamps must be caught safely by pure parsing functions without crashing distributed worker tasks.
- **Zero and negative consumption values**: Readings where `kwh <= 0.0` or invalid sensor spikes must be flagged or handled systematically.
- **Unmatched keys in RDD Join**: Households present in the meter log table that do not exist in the household metadata table must be evaluated to ensure inner vs. outer join semantics do not lead to silent data loss.
- **Empty input partitions**: Transformation functions must operate safely when an RDD partition contains zero rows after filtering.
- **Driver OOM prevention**: Inadvertent invocation of `.collect()` on multi-million row datasets must be blocked; only aggregated metrics ($\le$ 100 rows) may be retrieved to the driver.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Project MUST establish a clean, standardized directory structure containing `src/`, `notebooks/`, `tests/sample_data/`, and `docs/`.
- **FR-002**: Project MUST maintain a root `README.md` documenting the architecture, three-layer HDFS flow (`/raw`, `/cleaned`, `/analytics`), London Smart Meter dataset structure, team members, and incremental course checkpoints.
- **FR-003**: Repository MUST include a production-grade `.gitignore` preventing `.agents/`, `.venv/`, `__pycache__/`, `.ipynb_checkpoints/`, and all `.csv`/`.parquet` data files from being tracked in Git.
- **FR-004**: Project MUST provide a `requirements.txt` specifying exact dependencies for local development and testing (`pyspark`, `pytest`).
- **FR-005**: Centralized module `src/spark_config.py` MUST provide a `get_spark_session(app_name, master)` factory function configurable for local test runs and cloud cluster execution.
- **FR-006**: Centralized module `src/spark_config.py` MUST export explicit `StructType` schemas for `METER_SCHEMA`, `HOUSEHOLD_SCHEMA`, and `WEATHER_SCHEMA`, strictly prohibiting `inferSchema=True`.
- **FR-007**: Centralized module `src/spark_config.py` MUST define constants for the three HDFS directory layers: `HDFS_RAW_DIR`, `HDFS_CLEANED_DIR`, and `HDFS_ANALYTICS_DIR`.
- **FR-008**: Test suite MUST provide `tests/sample_data/sample_meters.csv` ($\le$ 100 rows) with realistic `LCLid`, timestamps, valid float kWh, zero/negative readings, and `"Null"` string literals.
- **FR-009**: Test suite MUST provide `tests/sample_data/sample_households.csv` ($\le$ 100 rows) with valid `LCLid`, `stdorToU` tariff designations (`Std` and `ToU`), and ACORN classifications.
- **FR-010**: Module `src/rdd_ops.py` MUST implement a pure filter function `filter_valid_meter_records(rdd)` that eliminates CSV header rows, malformed rows, and literal `"Null"` values.
- **FR-011**: Module `src/rdd_ops.py` MUST implement a pure mapping function `map_meter_to_pair(rdd)` producing key-value pairs of `(LCLid, kwh_float)`.
- **FR-012**: Module `src/rdd_ops.py` MUST implement a pure flatMap function `flatmap_meter_tokens(rdd)` demonstrating token-level decomposition of raw records.
- **FR-013**: Module `src/rdd_ops.py` MUST implement a reduce action `find_max_consumption(rdd)` returning the global maximum kWh reading across the dataset.
- **FR-014**: Module `src/rdd_ops.py` MUST implement `sum_consumption_by_household_reduce(rdd)` using `reduceByKey` to compute total consumption per household with map-side combine efficiency.
- **FR-015**: Module `src/rdd_ops.py` MUST implement `group_consumption_by_household(rdd)` using `groupByKey` to enable distribution inspection and algorithmic comparison with `reduceByKey`.
- **FR-016**: Module `src/rdd_ops.py` MUST implement `join_meters_with_households(meter_rdd, household_rdd)` using RDD `join` to produce enriched `(LCLid, (kwh, tariff))` pairs.
- **FR-017**: All RDD operations in `src/rdd_ops.py` MUST be accompanied by comprehensive unit tests in `tests/test_rdd_ops.py` achieving 100% test pass against mock data.
- **FR-018**: Notebook `notebooks/01_hdfs_and_rdd_ops.ipynb` MUST provide self-contained, reproducible instructions to configure pseudo-distributed Hadoop HDFS, upload CSVs to `/raw`, and demonstrate the 7 core RDD operations with outputs bounded to $\le$ 100 rows.

### Key Entities *(include if feature involves data)*

- **Meter Record (`halfhourly_logs`)**: Raw or parsed smart meter record containing `LCLid` (household identifier string), `DateTime` (timestamp string/timestamp), and `KWH/hh` (half-hourly electricity consumption in kilowatt-hours as string/float).
- **Household Profile (`informations_households`)**: Household demographic and commercial metadata containing `LCLid` (household identifier string), `stdorToU` (tariff type: Standard `Std` vs. Time-of-Use `ToU`), `Acorn` (demographic group code), and `Acorn_grouped` (broad demographic category).
- **Weather Observation (`weather_hourly`)**: London hourly meteorological record containing `DateTime` (timestamp string/timestamp), `temperature` (float), `apparentTemperature` (float), `humidity` (float), `windSpeed` (float), and `summary` (weather condition string).
- **Enriched Household Consumption**: Paired analytical entity resulting from joining meter logs with household profiles, containing `LCLid`, `kwh`, `stdorToU`, and demographic category.
- **Household Consumption Aggregate**: Analytical summary tuple containing `LCLid`, `total_kwh` (float), and `record_count` (int).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Local test suite executes 100% of unit tests covering all 7 RDD operations and schema definitions in under 2 seconds on the 8GB RAM host.
- **SC-002**: Peak local memory consumption during test suite execution remains below 500 MB RAM, strictly protecting host hardware stability.
- **SC-003**: 100% of DataFrame and RDD data loading operations use explicit `StructType` contracts, with zero occurrences of `inferSchema=True`.
- **SC-004**: All 7 core RDD operations (`map`, `filter`, `flatMap`, `reduce`, `groupByKey`, `reduceByKey`, `join`) achieve 100% test pass rate with zero silent data corruption or unhandled `"Null"` exceptions.
- **SC-005**: 100% of unaggregated distributed transformations execute within Spark worker tasks; zero unaggregated rows are retrieved via driver-side `.collect()` or `.toPandas()`.
- **SC-006**: The cloud lab notebook successfully initializes single-node pseudo-distributed HDFS (`localhost:9000`), uploads raw CSV blocks to `/raw`, and executes the full Phase 1 demonstration pipeline in under 5 minutes on Google Colab.
- **SC-007**: 0 bytes of raw multi-gigabyte CSV data or binary runtime cache are committed to Git, verified by clean repository tracking and strict `.gitignore` rules.

## Assumptions

- **Target Language and Runtime**: Python 3.9+ with PySpark 3.x and Hadoop 3.x.
- **Local Development OS**: Windows 10/11 with PowerShell 5.1 / PowerShell 7+ and Git for Windows.
- **Cloud Execution Environment**: Google Colab standard free-tier Linux environment (2 vCPUs, ~12 GB RAM, ~25 GB disk) capable of running single-node pseudo-distributed Hadoop HDFS and PySpark in `local[*]` mode.
- **Dataset Origin**: London Smart Meter dataset (Kaggle / London Datastore) comprising half-hourly consumption readings, household tariff metadata, and weather observations.
- **Course Alignment**: Spec 01 directly addresses the Practical Lab 1 exam scope (HDFS administration and foundational RDD operations) of the 4th-Year Big Data Analytics curriculum.
