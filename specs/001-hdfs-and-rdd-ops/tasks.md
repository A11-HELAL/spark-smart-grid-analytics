# Tasks: Spec 01 — Repository Architecture Foundation, Explicit Schemas, and Phase 1 HDFS & PySpark RDD Operations

**Input**: Design documents from `specs/001-hdfs-and-rdd-ops/` (`spec.md`, `plan.md`, `research.md`, `data-model.md`, `contracts/`, `quickstart.md`)  
**Prerequisites**: `plan.md` (required), `spec.md` (required), `research.md`, `data-model.md`, `contracts/`  
**Constitution Alignment**: Enforces Principle I (Three-Layer HDFS), Principle II (8GB RAM Guardrail), Principle III (Zero inferSchema), Principle IV (Driver Memory Protection & Pure Functions), Principle V (Course Checkpoint Alignment).  

## Format: `- [ ] [TaskID] [P?] [Story?] Description with file path`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: Which user story this task belongs to ([US1], [US2], [US3], [US4])
- Clear, unambiguous file paths in every task description

---

## Phase 1: Setup (Shared Infrastructure & Repository Skeleton)

**Purpose**: Initialize directory skeleton, documentation, dependencies, and Git exclusion guardrails.

- [X] T001 Create project directory structure (`src/`, `notebooks/`, `tests/sample_data/`, `docs/`) per implementation plan
- [X] T002 [P] Create Python `.gitignore` in repository root excluding `.agents/`, `.venv/`, `__pycache__/`, `.ipynb_checkpoints/`, and all multi-gigabyte data files (`*.csv`, `*.parquet`, `*.tar.gz`)
- [X] T003 [P] Create `requirements.txt` in repository root with `pyspark>=3.4.0` and `pytest>=7.4.0`
- [X] T004 Create `src/__init__.py` and `tests/__init__.py` package initializers
- [X] T005 [P] Create root `README.md` documenting project overview, team roster (Ahmed Saad, Mohamed Abu-elhajjaj, Shrief Abdelmoati, Moustafa Mahmoud, Sameh Esam), Three-Layer HDFS storage layout (`/raw`, `/cleaned`, `/analytics`), and course checkpoint roadmaps

---

## Phase 2: Foundational (Blocking Prerequisites & Configuration)

**Purpose**: Core infrastructure, explicit schemas, mock fixtures, and memory-safe test session fixtures that MUST be complete before ANY user story can be implemented.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [X] T006 Create mock smart meter dataset `tests/sample_data/sample_meters.csv` (50 rows total including header `LCLid,day,energy(kWh/hh)`, 40 valid records across 5 households, 5 literal `"Null"` strings, 3 negative readings `kwh < 0.0`, and 2 malformed rows)
- [X] T007 [P] Create mock household demographic dataset `tests/sample_data/sample_households.csv` (10 rows total including header `LCLid,stdorToU,Acorn,Acorn_grouped,file`, covering both `"Std"` and `"ToU"` tariffs and ACORN categories)
- [X] T008 Implement HDFS path constants in `src/spark_config.py` (`HDFS_BASE_URL = "hdfs://localhost:9000"`, `HDFS_RAW_DIR = "/raw"`, `HDFS_CLEANED_DIR = "/cleaned"`, `HDFS_ANALYTICS_DIR = "/analytics"`)
- [X] T009 Implement explicit `StructType` schemas in `src/spark_config.py`:
  - `METER_SCHEMA`: `"LCLid: StringType, nullable=False"`, `"tstp: StringType, nullable=False"`, `"energy(kWh/hh): StringType, nullable=True"`
  - `HOUSEHOLD_SCHEMA`: `"LCLid: StringType, nullable=False"`, `"stdorToU: StringType, nullable=False (enum: 'Std' | 'ToU')"`, `"Acorn: StringType, nullable=True"`, `"Acorn_grouped: StringType, nullable=True"`, `"file: StringType, nullable=True"`
  - `WEATHER_SCHEMA`: `"temperature: DoubleType, nullable=True"`, `"humidity: DoubleType, nullable=True"`, `"windSpeed: DoubleType, nullable=True"`, `"time: StringType, nullable=False"`, `"summary: StringType, nullable=True"`
- [X] T010 Implement `get_spark_session(app_name, is_local_test)` factory in `src/spark_config.py` with local 8GB guardrails (`master("local[2]")`, `spark.driver.memory="512m"`, `spark.sql.shuffle.partitions="2"`, `spark.ui.enabled="false"`)
- [X] T011 Create pytest session fixture in `tests/conftest.py` providing a shared lightweight `SparkSession` using `get_spark_session(is_local_test=True)`

**Checkpoint**: Foundation ready — memory-safe local test environment and explicit schema definitions verified.

---

## Phase 3: User Story 1 - Core Distributed RDD Processing & Transformation Pipeline (Priority: P1) 🎯 MVP

**Goal**: Implement and verify the 7 core low-level PySpark RDD operations (`filter`, `map`, `flatMap`, `reduce`, `groupByKey`, `reduceByKey`, `join`) on smart meter data using pure functions.

**Independent Test**: Run `pytest tests/test_rdd_ops.py -v` against `tests/sample_data/` and verify that all 7 RDD operations execute and pass in $\le 2$ seconds without runtime errors or `"Null"` float conversion crashes.

### Tests for User Story 1 ⚠️

> **NOTE: Write these tests FIRST in `tests/test_rdd_ops.py` and ensure they FAIL before implementation.**

- [ ] T012 [P] [US1] Write unit test for `filter_valid_meter_rows` in `tests/test_rdd_ops.py` asserting header, `"Null"` strings, negative values, and malformed rows are discarded
- [ ] T013 [P] [US1] Write unit test for `map_to_household_kwh_pairs` in `tests/test_rdd_ops.py` asserting emission of `(LCLid, float(kwh))` tuples
- [ ] T014 [P] [US1] Write unit test for `extract_tariff_acorn_tokens` in `tests/test_rdd_ops.py` asserting flattened token stream from household metadata
- [ ] T015 [P] [US1] Write unit test for `find_peak_reading` in `tests/test_rdd_ops.py` asserting correct global maximum `(LCLid, max_kwh)` identification
- [ ] T016 [P] [US1] Write unit test for `group_readings_by_household` in `tests/test_rdd_ops.py` asserting grouping into `(LCLid, [kwh_1, kwh_2, ...])`
- [ ] T017 [P] [US1] Write unit test for `sum_kwh_by_household` in `tests/test_rdd_ops.py` asserting map-side combined totals equal expected arithmetic sums
- [ ] T018 [P] [US1] Write unit test for `join_and_aggregate_by_tariff` in `tests/test_rdd_ops.py` asserting correct total and average consumption calculations for `"Std"` and `"ToU"`

### Implementation for User Story 1

- [ ] T019 [US1] Implement pure function `filter_valid_meter_rows(raw_rdd)` in `src/rdd_ops.py` using `filter()` to discard headers, malformed rows, `"Null"` strings, and negative values ($kwh < 0.0$)
- [ ] T020 [US1] Implement pure function `map_to_household_kwh_pairs(valid_rdd)` in `src/rdd_ops.py` using `map()` to emit `(LCLid, float(kwh))` key-value pairs
- [ ] T021 [US1] Implement pure function `extract_tariff_acorn_tokens(household_rdd)` in `src/rdd_ops.py` using `flatMap()` to tokenize demographic and tariff fields
- [ ] T022 [US1] Implement pure reduction function `find_peak_reading(pair_rdd)` in `src/rdd_ops.py` using `reduce()` with binary maximum comparator
- [ ] T023 [US1] Implement pure grouping function `group_readings_by_household(pair_rdd)` in `src/rdd_ops.py` using `groupByKey().mapValues(list)`
- [ ] T024 [US1] Implement pure aggregation function `sum_kwh_by_household(pair_rdd)` in `src/rdd_ops.py` using `reduceByKey(lambda a, b: a + b)`
- [ ] T025 [US1] Implement pure join & aggregate function `join_and_aggregate_by_tariff(meter_totals_rdd, household_tariff_rdd)` in `src/rdd_ops.py` using `join()`, mapping, and bounded aggregation ($\le 100$ rows collected)

**Checkpoint**: User Story 1 is fully functional and passes all tests locally in `pytest tests/test_rdd_ops.py` in $< 2$ seconds.

---

## Phase 4: User Story 2 - Centralized Configuration & Explicit Schema Enforcement (Priority: P2)

**Goal**: Guarantee zero schema inference across all dataset loaders by validating that `METER_SCHEMA`, `HOUSEHOLD_SCHEMA`, and `WEATHER_SCHEMA` load data with explicit types.

**Independent Test**: Run `pytest tests/test_spark_config.py -v` and assert that schemas load CSV fixtures without triggering `inferSchema` and enforce correct column names and nullability.

### Tests for User Story 2 ⚠️

- [ ] T026 [P] [US2] Write unit tests in `tests/test_spark_config.py` verifying `get_spark_session(is_local_test=True)` sets `master("local[2]")`, `512m` driver heap, and `spark.ui.enabled="false"`
- [ ] T027 [P] [US2] Write unit tests in `tests/test_spark_config.py` verifying that reading CSV files with `METER_SCHEMA`, `HOUSEHOLD_SCHEMA`, and `WEATHER_SCHEMA` creates DataFrames with exact expected types and zero `inferSchema` calls

### Implementation for User Story 2

- [ ] T028 [US2] Validate and refine schema definitions and CSV loader helper utilities in `src/spark_config.py`
- [ ] T029 [US2] Add schema validation assertions to verify column type matching on ingested DataFrames in `src/spark_config.py`

**Checkpoint**: User Stories 1 and 2 work independently with full schema safety.

---

## Phase 5: User Story 3 - Safe Local Testing & Mock Fixture Execution (Priority: P3)

**Goal**: Protect the 8GB RAM host machine by bounding local test runtime to $< 3$ seconds and memory consumption to $< 500$ MB RAM.

**Independent Test**: Execute `pytest tests/ -v` on Windows PowerShell; measure total execution duration and assert memory bounds.

### Tests & Verification for User Story 3

- [ ] T030 [P] [US3] Create `tests/test_local_guardrails.py` asserting that running the test suite on mock data does not exceed 500MB memory and executes in $< 3$ seconds
- [ ] T031 [US3] Verify edge case coverage in `tests/sample_data/sample_meters.csv` and `tests/sample_data/sample_households.csv` (literal `"Null"` strings, negative readings, multi-household distribution, `Std` vs. `ToU` balance)

**Checkpoint**: Local test guardrails strictly confirmed, ensuring zero host memory thrashing.

---

## Phase 6: User Story 4 - Cloud Lab Ingestion & Single-Node HDFS Setup (Priority: P4)

**Goal**: Deliver an end-to-end, self-contained Google Colab notebook (`notebooks/01_hdfs_and_rdd_ops.ipynb`) configuring Hadoop 3.4.1 pseudo-distributed HDFS, initializing the 3-layer directory layout, uploading CSV blocks into `/raw`, and executing all 7 RDD operations.

**Independent Test**: Upload `notebooks/01_hdfs_and_rdd_ops.ipynb` to Google Colab and run all cells; verify that HDFS daemons launch, raw CSV files upload to `hdfs://localhost:9000/raw`, and all 7 RDD transformations output clean summary tables.

### Implementation for User Story 4

- [ ] T032 [US4] Create notebook `notebooks/01_hdfs_and_rdd_ops.ipynb` with Markdown structure explaining architecture, course milestones, and team roster in bilingual English/Arabic
- [ ] T033 [US4] Implement Colab setup cells in `notebooks/01_hdfs_and_rdd_ops.ipynb` installing OpenJDK and Apache Hadoop 3.4.1, setting environment variables, and configuring `core-site.xml` (`hdfs://localhost:9000`) and `hdfs-site.xml` (`dfs.replication=1`)
- [ ] T034 [US4] Implement passwordless SSH, NameNode formatting (`hdfs namenode -format -force`), and HDFS daemon startup (`start-dfs.sh`) in `notebooks/01_hdfs_and_rdd_ops.ipynb`
- [ ] T035 [US4] Implement Three-Layer HDFS initialization (`hdfs dfs -mkdir -p /raw /cleaned /analytics`) and raw data ingestion (`hdfs dfs -put`) in `notebooks/01_hdfs_and_rdd_ops.ipynb`
- [ ] T036 [US4] Implement PySpark RDD cells executing all 7 core operations (`filter`, `map`, `flatMap`, `reduce`, `groupByKey`, `reduceByKey`, `join`) directly on HDFS raw blocks in `notebooks/01_hdfs_and_rdd_ops.ipynb`
- [ ] T037 [US4] Add final verification cells in `notebooks/01_hdfs_and_rdd_ops.ipynb` displaying tariff comparison aggregates and verifying driver memory safety ($\le 100$ rows collected)

**Checkpoint**: Practical Lab 1 Colab notebook is completely self-contained and reproducible.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Repository audit, hygiene validation, and documentation finalization.

- [ ] T038 [P] Audit Git tracking with `git status` to ensure 0 bytes of large `.csv` or `.parquet` data files are tracked
- [ ] T039 Execute full local test suite `pytest tests/ -v` and record test run duration and memory consumption
- [ ] T040 [P] Update `docs/spec-01-architecture.md` with the completed Spec 01 component architecture and Colab execution runbook

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — executes immediately.
- **Foundational (Phase 2)**: Depends on Phase 1 completion — BLOCKS all user stories.
- **User Story 1 (Phase 3 - Priority P1)**: Depends on Phase 2 completion. Can be delivered as standalone MVP.
- **User Story 2 (Phase 4 - Priority P2)**: Depends on Phase 2 completion. Operates in parallel with or after US1.
- **User Story 3 (Phase 5 - Priority P3)**: Depends on Phase 2 & Phase 3 test completion.
- **User Story 4 (Phase 6 - Priority P4)**: Depends on Phase 2 & Phase 3 logic completion (notebook packages `src/` logic for Colab).
- **Polish (Phase 7)**: Depends on completion of all user story phases.

### User Story Dependencies

```
Phase 1: Setup (T001-T005)
           │
           ▼
Phase 2: Foundational (T006-T011) [BLOCKING]
           │
           ├───────────────────────────────┐
           ▼                               ▼
Phase 3: US1 Core RDD Ops (T012-T025)   Phase 4: US2 Schemas & Config (T026-T029)
           │                               │
           ▼                               ▼
Phase 5: US3 Local Guardrails (T030-T031)  │
           │                               │
           └───────────────┬───────────────┘
                           ▼
Phase 6: US4 Cloud Colab Notebook (T032-T037)
                           │
                           ▼
Phase 7: Polish & Cross-Cutting (T038-T040)
```

---

## Parallel Opportunities

- **Setup Phase**: T002, T003, and T005 can be executed in parallel.
- **Foundational Phase**: T006 (`sample_meters.csv`) and T007 (`sample_households.csv`) can be created in parallel.
- **User Story 1 Tests**: T012 through T018 can all be authored in parallel before implementing RDD operations.
- **User Story 2 Tests**: T026 and T027 can be authored in parallel.
- **Polish Phase**: T038 and T040 can execute in parallel.

---

## Implementation Strategy: MVP First

1. **Step 1**: Complete Phase 1 (Setup) and Phase 2 (Foundational prerequisites).
2. **Step 2**: Author User Story 1 tests (T012-T018) in `tests/test_rdd_ops.py` and verify they fail.
3. **Step 3**: Implement pure RDD functions (T019-T025) in `src/rdd_ops.py` until all US1 tests pass.
4. **Step 4 (MVP Checkpoint)**: Verify `pytest tests/test_rdd_ops.py` passes in $< 2$ seconds on local laptop.
5. **Step 5**: Complete User Story 2 (T026-T029) and User Story 3 (T030-T031) to lock in schema contracts and 8GB memory guardrails.
6. **Step 6**: Complete User Story 4 (T032-T037) to deliver the Colab pseudo-distributed HDFS notebook.
7. **Step 7**: Complete Polish (T038-T040).
