# Research & Architectural Decisions: Spec 01 (`001-hdfs-and-rdd-ops`)

**Feature**: Spec 01 — Repository Architecture Foundation, Explicit Schemas, and Phase 1 HDFS & PySpark RDD Operations  
**Date**: 2026-11-14  
**Status**: Completed  

---

## 1. PySpark Local Session Tuning for 8GB RAM Laptop

### Decision
Implement a parameterized SparkSession builder function in `src/spark_config.py`:
`get_spark_session(app_name: str, is_local_test: bool = False) -> SparkSession`.
When `is_local_test=True`:
- `master("local[2]")`: Limits PySpark execution to 2 worker threads (matching the dual-core Intel Core i5-7200U architecture).
- `spark.driver.memory="512m"`: Caps JVM heap to 512MB, preventing any possibility of memory exhaustion on the 8GB host.
- `spark.sql.shuffle.partitions="2"`: Avoids Spark's default 200 shuffle partitions for tiny mock tests.
- `spark.ui.enabled="false"`: Disables the embedded Web UI server to eliminate port binding delays and background threads.
- `spark.driver.extraJavaOptions="-Djava.security.manager=allow"`: Ensures compatibility on modern JVM runtimes without security warning overhead.

When `is_local_test=False` (Google Colab production mode):
- `master("local[*]")`: Utilizes all available vCPUs in the Colab VM.
- `spark.driver.memory="4g"`: Allocates up to 4GB driver heap.
- Integrates with Hadoop HDFS at `hdfs://localhost:9000`.

### Rationale
Running Spark locally on an 8GB machine can easily freeze Windows if default configurations spawn 200 partitions, 4GB heap allocations, or multiple background UI servers. Bounding memory to 512MB and shuffle partitions to 2 enables local `pytest` executions over 50-row mock data to complete in $\le$ 2 seconds with zero host memory pressure, directly enforcing Constitutional Principle II.

### Alternatives Considered
- *Using full `local[*]` locally*: Rejected because on a 2-core / 4-thread laptop under general OS load, unconstrained threads compete with system processes and trigger JVM GC spikes.
- *Testing without Spark (pure Python mocks)*: Rejected because the assignment requires verifying genuine PySpark RDD transformations and `StructType` schema contracts.

---

## 2. Low-Level RDD Transformation Mechanics (The 7 Core Operations)

### Decision
Implement pure, deterministic functions in `src/rdd_ops.py` operating strictly on distributed RDD partitions:

1. **`filter_valid_meter_rows(raw_rdd)` (`filter`)**:
   - Discards CSV header row (`LCLid,stdorToU...` or `LCLid,day...`).
   - Discards malformed rows where column count $\ne 3$.
   - Discards rows where energy column equals string `"Null"`, `""`, or cannot be parsed as a float.
   - Discards invalid negative readings ($kwh < 0.0$).
2. **`map_to_household_kwh_pairs(valid_rdd)` (`map`)**:
   - Parses CSV string `LCLid,tstp,energy` into `(LCLid, float(energy))`.
3. **`extract_tariff_acorn_tokens(household_rdd)` (`flatMap`)**:
   - Tokenizes demographic and tariff string fields (`stdorToU`, `Acorn`, `Acorn_grouped`) into individual normalized categorical tags for downstream indexing.
4. **`find_peak_reading(pair_rdd)` (`reduce`)**:
   - Applies binary comparator `lambda a, b: a if a[1] >= b[1] else b` over `(LCLid, kwh)` pairs to return the single highest half-hourly consumption reading globally.
5. **`group_readings_by_household(pair_rdd)` (`groupByKey`)**:
   - Groups energy readings per `LCLid` into an iterable list: `.groupByKey().mapValues(list)`. Applied to a filtered subset of households to demonstrate wide-dependency shuffling safely.
6. **`sum_kwh_by_household(pair_rdd)` (`reduceByKey`)**:
   - Aggregates energy consumption per household: `.reduceByKey(lambda a, b: a + b)`. Demonstrates map-side combine efficiency vs. `groupByKey`.
7. **`join_and_aggregate_by_tariff(meter_totals_rdd, household_tariff_rdd)` (`join`)**:
   - Keyed join on `LCLid`: joins `(LCLid, total_kwh)` with `(LCLid, stdorToU)`.
   - Maps to `(tariff, (total_kwh, 1))` and aggregates via `reduceByKey` to compute both sum and record count, deriving total and average consumption per tariff (`Std` vs. `ToU`).

### Rationale
These 7 operations represent the standard academic syllabus for distributed processing, illustrating fundamental big data concepts: narrow dependencies (`map`, `filter`, `flatMap`), wide dependencies with shuffling (`groupByKey`, `reduceByKey`, `join`), map-side aggregation optimization (`reduceByKey` vs `groupByKey`), and distributed reduction (`reduce`).

### Alternatives Considered
- *Performing aggregation via Spark DataFrames / SQL*: Deferred to Spec 02 and Spec 03. Spec 01 explicitly mandates proving foundational understanding of raw RDD transformations.

---

## 3. Pseudo-Distributed Hadoop 3.4.1 on Google Colab

### Decision
Automate single-node pseudo-distributed Hadoop 3.4.1 setup via executable bash cells in `notebooks/01_hdfs_and_rdd_ops.ipynb`:
1. Install OpenJDK 8 or 11 and download Apache Hadoop 3.4.1 binary tarball.
2. Configure environment variables (`HADOOP_HOME`, `JAVA_HOME`, `PATH`).
3. Generate XML configurations:
   - `etc/hadoop/core-site.xml`:
     ```xml
     <configuration>
       <property>
         <name>fs.defaultFS</name>
         <value>hdfs://localhost:9000</value>
       </property>
     </configuration>
     ```
   - `etc/hadoop/hdfs-site.xml`:
     ```xml
     <configuration>
       <property>
         <name>dfs.replication</name>
         <value>1</value>
       </property>
       <property>
         <name>dfs.namenode.name.dir</name>
         <value>file:///tmp/hadoop-root/dfs/name</value>
       </property>
       <property>
         <name>dfs.datanode.data.dir</name>
         <value>file:///tmp/hadoop-root/dfs/data</value>
       </property>
     </configuration>
     ```
4. Set up passwordless SSH for `localhost` (`ssh-keygen` and appending to `authorized_keys`).
5. Format NameNode (`hdfs namenode -format -force`).
6. Launch NameNode, DataNode, and SecondaryNameNode via `start-dfs.sh`.
7. Provision the 3 HDFS layer directories:
   - `hdfs dfs -mkdir -p /raw /cleaned /analytics`
8. Ingest raw CSV blocks into `hdfs://localhost:9000/raw` using `hdfs dfs -put`.

### Rationale
Google Colab provides a disposable Linux VM with ~12GB RAM and ~25GB SSD, ideal for executing pseudo-distributed Hadoop with `dfs.replication=1`. This completely isolates heavy HDFS execution from the student's 8GB Windows machine while providing authentic distributed filesystem experience.

---

## 4. Explicit StructType Schema Strategy (Zero inferSchema)

### Decision
Define explicit schemas in `src/spark_config.py` using PySpark `StructType` and `StructField`:
- `METER_SCHEMA`:
  - `LCLid`: `StringType`, nullable=False
  - `DateTime`: `StringType`, nullable=False
  - `KWH/hh`: `StringType`, nullable=True (StringType accommodates literal `"Null"` tokens prior to data cleaning in Spec 02)
- `HOUSEHOLD_SCHEMA`:
  - `LCLid`: `StringType`, nullable=False
  - `stdorToU`: `StringType`, nullable=False
  - `Acorn`: `StringType`, nullable=True
  - `Acorn_grouped`: `StringType`, nullable=True
  - `file`: `StringType`, nullable=True
- `WEATHER_SCHEMA`:
  - `temperature`: `DoubleType`, nullable=True
  - `humidity`: `DoubleType`, nullable=True
  - `windSpeed`: `DoubleType`, nullable=True
  - `time`: `StringType`, nullable=False
  - `summary`: `StringType`, nullable=True

### Rationale
By defining schemas up-front, reading CSV data bypasses the costly pre-scan that `inferSchema=True` performs over gigabytes of raw data. Furthermore, keeping `KWH/hh` as `StringType` in raw schema ingestion prevents silent parser failures when encountering `"Null"` strings; explicit transformation parses and validates numeric conversion safely.

---

## 5. Synthetic Mock Data & Unit Testing Design

### Decision
Create two tiny mock CSV files under `tests/sample_data/`:
1. `sample_meters.csv` (50 rows):
   - Header row.
   - 40 valid records across 5 distinct `LCLid`s with valid ISO timestamps and positive float kWh values.
   - 5 rows with literal `"Null"` string in the energy column.
   - 3 rows with negative energy values (`-0.150`).
   - 2 malformed rows (missing delimiters / corrupted tokens).
2. `sample_households.csv` (10 rows):
   - Header row.
   - 10 household profiles matching the 5 meter `LCLid`s plus 5 additional households.
   - Mix of `Std` and `ToU` tariffs and diverse ACORN demographic groups (`Adversity`, `Comfortable`, `Affluent`).

### Rationale
Enables fast, thorough unit testing of every edge case identified in the feature specification. Local tests run in $\le$ 2 seconds using `pytest` without any external network, Hadoop daemon, or cloud dependency.
