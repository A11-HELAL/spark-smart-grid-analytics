# Component Contract: `src/spark_config.py`

**Module**: `src.spark_config`  
**Purpose**: Centralized SparkSession builder, explicit dataset schemas, and storage path constants.  
**Constitution Alignment**: Enforces Principle I (HDFS Layering), Principle II (8GB RAM Guardrail), and Principle III (Zero inferSchema).

---

## 1. Constants

### 1.1 HDFS Storage Paths
```python
HDFS_BASE_URL: str = "hdfs://localhost:9000"
HDFS_RAW_DIR: str = "/raw"
HDFS_CLEANED_DIR: str = "/cleaned"
HDFS_ANALYTICS_DIR: str = "/analytics"
```

### 1.2 Explicit Schemas (StructType)

#### `METER_SCHEMA`
Explicit schema for `halfhourly_logs` CSV files:
```python
from pyspark.sql.types import StructType, StructField, StringType

METER_SCHEMA = StructType([
    StructField("LCLid", StringType(), nullable=False),
    StructField("tstp", StringType(), nullable=False),
    StructField("energy(kWh/hh)", StringType(), nullable=True)  # StringType to safely handle "Null" strings before cleaning
])
```

#### `HOUSEHOLD_SCHEMA`
Explicit schema for `informations_households.csv`:
```python
HOUSEHOLD_SCHEMA = StructType([
    StructField("LCLid", StringType(), nullable=False),
    StructField("stdorToU", StringType(), nullable=False),
    StructField("Acorn", StringType(), nullable=True),
    StructField("Acorn_grouped", StringType(), nullable=True),
    StructField("file", StringType(), nullable=True)
])
```

#### `WEATHER_SCHEMA`
Explicit schema for `weather_hourly_darksky.csv`:
```python
from pyspark.sql.types import DoubleType

WEATHER_SCHEMA = StructType([
    StructField("temperature", DoubleType(), nullable=True),
    StructField("humidity", DoubleType(), nullable=True),
    StructField("windSpeed", DoubleType(), nullable=True),
    StructField("time", StringType(), nullable=False),
    StructField("summary", StringType(), nullable=True)
])
```

---

## 2. Functions

### `get_spark_session`

```python
def get_spark_session(app_name: str = "SmartGridAnalytics", is_local_test: bool = False) -> SparkSession:
    """
    Builds or retrieves an active SparkSession configured for either lightweight
    local testing or cloud cluster execution.

    Parameters:
        app_name (str): The name of the Spark application.
        is_local_test (bool): If True, configures the session with strict 8GB RAM laptop
                              guardrails (local[2], 512MB heap, 2 shuffle partitions, UI disabled).
                              If False, configures full local[*] master for Google Colab/cluster.

    Returns:
        SparkSession: Configured active SparkSession instance.
    """
```

#### Behavioral Contract:
- **When `is_local_test=True`**:
  - `master`: `"local[2]"`
  - `spark.driver.memory`: `"512m"`
  - `spark.sql.shuffle.partitions`: `"2"`
  - `spark.ui.enabled`: `"false"`
  - `spark.driver.extraJavaOptions`: `"-Djava.security.manager=allow"`
- **When `is_local_test=False`**:
  - `master`: `"local[*]"`
  - `spark.driver.memory`: `"4g"`
  - Default shuffle partitions and Web UI enabled.
- **Idempotency**: Multiple calls with matching arguments return the existing or updated active session without creating duplicate JVM processes.
