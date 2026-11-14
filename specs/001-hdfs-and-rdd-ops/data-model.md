# Data Model & Entity Specifications: Spec 01 (`001-hdfs-and-rdd-ops`)

**Feature**: Spec 01 — Repository Architecture Foundation, Explicit Schemas, and Phase 1 HDFS & PySpark RDD Operations  
**Date**: 2026-11-14  
**Status**: Completed  

---

## 1. Raw Ingestion Entities (Three-Layer HDFS Storage)

### 1.1 `RawMeterReading` (Source: `halfhourly_dataset/*.csv`)
Represents an unparsed, raw line of half-hourly energy consumption from London smart meters stored in `hdfs://localhost:9000/raw`.

| Field Name | Raw CSV Type | Explicit Spark Type | Nullable | Validation & Constraints |
|---|---|---|---|---|
| `LCLid` | `String` | `StringType` | False | Format: `MAC\d{6}` (e.g., `MAC000002`). Mandatory primary identifier. |
| `DateTime` | `String` | `StringType` | False | Format: `YYYY-MM-DD HH:MM:SS.SSSSSSS` or `YYYY-MM-DD HH:MM:SS`. |
| `KWH/hh` | `String` | `StringType` | True | Raw string representation of kilowatt-hours consumed. May contain `"Null"`, negative numbers, or empty strings. |

### 1.2 `RawHouseholdProfile` (Source: `informations_households.csv`)
Represents household demographic, tariff, and batch allocation metadata stored in `hdfs://localhost:9000/raw`.

| Field Name | Raw CSV Type | Explicit Spark Type | Nullable | Validation & Constraints |
|---|---|---|---|---|
| `LCLid` | `String` | `StringType` | False | Foreign key linking to meter readings. |
| `stdorToU` | `String` | `StringType` | False | Enum: `'Std'` (Standard tariff) or `'ToU'` (Time-of-Use dynamic tariff). |
| `Acorn` | `String` | `StringType` | True | ACORN demographic segment (e.g., `'ACORN-A'`, `'ACORN-Q'`). |
| `Acorn_grouped` | `String` | `StringType` | True | Broad demographic group (e.g., `'Affluent'`, `'Comfortable'`, `'Adversity'`). |
| `file` | `String` | `StringType` | True | Batch allocation indicator (e.g., `'block_0'`). |

### 1.3 `RawWeatherObservation` (Source: `weather_hourly_darksky.csv`)
Represents hourly London meteorological observations stored in `hdfs://localhost:9000/raw`.

| Field Name | Raw CSV Type | Explicit Spark Type | Nullable | Validation & Constraints |
|---|---|---|---|---|
| `temperature` | `Double` | `DoubleType` | True | Celsius temperature. Range: $[-15.0, 45.0]$. |
| `humidity` | `Double` | `DoubleType` | True | Relative humidity fraction $[0.0, 1.0]$. |
| `windSpeed` | `Double` | `DoubleType` | True | Wind speed in meters per second ($\ge 0.0$). |
| `time` | `String` | `StringType` | False | ISO timestamp string formatted as `YYYY-MM-DD HH:MM:SS`. |
| `summary` | `String` | `StringType` | True | Textual weather condition (e.g., `'Partly Cloudy'`, `'Rain'`). |

---

## 2. In-Memory RDD Transformation Entities

These transient distributed entities are constructed and manipulated during RDD execution in `src/rdd_ops.py`.

### 2.1 `ValidMeterRecord`
Emitted by `filter_valid_meter_rows`:
- **Structure**: `Tuple[str, str, float]` -> `(LCLid, timestamp, kwh)`
- **Invariants**:
  - `LCLid` is non-empty.
  - `kwh` is a parsed non-negative floating-point number ($kwh \ge 0.0$).
  - Header lines and `"Null"` string tokens are discarded.

### 2.2 `HouseholdKwhPair`
Emitted by `map_to_household_kwh_pairs`:
- **Structure**: `Tuple[str, float]` -> `(LCLid, kwh)`
- **Purpose**: Standard key-value RDD format required for keyed aggregations (`reduceByKey`, `groupByKey`).

### 2.3 `HouseholdGroupedReadings`
Emitted by `group_readings_by_household`:
- **Structure**: `Tuple[str, List[float]]` -> `(LCLid, [kwh_1, kwh_2, ..., kwh_n])`
- **Purpose**: Groups all half-hourly readings belonging to a household into an iterable sequence.

### 2.4 `HouseholdTotalKwh`
Emitted by `sum_kwh_by_household`:
- **Structure**: `Tuple[str, float]` -> `(LCLid, total_kwh)`
- **Invariants**: `total_kwh` equals $\sum_{i=1}^{N} kwh_i$ for household `LCLid`. Computed via map-side combine.

### 2.5 `HouseholdTariffPair`
Parsed household metadata for join operations:
- **Structure**: `Tuple[str, str]` -> `(LCLid, stdorToU)`
- **Invariants**: `stdorToU` $\in \{'Std', 'ToU'\}$.

### 2.6 `EnrichedMeterRecord`
Result of `meter_totals_rdd.join(household_tariff_rdd)`:
- **Structure**: `Tuple[str, Tuple[float, str]]` -> `(LCLid, (total_kwh, stdorToU))`
- **Purpose**: Correlates total consumption with consumer tariff type for comparative analytics.

### 2.7 `TariffConsumptionSummary`
Final analytical aggregate emitted by `join_and_aggregate_by_tariff`:
- **Structure**: `Dict[str, Dict[str, float]]`:
  ```python
  {
      "Std": {"total_kwh": float, "count": int, "avg_kwh": float},
      "ToU": {"total_kwh": float, "count": int, "avg_kwh": float}
  }
  ```
- **Driver Constraint**: Total collected records $= 2$ rows, strictly satisfying Constitutional Principle IV ($\le 100$ rows).

---

## 3. Entity Relationships

```
+-----------------------------------+               +----------------------------------+
|          RawMeterReading          |               |       RawHouseholdProfile        |
|  (halfhourly_logs / sample_meters)|               |  (informations_households.csv)   |
+-----------------------------------+               +----------------------------------+
| LCLid: String                     |               | LCLid: String                    |
| DateTime: String                  |               | stdorToU: String ('Std'/'ToU')   |
| KWH/hh: String (clean -> float)   |               | Acorn: String                    |
+-----------------+-----------------+               +----------------+-----------------+
                  |                                                  |
                  | filter & map                                     | map
                  v                                                  v
+-----------------+-----------------+               +----------------+-----------------+
|        HouseholdTotalKwh          |               |       HouseholdTariffPair        |
| (LCLid, total_kwh)                |               | (LCLid, stdorToU)                |
+-----------------+-----------------+               +----------------+-----------------+
                  |                                                  |
                  +------------------------+-------------------------+
                                           |
                                           | RDD join(LCLid)
                                           v
                        +------------------+-------------------+
                        |             EnrichedMeterRecord      |
                        |      (LCLid, (total_kwh, stdorToU))  |
                        +------------------+-------------------+
                                           |
                                           | map & reduceByKey
                                           v
                        +------------------+-------------------+
                        |       TariffConsumptionSummary       |
                        | (Tariff, (Total_Kwh, Count, Avg_Kwh))|
                        +--------------------------------------+
```
