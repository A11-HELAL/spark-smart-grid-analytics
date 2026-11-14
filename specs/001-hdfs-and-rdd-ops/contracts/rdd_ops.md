# Component Contract: `src/rdd_ops.py`

**Module**: `src.rdd_ops`  
**Purpose**: Pure, testable functions implementing the 7 core RDD operations from Lab 1.  
**Constitution Alignment**: Enforces Principle IV (Driver Memory Protection & Pure Functions) and Principle V (Lab Checkpoint Alignment).

---

## Functions (The 7 Core Operations)

### 1. `filter_valid_meter_rows`
```python
def filter_valid_meter_rows(raw_rdd: RDD[str]) -> RDD[Tuple[str, str, float]]:
    """
    Operation 1: filter
    Filters raw CSV lines from meter reading logs to discard:
      - Header rows (lines containing 'LCLid')
      - Malformed lines (lines with column count != 3)
      - Missing or 'Null' string literal readings
      - Negative consumption values (kwh < 0.0)

    Parameters:
        raw_rdd (RDD[str]): RDD of raw text lines from CSV.

    Returns:
        RDD[Tuple[str, str, float]]: RDD of validated (LCLid, timestamp, kwh) tuples.
    """
```

### 2. `map_to_household_kwh_pairs`
```python
def map_to_household_kwh_pairs(valid_rdd: RDD[Tuple[str, str, float]]) -> RDD[Tuple[str, float]]:
    """
    Operation 2: map
    Maps validated meter records into key-value pairs of (LCLid, kwh).

    Parameters:
        valid_rdd (RDD[Tuple[str, str, float]]): Validated meter records.

    Returns:
        RDD[Tuple[str, float]]: Key-value RDD of (household_id, kwh_reading).
    """
```

### 3. `extract_tariff_acorn_tokens`
```python
def extract_tariff_acorn_tokens(household_raw_rdd: RDD[str]) -> RDD[str]:
    """
    Operation 3: flatMap
    Tokenizes household profile lines to extract individual metadata tokens
    (tariff type and ACORN demographic tags) as a flattened stream of terms.

    Parameters:
        household_raw_rdd (RDD[str]): Raw CSV text lines from informations_households.csv.

    Returns:
        RDD[str]: Flattened RDD of individual normalized metadata tokens.
    """
```

### 4. `find_peak_reading`
```python
def find_peak_reading(pair_rdd: RDD[Tuple[str, float]]) -> Tuple[str, float]:
    """
    Operation 4: reduce
    Executes a distributed reduction to find the record with the maximum
    half-hourly energy consumption across the entire dataset.

    Parameters:
        pair_rdd (RDD[Tuple[str, float]]): Key-value RDD of (LCLid, kwh).

    Returns:
        Tuple[str, float]: (LCLid, max_kwh) corresponding to the peak reading.
    """
```

### 5. `group_readings_by_household`
```python
def group_readings_by_household(pair_rdd: RDD[Tuple[str, float]]) -> RDD[Tuple[str, List[float]]]:
    """
    Operation 5: groupByKey
    Groups half-hourly readings by household ID into a list of values.
    Demonstrates wide-dependency shuffle grouping.

    Parameters:
        pair_rdd (RDD[Tuple[str, float]]): Key-value RDD of (LCLid, kwh).

    Returns:
        RDD[Tuple[str, List[float]]]: RDD of (LCLid, [kwh_1, kwh_2, ...]).
    """
```

### 6. `sum_kwh_by_household`
```python
def sum_kwh_by_household(pair_rdd: RDD[Tuple[str, float]]) -> RDD[Tuple[str, float]]:
    """
    Operation 6: reduceByKey
    Aggregates total energy consumption per household using map-side combine
    before network shuffling.

    Parameters:
        pair_rdd (RDD[Tuple[str, float]]): Key-value RDD of (LCLid, kwh).

    Returns:
        RDD[Tuple[str, float]]: RDD of (LCLid, total_kwh).
    """
```

### 7. `join_and_aggregate_by_tariff`
```python
def join_and_aggregate_by_tariff(
    meter_totals_rdd: RDD[Tuple[str, float]],
    household_tariff_rdd: RDD[Tuple[str, str]]
) -> Dict[str, Dict[str, float]]:
    """
    Operation 7: join
    Joins household total energy consumption with household tariff classifications
    on LCLid, then computes total consumption, count, and average consumption
    for each tariff category ('Std' vs 'ToU').

    Parameters:
        meter_totals_rdd (RDD[Tuple[str, float]]): (LCLid, total_kwh) from reduceByKey.
        household_tariff_rdd (RDD[Tuple[str, str]]): (LCLid, stdorToU).

    Returns:
        Dict[str, Dict[str, float]]: Bounded summary dictionary collected to Driver (<= 100 rows):
            {
                "Std": {"total_kwh": float, "count": int, "avg_kwh": float},
                "ToU": {"total_kwh": float, "count": int, "avg_kwh": float}
            }
    """
```
