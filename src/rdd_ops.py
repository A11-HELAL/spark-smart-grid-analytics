"""Phase 1 PySpark RDD Operations Module (The 7 Core Operations).

Pure, side-effect-free, testable distributed transformations adhering strictly to:
- Principle II: Local Hardware Protection (pure functions, bounded memory)
- Principle IV: Driver Memory Protection (only aggregates <= 100 rows collected)
- Principle V: Course Checkpoint Alignment (Spec 01 Lab 1)
"""

from typing import Any, Dict, List, Tuple
from pyspark import RDD


def filter_valid_meter_rows(raw_rdd: RDD) -> RDD:
    """Operation 1: filter.

    Filters raw CSV text lines from smart meter logs to discard:
      - Header lines (containing 'LCLid')
      - Malformed lines (column count != 3)
      - Corrupted or 'Null' string literal readings
      - Negative consumption readings (kwh < 0.0)

    Parameters
    ----------
    raw_rdd : RDD
        RDD containing raw text lines from CSV.

    Returns
    -------
    RDD
        RDD of validated (LCLid, timestamp, float(kwh)) tuples.
    """
    def _is_valid(line: str) -> bool:
        if not line or "LCLid" in line:
            return False
        parts = line.split(",")
        if len(parts) != 3:
            return False
        raw_val = parts[2].strip()
        if raw_val in ("Null", "NULL", "null", ""):
            return False
        try:
            val = float(raw_val)
            return val >= 0.0
        except ValueError:
            return False

    def _parse(line: str) -> Tuple[str, str, float]:
        parts = line.split(",")
        return parts[0].strip(), parts[1].strip(), float(parts[2].strip())

    return raw_rdd.filter(_is_valid).map(_parse)


def map_to_household_kwh_pairs(valid_rdd: RDD) -> RDD:
    """Operation 2: map.

    Transforms validated meter tuples into key-value pairs of (LCLid, float(kwh)).

    Parameters
    ----------
    valid_rdd : RDD
        RDD of validated tuples (LCLid, timestamp, kwh).

    Returns
    -------
    RDD
        Key-value RDD of (LCLid, float(kwh)).
    """
    return valid_rdd.map(lambda record: (record[0], float(record[2])))


def extract_tariff_acorn_tokens(household_raw_rdd: RDD) -> RDD:
    """Operation 3: flatMap.

    Tokenizes household profile lines to extract individual metadata tokens
    (tariff type and ACORN demographic tags) as a flattened stream of terms.

    Parameters
    ----------
    household_raw_rdd : RDD
        Raw CSV text lines from informations_households.csv.

    Returns
    -------
    RDD
        Flattened RDD of individual normalized metadata tokens.
    """
    def _tokenize(line: str) -> List[str]:
        if not line or "LCLid" in line:
            return []
        parts = [p.strip() for p in line.split(",") if p.strip()]
        # Skip household ID (index 0) and extract tariff and demographic tokens
        return parts[1:] if len(parts) > 1 else []

    return household_raw_rdd.flatMap(_tokenize)


def find_peak_reading(pair_rdd: RDD) -> Tuple[str, float]:
    """Operation 4: reduce.

    Executes a distributed reduction to find the single record with the
    maximum half-hourly energy consumption across the entire dataset.

    Parameters
    ----------
    pair_rdd : RDD
        Key-value RDD of (LCLid, float(kwh)).

    Returns
    -------
    Tuple[str, float]
        (LCLid, max_kwh) corresponding to the peak half-hourly reading.
    """
    return pair_rdd.reduce(lambda a, b: a if a[1] >= b[1] else b)


def group_readings_by_household(pair_rdd: RDD) -> RDD:
    """Operation 5: groupByKey.

    Groups half-hourly readings by household ID into a list of values.
    Demonstrates wide-dependency shuffle grouping.

    Parameters
    ----------
    pair_rdd : RDD
        Key-value RDD of (LCLid, float(kwh)).

    Returns
    -------
    RDD
        RDD of (LCLid, List[float(kwh)]).
    """
    return pair_rdd.groupByKey().mapValues(list)


def sum_kwh_by_household(pair_rdd: RDD) -> RDD:
    """Operation 6: reduceByKey.

    Aggregates total energy consumption per household using map-side combine
    before network shuffling.

    Parameters
    ----------
    pair_rdd : RDD
        Key-value RDD of (LCLid, float(kwh)).

    Returns
    -------
    RDD
        RDD of (LCLid, float(total_kwh)).
    """
    return pair_rdd.reduceByKey(lambda a, b: a + b)


def join_and_aggregate_by_tariff(
    meter_totals_rdd: RDD,
    household_tariff_rdd: RDD,
) -> Dict[str, Dict[str, float]]:
    """Operation 7: join.

    Joins household total consumption with household tariff classifications
    on LCLid, then computes total consumption, count, and average consumption
    for each tariff category ('Std' vs 'ToU').

    Parameters
    ----------
    meter_totals_rdd : RDD
        RDD of (LCLid, total_kwh) from reduceByKey.
    household_tariff_rdd : RDD
        RDD of (LCLid, stdorToU).

    Returns
    -------
    Dict[str, Dict[str, float]]
        Bounded summary dictionary collected to the Driver (<= 100 rows):
        {
            "Std": {"total_kwh": float, "count": int, "avg_kwh": float},
            "ToU": {"total_kwh": float, "count": int, "avg_kwh": float}
        }
    """
    # Join on LCLid -> (LCLid, (total_kwh, tariff))
    joined_rdd = meter_totals_rdd.join(household_tariff_rdd)

    # Map to (tariff, (total_kwh, 1))
    tariff_pairs = joined_rdd.map(lambda item: (item[1][1], (item[1][0], 1)))

    # reduceByKey to aggregate (sum_kwh, count) per tariff
    tariff_aggregated = tariff_pairs.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )

    # Collect bounded aggregate to Driver (exactly 2 rows for Std and ToU)
    collected = tariff_aggregated.collect()

    summary: Dict[str, Dict[str, float]] = {}
    for tariff, (total_kwh, count) in collected:
        avg_kwh = (total_kwh / count) if count > 0 else 0.0
        summary[tariff] = {
            "total_kwh": round(total_kwh, 4),
            "count": count,
            "avg_kwh": round(avg_kwh, 4),
        }

    return summary
