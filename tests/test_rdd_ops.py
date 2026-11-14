"""Unit test suite for Phase 1 RDD Operations (The 7 Core Operations).

Tests all functions in src/rdd_ops.py against tests/sample_data fixtures
following Test-Driven Development (TDD).
"""

from src.rdd_ops import (
    extract_tariff_acorn_tokens,
    filter_valid_meter_rows,
    find_peak_reading,
    group_readings_by_household,
    join_and_aggregate_by_tariff,
    map_to_household_kwh_pairs,
    sum_kwh_by_household,
)


def test_filter_valid_meter_rows(sc, sample_meters_path):
    """T012: Verify filter_valid_meter_rows discards header, 'Null', negative, and malformed rows."""
    raw_rdd = sc.textFile(sample_meters_path)
    total_raw_count = raw_rdd.count()
    assert total_raw_count == 51  # 1 header + 50 data lines

    valid_rdd = filter_valid_meter_rows(raw_rdd)
    valid_records = valid_rdd.collect()

    # In sample_meters.csv: 50 lines - 1 header - 5 "Null" - 3 negative - 2 malformed = 40 valid lines
    assert len(valid_records) == 40

    for lclid, day, kwh in valid_records:
        assert lclid.startswith("MAC")
        assert len(day) > 0
        assert isinstance(kwh, float)
        assert kwh >= 0.0


def test_map_to_household_kwh_pairs(sc, sample_meters_path):
    """T013: Verify map_to_household_kwh_pairs produces (LCLid, float(kwh)) tuples."""
    raw_rdd = sc.textFile(sample_meters_path)
    valid_rdd = filter_valid_meter_rows(raw_rdd)
    pair_rdd = map_to_household_kwh_pairs(valid_rdd)

    pairs = pair_rdd.collect()
    assert len(pairs) == 40
    for lclid, kwh in pairs:
        assert lclid.startswith("MAC")
        assert isinstance(kwh, float)
        assert kwh >= 0.0


def test_extract_tariff_acorn_tokens(sc, sample_households_path):
    """T014: Verify extract_tariff_acorn_tokens decomposes household metadata into tokens."""
    household_rdd = sc.textFile(sample_households_path)
    tokens_rdd = extract_tariff_acorn_tokens(household_rdd)
    tokens = tokens_rdd.collect()

    assert len(tokens) > 0
    # Must contain recognized tariff types and demographic groups
    assert "Std" in tokens
    assert "ToU" in tokens
    assert "Affluent" in tokens
    assert "Comfortable" in tokens
    assert "Adversity" in tokens


def test_find_peak_reading(sc, sample_meters_path):
    """T015: Verify find_peak_reading correctly identifies global maximum consumption."""
    raw_rdd = sc.textFile(sample_meters_path)
    valid_rdd = filter_valid_meter_rows(raw_rdd)
    pair_rdd = map_to_household_kwh_pairs(valid_rdd)

    peak_household, peak_kwh = find_peak_reading(pair_rdd)
    # In sample_meters.csv, MAC000003 has the peak reading of 1.55 kWh
    assert peak_household == "MAC000003"
    assert round(peak_kwh, 3) == 1.550


def test_group_readings_by_household(sc, sample_meters_path):
    """T016: Verify group_readings_by_household groups readings per LCLid."""
    raw_rdd = sc.textFile(sample_meters_path)
    valid_rdd = filter_valid_meter_rows(raw_rdd)
    pair_rdd = map_to_household_kwh_pairs(valid_rdd)

    grouped_rdd = group_readings_by_household(pair_rdd)
    grouped = dict(grouped_rdd.collect())

    # 5 unique households with 8 valid readings each
    assert len(grouped) == 5
    for lclid in ["MAC000001", "MAC000002", "MAC000003", "MAC000004", "MAC000005"]:
        assert lclid in grouped
        assert len(grouped[lclid]) == 8
        assert all(isinstance(v, float) for v in grouped[lclid])


def test_sum_kwh_by_household(sc, sample_meters_path):
    """T017: Verify sum_kwh_by_household aggregates total energy consumed per household."""
    raw_rdd = sc.textFile(sample_meters_path)
    valid_rdd = filter_valid_meter_rows(raw_rdd)
    pair_rdd = map_to_household_kwh_pairs(valid_rdd)

    totals_rdd = sum_kwh_by_household(pair_rdd)
    totals = dict(totals_rdd.collect())

    assert len(totals) == 5
    # Expected sum for MAC000001: 0.25+0.31+0.18+0.21+0.19+0.22+0.20+0.24 = 1.80 kWh
    assert round(totals["MAC000001"], 2) == 1.80
    # Expected sum for MAC000005: 0.12+0.15+0.11+0.14+0.13+0.16+0.10+0.18 = 1.09 kWh
    assert round(totals["MAC000005"], 2) == 1.09


def test_join_and_aggregate_by_tariff(sc, sample_meters_path, sample_households_path):
    """T018: Verify join_and_aggregate_by_tariff calculates total, count, and average by tariff."""
    raw_rdd = sc.textFile(sample_meters_path)
    valid_rdd = filter_valid_meter_rows(raw_rdd)
    pair_rdd = map_to_household_kwh_pairs(valid_rdd)
    totals_rdd = sum_kwh_by_household(pair_rdd)

    household_rdd = sc.textFile(sample_households_path)
    # Parse household lines into (LCLid, stdorToU) pairs
    tariff_rdd = household_rdd.filter(lambda line: "LCLid" not in line and "," in line).map(
        lambda line: (line.split(",")[0].strip(), line.split(",")[1].strip())
    )

    summary = join_and_aggregate_by_tariff(totals_rdd, tariff_rdd)

    assert "Std" in summary
    assert "ToU" in summary

    # In sample_households.csv:
    # Std households with readings: MAC000001, MAC000003, MAC000005 (count = 3)
    # ToU households with readings: MAC000002, MAC000004 (count = 2)
    assert summary["Std"]["count"] == 3
    assert summary["ToU"]["count"] == 2

    assert summary["Std"]["total_kwh"] > 0
    assert summary["ToU"]["total_kwh"] > 0
    assert round(summary["Std"]["total_kwh"] / summary["Std"]["count"], 3) == round(summary["Std"]["avg_kwh"], 3)
    assert round(summary["ToU"]["total_kwh"] / summary["ToU"]["count"], 3) == round(summary["ToU"]["avg_kwh"], 3)
