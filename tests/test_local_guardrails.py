"""Unit tests for Safe Local Testing & Mock Fixture Execution (Spec 01 User Story 3).

Verifies:
- Execution speed (< 3 seconds) and memory safety (< 500MB RAM) for 8GB host laptop.
- Edge case completeness in tests/sample_data/ mock files.
"""

import os
from pathlib import Path
import time
import tracemalloc
from src.rdd_ops import filter_valid_meter_rows, map_to_household_kwh_pairs, sum_kwh_by_household


def test_local_test_execution_time_and_memory(sc, sample_meters_path):
    """T030: Assert that local operations execute within 3 seconds and under 500MB RAM."""
    tracemalloc.start()
    start_time = time.perf_counter()

    # Execute full pipeline over mock data
    raw_rdd = sc.textFile(sample_meters_path)
    valid_rdd = filter_valid_meter_rows(raw_rdd)
    pair_rdd = map_to_household_kwh_pairs(valid_rdd)
    totals_rdd = sum_kwh_by_household(pair_rdd)
    result = totals_rdd.collect()

    duration = time.perf_counter() - start_time
    current_mem, peak_mem = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    peak_mem_mb = peak_mem / (1024 * 1024)

    # 8GB RAM host guardrails: duration < 3.0 seconds, peak memory < 500 MB
    assert duration < 3.0, f"Execution took {duration:.2f}s, exceeding 3.0s limit"
    assert peak_mem_mb < 500.0, f"Peak memory {peak_mem_mb:.2f}MB exceeded 500MB limit"
    assert len(result) == 5


def test_sample_data_edge_case_coverage(sample_meters_path, sample_households_path):
    """T031: Verify mock datasets include all necessary edge cases and remain <= 100 rows."""
    # 1. Meters CSV Audit
    with open(sample_meters_path, "r", encoding="utf-8") as f:
        meter_lines = [line.strip() for line in f if line.strip()]

    assert len(meter_lines) <= 100, "sample_meters.csv must not exceed 100 lines"
    assert any("Null" in line for line in meter_lines), "Must contain literal 'Null' test cases"
    assert any("-" in line.split(",")[-1] for line in meter_lines if len(line.split(",")) == 3 and line.split(",")[-1] != "Null"), "Must contain negative values"
    assert any(len(line.split(",")) != 3 for line in meter_lines[1:]), "Must contain malformed lines"

    # File size must be small (< 50 KB)
    assert os.path.getsize(sample_meters_path) < 50 * 1024

    # 2. Households CSV Audit
    with open(sample_households_path, "r", encoding="utf-8") as f:
        household_lines = [line.strip() for line in f if line.strip()]

    assert len(household_lines) <= 100, "sample_households.csv must not exceed 100 lines"
    assert any(",Std," in line for line in household_lines), "Must contain Std tariff households"
    assert any(",ToU," in line for line in household_lines), "Must contain ToU tariff households"
    assert any("Affluent" in line for line in household_lines), "Must contain Affluent Acorn group"
    assert any("Comfortable" in line for line in household_lines), "Must contain Comfortable Acorn group"
    assert any("Adversity" in line for line in household_lines), "Must contain Adversity Acorn group"
