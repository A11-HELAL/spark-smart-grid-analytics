"""Unit tests for Centralized Spark Configuration & Explicit Schemas (Spec 01 User Story 2).

Verifies:
- get_spark_session configuration guardrails for 8GB RAM host.
- Explicit StructType schemas (METER_SCHEMA, HOUSEHOLD_SCHEMA, WEATHER_SCHEMA).
- Zero inferSchema contract enforcement on CSV loaders.
"""

from pyspark.sql.types import DoubleType, StringType
from src.spark_config import (
    HDFS_ANALYTICS_DIR,
    HDFS_BASE_URL,
    HDFS_CLEANED_DIR,
    HDFS_RAW_DIR,
    HOUSEHOLD_SCHEMA,
    METER_SCHEMA,
    WEATHER_SCHEMA,
    load_household_dataframe,
    load_meter_dataframe,
)


def test_hdfs_path_constants():
    """Verify HDFS layer path constants match Constitutional Principle I."""
    assert HDFS_BASE_URL == "hdfs://localhost:9000"
    assert HDFS_RAW_DIR == "/raw"
    assert HDFS_CLEANED_DIR == "/cleaned"
    assert HDFS_ANALYTICS_DIR == "/analytics"


def test_spark_session_config_guardrails(spark):
    """T026: Verify spark session configuration adheres to 8GB RAM local guardrails."""
    conf = spark.conf
    # Verify local master and shuffle partitions
    master = conf.get("spark.master", "")
    assert "local" in master

    memory = conf.get("spark.driver.memory", "")
    assert memory == "512m"

    shuffle_partitions = conf.get("spark.sql.shuffle.partitions", "")
    assert shuffle_partitions == "2"

    ui_enabled = conf.get("spark.ui.enabled", "")
    assert ui_enabled == "false"


def test_explicit_schemas_no_infer(spark, sample_meters_path, sample_households_path):
    """T027: Verify explicit schemas prevent inferSchema and enforce exact datatypes."""
    # 1. Meter Schema
    assert len(METER_SCHEMA.fields) == 3
    field_names = [f.name for f in METER_SCHEMA.fields]
    assert "LCLid" in field_names
    assert "day" in field_names
    assert "energy(kWh/hh)" in field_names
    for field in METER_SCHEMA.fields:
        assert isinstance(field.dataType, StringType)

    # 2. Household Schema
    assert len(HOUSEHOLD_SCHEMA.fields) == 5
    household_fields = {f.name: f.dataType for f in HOUSEHOLD_SCHEMA.fields}
    assert isinstance(household_fields["LCLid"], StringType)
    assert isinstance(household_fields["stdorToU"], StringType)
    assert isinstance(household_fields["Acorn"], StringType)

    # 3. Weather Schema
    assert len(WEATHER_SCHEMA.fields) == 5
    weather_fields = {f.name: f.dataType for f in WEATHER_SCHEMA.fields}
    assert isinstance(weather_fields["temperature"], DoubleType)
    assert isinstance(weather_fields["humidity"], DoubleType)
    assert isinstance(weather_fields["windSpeed"], DoubleType)
    assert isinstance(weather_fields["time"], StringType)
    assert isinstance(weather_fields["summary"], StringType)

    # 4. Loader Functions test
    meter_df = load_meter_dataframe(spark, sample_meters_path)
    assert meter_df.schema == METER_SCHEMA
    assert meter_df.count() == 50

    household_df = load_household_dataframe(spark, sample_households_path)
    assert household_df.schema == HOUSEHOLD_SCHEMA
    assert household_df.count() == 10
