"""Pytest configuration and session fixtures for Distributed Smart-Grid Demand & Tariff Analytics.

Configures a shared lightweight PySpark SparkSession for local testing within
the strict 8GB RAM host guardrail.
"""

from pathlib import Path
import pytest
from src.spark_config import get_spark_session


@pytest.fixture(scope="session")
def spark():
    """Provides a shared lightweight SparkSession for the test suite.

    Enforces local[2], 512m driver memory, 2 shuffle partitions, and no UI.
    """
    session = get_spark_session(app_name="SmartGridTestSession", is_local_test=True)
    yield session
    session.stop()


@pytest.fixture(scope="session")
def sample_data_dir() -> Path:
    """Returns the Path to the sample data fixtures directory."""
    return Path(__file__).parent / "sample_data"


@pytest.fixture(scope="session")
def sample_meters_path(sample_data_dir: Path) -> str:
    """Returns the absolute string path to sample_meters.csv."""
    return str(sample_data_dir / "sample_meters.csv")


@pytest.fixture(scope="session")
def sample_households_path(sample_data_dir: Path) -> str:
    """Returns the absolute string path to sample_households.csv."""
    return str(sample_data_dir / "sample_households.csv")
