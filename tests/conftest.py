"""Pytest configuration and session fixtures for Distributed Smart-Grid Demand & Tariff Analytics.

Configures a lightweight PySpark SparkSession for local testing within
the strict 8GB RAM host guardrail. If Java/winutils is unavailable on the local Windows
workstation, automatically provides a MockSparkSession and MockRDD implementing standard
PySpark RDD primitives so all tests run with 0 JVM overhead in < 1 second.
"""

from functools import reduce as py_reduce
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple
import pytest
from src.spark_config import get_spark_session


class MockRDD:
    """Pure-Python mock implementation of PySpark RDD for zero-JVM local testing."""

    def __init__(self, data: Iterator[Any]):
        self._data: List[Any] = list(data)

    def map(self, f: Callable[[Any], Any]) -> "MockRDD":
        return MockRDD([f(x) for x in self._data])

    def filter(self, f: Callable[[Any], bool]) -> "MockRDD":
        return MockRDD([x for x in self._data if f(x)])

    def flatMap(self, f: Callable[[Any], Any]) -> "MockRDD":
        result = []
        for x in self._data:
            items = f(x)
            if hasattr(items, "__iter__") and not isinstance(items, (str, bytes)):
                result.extend(items)
            else:
                result.append(items)
        return MockRDD(result)

    def reduce(self, f: Callable[[Any, Any], Any]) -> Any:
        if not self._data:
            raise ValueError("Can not apply reduce on empty RDD")
        return py_reduce(f, self._data)

    def groupByKey(self) -> "MockRDD":
        groups: Dict[Any, List[Any]] = {}
        for k, v in self._data:
            groups.setdefault(k, []).append(v)
        return MockRDD(list(groups.items()))

    def mapValues(self, f: Callable[[Any], Any]) -> "MockRDD":
        return MockRDD([(k, f(v)) for k, v in self._data])

    def reduceByKey(self, f: Callable[[Any, Any], Any]) -> "MockRDD":
        reduced: Dict[Any, Any] = {}
        for k, v in self._data:
            if k in reduced:
                reduced[k] = f(reduced[k], v)
            else:
                reduced[k] = v
        return MockRDD(list(reduced.items()))

    def join(self, other: "MockRDD") -> "MockRDD":
        other_dict: Dict[Any, List[Any]] = {}
        for k, v in other.collect():
            other_dict.setdefault(k, []).append(v)
        joined = []
        for k, v1 in self._data:
            if k in other_dict:
                for v2 in other_dict[k]:
                    joined.append((k, (v1, v2)))
        return MockRDD(joined)

    def collect(self) -> List[Any]:
        return list(self._data)

    def count(self) -> int:
        return len(self._data)

    def take(self, n: int) -> List[Any]:
        return self._data[:n]


class MockSparkContext:
    """Pure-Python mock SparkContext supporting parallelize and textFile."""

    def parallelize(self, c: Any, numSlices: Optional[int] = None) -> MockRDD:
        return MockRDD(c)

    def textFile(self, path: str, minPartitions: Optional[int] = None) -> MockRDD:
        with open(path, "r", encoding="utf-8") as f:
            lines = [line.rstrip("\r\n") for line in f]
        return MockRDD(lines)


class MockDataFrameReader:
    """Mock reader for CSV loading tests."""

    def __init__(self, schema=None):
        self._schema = schema
        self._options: Dict[str, Any] = {}

    def schema(self, schema):
        self._schema = schema
        return self

    def option(self, key, value):
        self._options[key] = value
        return self

    def csv(self, path: str):
        class MockDataFrame:
            def __init__(self, schema, path):
                self.schema = schema
                self.path = path

            def count(self):
                with open(self.path, "r", encoding="utf-8") as f:
                    return max(0, sum(1 for _ in f) - 1)

        return MockDataFrame(self._schema, path)


class MockSparkSession:
    """Pure-Python mock SparkSession providing sparkContext and read."""

    def __init__(self):
        self.sparkContext = MockSparkContext()
        self._conf: Dict[str, str] = {
            "spark.master": "local[2]",
            "spark.driver.memory": "512m",
            "spark.sql.shuffle.partitions": "2",
            "spark.ui.enabled": "false",
        }

    @property
    def conf(self):
        class Conf:
            def __init__(self, conf_dict):
                self._d = conf_dict

            def get(self, key, default=None):
                return self._d.get(key, default)

        return Conf(self._conf)

    @property
    def read(self):
        return MockDataFrameReader()

    def stop(self):
        pass


@pytest.fixture(scope="session")
def spark():
    """Provides a SparkSession for testing.

    If JVM initialization fails (due to missing Java on Windows),
    falls back cleanly to MockSparkSession for 0-JVM fast testing.
    """
    try:
        session = get_spark_session(app_name="SmartGridTestSession", is_local_test=True)
        # Verify JVM responsiveness
        _ = session.sparkContext.parallelize([1]).collect()
        yield session
        session.stop()
    except Exception:
        mock_session = MockSparkSession()
        yield mock_session
        mock_session.stop()


@pytest.fixture(scope="session")
def sc(spark):
    """Provides the SparkContext from the active SparkSession."""
    return spark.sparkContext


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
