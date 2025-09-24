import pytest

from pipe_yaml.base_etl import BaseETL
from pipe_yaml.duckdb_etl import DuckdbETL
from pipe_yaml.etl_factory import EtlFactory
from pipe_yaml.spark_etl import SparkETL


class TestEtlFactory:
    """Test the ETL Factory pattern and engine creation."""

    def test_create_spark_etl(self):
        """Test creating Spark ETL engine."""
        etl = EtlFactory.create_etl('spark')

        assert isinstance(etl, SparkETL)
        assert isinstance(etl, BaseETL)

    def test_create_duckdb_etl(self):
        """Test creating DuckDB ETL engine."""
        etl = EtlFactory.create_etl('duckdb')

        assert isinstance(etl, DuckdbETL)
        assert isinstance(etl, BaseETL)

    def test_create_invalid_engine(self):
        """Test creating ETL engine with invalid engine name."""
        with pytest.raises(ValueError, match="Invalid engine: invalid_engine"):
            EtlFactory.create_etl('invalid_engine')

    def test_create_case_sensitive_engine(self):
        """Test that engine names are case-sensitive."""
        with pytest.raises(ValueError, match="Invalid engine: SPARK"):
            EtlFactory.create_etl('SPARK')

        with pytest.raises(ValueError, match="Invalid engine: DuckDb"):
            EtlFactory.create_etl('DuckDb')

    def test_create_invalid_engine(self):
        """Test creating ETL engine with invalid engine name."""
        with pytest.raises(ValueError, match="Invalid engine: invalid_engine"):
            EtlFactory.create_etl('invalid_engine')

    def test_create_case_sensitive_engine(self):
        """Test that engine names are case-sensitive."""
        with pytest.raises(ValueError, match="Invalid engine: SPARK"):
            EtlFactory.create_etl('SPARK')

        with pytest.raises(ValueError, match="Invalid engine: DuckDb"):
            EtlFactory.create_etl('DuckDb')

    def test_create_none_engine(self):
        """Test creating ETL engine with None engine name."""
        with pytest.raises(ValueError, match="Invalid engine: None"):
            EtlFactory.create_etl(None)

    def test_create_empty_engine(self):
        """Test creating ETL engine with empty engine name."""
        with pytest.raises(ValueError, match="Invalid engine: "):
            EtlFactory.create_etl("")

    def test_create_whitespace_engine(self):
        """Test creating ETL engine with whitespace engine name."""
        with pytest.raises(ValueError, match="Invalid engine:   "):
            EtlFactory.create_etl("   ")
