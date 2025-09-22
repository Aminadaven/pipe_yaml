from pipe_yaml.base_etl import BaseETL
from pipe_yaml.duckdb_etl import DuckdbETL
from pipe_yaml.spark_etl import SparkETL


class EtlFactory:
    @staticmethod
    def create_etl(engine: str) -> BaseETL:
        if engine == 'spark':
            return SparkETL()
        elif engine == 'duckdb':
            return DuckdbETL()
        else:
            raise ValueError(f"Invalid engine: {engine}")
