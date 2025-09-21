from duckdb_etl import DuckdbETL
from etl import ETL
from spark_etl import SparkETL


class EtlFactory:
    @staticmethod
    def create_etl(engine: str) -> ETL:
        if engine == 'spark':
            return SparkETL()
        elif engine == 'duckdb':
            return DuckdbETL()
        else:
            raise ValueError(f"Invalid engine: {engine}")
