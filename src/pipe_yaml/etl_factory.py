from pipe_yaml.base_etl import BaseETL
from pipe_yaml.duckdb_etl import DuckdbETL
from pipe_yaml.models import Pipeline
from pipe_yaml.spark_etl import SparkETL


class EtlFactory:
    @staticmethod
    def create_etl(pipeline: Pipeline) -> BaseETL:
        if pipeline.engine == 'spark':
            return SparkETL(pipeline)
        elif pipeline.engine == 'duckdb':
            return DuckdbETL(pipeline)
        else:
            raise ValueError(f"Invalid engine: {pipeline.engine}")
