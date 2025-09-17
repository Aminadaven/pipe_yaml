import duckdb

from abc_etl import *


class DuckdbETL(ETL):
    def _extract_step(self, step: ExtractStep):
        super()._extract_step(step)
        duckdb.sql(f"CREATE TABLE {step.df} AS SELECT * FROM '{step.path}';")

    def _transform_step(self, step: TransformStep):
        super()._transform_step(step)
        if step.df:
            duckdb.sql(f"CREATE TABLE {step.df} AS {step.sql};")
            return
        duckdb.sql(step.sql).show()

    def _load_step(self, step: LoadStep):
        super()._load_step(step)
        df_to_load = duckdb.sql(f"SELECT * FROM {step.df}")
        if step.format == 'parquet':
            df_to_load.write_parquet(step.path, **step.options)
        elif step.format == 'csv':
            df_to_load.write_csv(step.path, **step.options)
        else:
            raise ValueError(f"Unsupported target type: {step.format}")
