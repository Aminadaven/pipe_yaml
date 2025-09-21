from contextlib import contextmanager

from pyspark.sql import SparkSession, DataFrame

from etl import *


@contextmanager
def spark_context_manager(app_name=None, cores='*', log_level='WARN', **kwargs):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(f'local[{cores}]')
        # Force Spark to use the raw local filesystem (bypasses winutils.exe)
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config(map=kwargs or {})
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(log_level)

    try:
        yield spark
    finally:
        spark.stop()


class SparkETL(ETL):
    def __init__(self):
        self._spark = None

    @staticmethod
    def _prepare_df(df: DataFrame, df_name: str, cache: bool = False):
        if cache:
            df.cache()
        df.createOrReplaceTempView(df_name)
        return df

    def _extract_step(self, step: ExtractStep):
        super()._extract_step(step)
        data = self._spark.read.load(step.path, step.format, **step.options)
        self._prepare_df(data, step.df, step.cache)

    def _transform_step(self, step: TransformStep):
        super()._transform_step(step)
        data = self._spark.sql(step.sql)
        if step.df:
            self._prepare_df(data, step.df, step.cache)
            return
        data.show()

    def _load_step(self, step: LoadStep):
        super()._load_step(step)
        df = self._spark.sql(f"SELECT * FROM {step.df}")
        df.write.mode(step.mode).format(step.format).save(step.path, **step.options)

    def run(self, pipeline: Pipeline):
        with spark_context_manager(app_name=pipeline.name, cores='*', log_level='ERROR') as sc:
            self._spark = sc
            super().run(pipeline)
