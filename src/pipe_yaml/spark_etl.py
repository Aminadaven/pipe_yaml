from contextlib import contextmanager

from pyspark.sql import SparkSession, DataFrame

from pipe_yaml.base_etl import *


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


class SparkETL(BaseETL):
    def __init__(self, pipeline: Pipeline):
        super().__init__(pipeline=pipeline)
        self._spark = None

    @staticmethod
    def _prepare_df(df: DataFrame, df_name: str, extra: SparkExtra):
        if extra.cache:
            df.cache()
        if extra.repartition:
            if extra.partition_by:
                df = df.repartition(extra.repartition, *extra.partition_by)
            else:
                df = df.repartition(extra.repartition)
        df.createOrReplaceTempView(df_name)
        return df

    def _extract_step(self, step: ExtractStep):
        super()._extract_step(step)
        data = self._spark.read.load(step.path, step.format, **step.options)
        self._prepare_df(data, step.df, step.extra)

    def _transform_step(self, step: TransformStep):
        super()._transform_step(step)
        data = self._spark.sql(step.sql)
        if step.df:
            self._prepare_df(data, step.df, step.extra)
            return
        data.show()

    def _load_step(self, step: LoadStep):
        super()._load_step(step)
        df = self._spark.sql(f"SELECT * FROM {step.df}")
        df = self._prepare_df(df, step.df, step.extra)
        df.write.mode(step.mode).format(step.format).save(step.path, **step.options)

    def run(self):
        with spark_context_manager(app_name=self._pipeline.name, cores='*', log_level='ERROR') as sc:
            self._spark = sc
            super().run()
