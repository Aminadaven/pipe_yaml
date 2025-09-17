from contextlib import contextmanager

from pyspark.sql import SparkSession

from abc_etl import *


@contextmanager
def spark_context_manager(app_name=None, cores='*', log_level='WARN'):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(f'local[{cores}]')
        # Force Spark to use the raw local filesystem (bypasses winutils.exe)
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(log_level)

    try:
        yield spark
    finally:
        spark.stop()


class SparkETL(ETL):

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def _extract_step(self, step: ExtractStep):
        super()._extract_step(step)
        data = self._spark.read.load(step.path, step.format, **step.options)
        data.createOrReplaceTempView(step.df)

    def _transform_step(self, step: TransformStep):
        super()._transform_step(step)
        if step.df:
            self._spark.sql(step.sql).createOrReplaceTempView(step.df)
            return
        self._spark.sql(step.sql).show()

    def _load_step(self, step: LoadStep):
        super()._load_step(step)
        df = self._spark.sql(f"SELECT * FROM {step.df}")
        df.write.mode(step.mode).format(step.format).save(step.path, **step.options)
