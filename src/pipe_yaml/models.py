from typing import Self

from pydantic import BaseModel, Field, model_validator


class SparkExtra(BaseModel):
    cache: bool = False
    repartition: int = None
    partition_by: list[str] = None


class ExtractStep(BaseModel):
    name: str = Field(alias="step")
    path: str
    df: str
    format: str
    extra: SparkExtra = Field(default_factory=SparkExtra)
    options: dict = Field(default_factory=dict)


class TransformStep(BaseModel):
    name: str = Field(alias="step")
    sql: str = None
    sql_file: str = None
    inputs: dict = Field(default_factory=dict)
    df: str = None
    extra: SparkExtra = Field(default_factory=SparkExtra)

    @model_validator(mode='after')
    def validate_transformation(self) -> Self:
        if bool(self.sql) ^ bool(self.sql_file):
            return self
        raise ValueError('Invalid Transformation: must provide sql or sql_file!')


class LoadStep(BaseModel):
    name: str = Field(alias="step")
    df: str
    mode: str
    format: str
    path: str
    options: dict = Field(default_factory=dict)
    extra: SparkExtra = Field(default_factory=SparkExtra)


class Pipeline(BaseModel):
    name: str = Field(alias="pipeline", default="Pipeline")
    engine: str = Field(default="spark")
    dotenv_file: str = None
    extract_steps: list[ExtractStep] = Field(alias="extract", default_factory=list)
    transform_steps: list[TransformStep] = Field(alias="transform", default_factory=list)
    load_steps: list[LoadStep] = Field(alias="load", default_factory=list)

    @model_validator(mode='after')
    def validate_pipeline(self) -> Self:
        if self.transform_steps or (self.extract_steps and self.load_steps):
            return self
        raise ValueError('Invalid ETL configuration file')
