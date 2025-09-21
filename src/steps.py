from typing import Self

from pydantic import BaseModel, Field, model_validator


class ExtractStep(BaseModel):
    name: str = Field(alias="step")
    path: str
    df: str
    format: str
    cache: bool = False
    options: dict = Field(default_factory=dict)


class TransformStep(BaseModel):
    name: str = Field(alias="step")
    sql: str
    df: str = None
    cache: bool = False


class LoadStep(BaseModel):
    name: str = Field(alias="step")
    df: str
    mode: str
    format: str
    path: str
    options: dict = Field(default_factory=dict)


class Pipeline(BaseModel):
    name: str = Field(alias="pipeline", default="Pipeline")
    engine: str = Field(default="spark")
    extract_steps: list[ExtractStep] = Field(alias="extract", default_factory=list)
    transform_steps: list[TransformStep] = Field(alias="transform", default_factory=list)
    load_steps: list[LoadStep] = Field(alias="load", default_factory=list)

    @model_validator(mode='after')
    def validate_pipeline(self) -> Self:
        if self.transform_steps or (self.extract_steps and self.load_steps):
            return self
        raise ValueError('Invalid ETL configuration file')
