from abc import ABC, abstractmethod

import jinja2

from models import *


class BaseETL(ABC):
    def __init__(self):
        self._jinja_env = jinja2.Environment()

    @abstractmethod
    def _extract_step(self, step: ExtractStep):
        print(f"Step {step.name}: \n\tLoading data from {step.path} into {step.df}")

    def extract_data(self, steps: list[ExtractStep]):
        print("Extracting data from sources...")
        for step in steps:
            self._extract_step(step)

    @abstractmethod
    def _transform_step(self, step: TransformStep):
        if step.sql_file:
            with open(step.sql_file, "r") as f:
                sql_template = f.read()
            template = self._jinja_env.from_string(sql_template)
            step.sql = template.render(**step.inputs)
        print(f"Step {step.name}: \n\tTransforming data using SQL: \n{step.sql}")

    def transform_data(self, steps: list[TransformStep]):
        print("Transforming data...")
        for step in steps:
            self._transform_step(step)

    @abstractmethod
    def _load_step(self, step: LoadStep):
        print(f"Step {step.name}: Loading data from {step.df} into {step.path}")

    def load_data(self, steps: list[LoadStep]):
        print("Loading data into targets...")
        for step in steps:
            self._load_step(step)

    def run(self, pipeline: Pipeline):
        print(f'Starting ETL: {pipeline.name}')
        if pipeline.extract_steps:
            self.extract_data(pipeline.extract_steps)
        if pipeline.transform_steps:
            self.transform_data(pipeline.transform_steps)
        if pipeline.load_steps:
            self.load_data(pipeline.load_steps)
        print(f'Finished ETL: {pipeline.name}')
