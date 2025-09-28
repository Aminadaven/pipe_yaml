import os
import sys

import jinja2
from dotenv import load_dotenv
from loguru import logger

from pipe_yaml.models import *

logger.add(sys.stdout, serialize=True)


class BaseETL:
    def __init__(self, pipeline: Pipeline):
        self._jinja_env = jinja2.Environment()
        if pipeline.dotenv_file:
            load_dotenv(pipeline.dotenv_file)
        prepared_env = {f'ENV_{key.upper()}': value for key, value in os.environ.items()}
        self._pipeline = pipeline
        self._state = {}
        self._state.update(**prepared_env)

    def _extract_step(self, step: ExtractStep):
        logger.info(f"Step {step.name}: \n\tLoading data from {step.path} into {step.df}")

    def extract_data(self):
        logger.info("Extracting data from sources...")
        for step in self._pipeline.extract_steps:
            self._extract_step(step)

    def _transform_step(self, step: TransformStep):
        sql_template = step.sql
        if step.sql_file:
            with open(step.sql_file, "r") as f:
                sql_template = f.read()
        template = self._jinja_env.from_string(sql_template)
        step.sql = template.render(**{**self._state, **step.inputs})
        logger.info(f"Step {step.name}: \n\tTransforming data using SQL: \n{step.sql}")

    def transform_data(self):
        logger.info("Transforming data...")
        for step in self._pipeline.transform_steps:
            self._transform_step(step)

    def _load_step(self, step: LoadStep):
        logger.info(f"Step {step.name}: Loading data from {step.df} into {step.path}")

    def load_data(self):
        logger.info("Loading data into targets...")
        for step in self._pipeline.load_steps:
            self._load_step(step)

    def run(self):
        logger.info(f'Starting ETL: {self._pipeline.name}')
        if self._pipeline.extract_steps:
            self.extract_data()
        if self._pipeline.transform_steps:
            self.transform_data()
        if self._pipeline.load_steps:
            self.load_data()
        logger.info(f'Finished ETL: {self._pipeline.name}')
