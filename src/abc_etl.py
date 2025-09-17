from abc import ABC, abstractmethod


class ExtractStep:
    def __init__(self, step, path, df, format, options=None):
        self.name = step
        self.path = path
        self.df = df
        self.format = format
        self.options = options or {}


class TransformStep:
    def __init__(self, step, sql, df=None):
        self.name = step
        self.sql = sql
        self.df = df


class LoadStep:
    def __init__(self, step, df, mode, format, path, options=None):
        self.name = step
        self.mode = mode
        self.path = path
        self.df = df
        self.format = format
        self.options = options or {}


class ETL(ABC):

    @abstractmethod
    def _extract_step(self, step: ExtractStep):
        print(f"Step {step.name}: Loading data from {step.path} into {step.df}")

    def extract_data(self, steps: list[ExtractStep]):
        print("Extracting data from sources...")
        for step in steps:
            self._extract_step(step)

    @abstractmethod
    def _transform_step(self, step: TransformStep):
        print(f"Step {step.name}: Transforming data using SQL: {step.sql}")

    def transform_data(self, steps: list[TransformStep]):
        print("Transforming data...")
        for step in steps:
            print(f"Step {step.name}: Transforming data using SQL: {step.sql}")
            self._transform_step(step)

    @abstractmethod
    def _load_step(self, step: LoadStep):
        print(f"Step {step.name}: Loading data from {step.df} into {step.path}")

    def load_data(self, steps: list[LoadStep]):
        print("Loading data into targets...")
        for step in steps:
            self._load_step(step)
