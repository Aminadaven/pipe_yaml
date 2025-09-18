import argparse

from abc_etl import *
from duckdb_etl import DuckdbETL
from spark_etl import SparkETL, spark_context_manager
from util import *


class Args:
    def __init__(self, args):
        self.etl_file = args.etl_file

    @classmethod
    def from_args(cls):
        parser = argparse.ArgumentParser(prog='ETL Pipeline Runner')
        parser.add_argument('etl_file', type=str, help='Path to the ETL configuration file', default='config.yaml')
        args = parser.parse_args()
        return cls(args)


def parse_etl(etl_file: str):
    config = load_yaml_config(etl_file)
    pipeline = config.get('pipeline', 'Pipeline')
    engine = config.get('engine', 'spark')
    extract_steps = create_steps_from_list(ExtractStep, config.get('extract', []))
    transform_steps = create_steps_from_list(TransformStep, config.get('transform', []))
    load_steps = create_steps_from_list(LoadStep, config.get('load', []))

    return pipeline, engine, extract_steps, transform_steps, load_steps


def main(args: Args):
    pipeline, engine, extract_steps, transform_steps, load_steps = parse_etl(args.etl_file)
    if not extract_steps or (not transform_steps and not load_steps):
        print('Invalid ETL configuration file')
        exit(1)
    print(f'Starting ETL: {pipeline}')

    etl_runner: ETL
    if engine == 'spark':
        with spark_context_manager(app_name=pipeline, cores='*', log_level='ERROR') as sc:
            etl_runner = SparkETL(sc)
    elif engine == 'duckdb':
        etl_runner = DuckdbETL()
    else:
        print(f'Invalid engine: {engine}')
        exit(1)

    etl_runner.extract_data(extract_steps)
    if transform_steps:
        etl_runner.transform_data(transform_steps)
    if load_steps:
        etl_runner.load_data(load_steps)

    print(f'Finished ETL: {pipeline}')


if __name__ == "__main__":
    main(Args.from_args())
