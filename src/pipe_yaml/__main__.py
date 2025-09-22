import argparse

import yaml

from pipe_yaml.etl_factory import EtlFactory
from pipe_yaml.models import *


class Args:
    def __init__(self, args):
        self.etl_file = args.etl_file

    @classmethod
    def from_args(cls):
        parser = argparse.ArgumentParser(prog='ETL Pipeline Runner')
        parser.add_argument('etl_file', type=str, help='Path to the ETL configuration file', default='config.yaml')
        args = parser.parse_args()
        return cls(args)


def load_pipeline(args):
    with open(args.etl_file, "r") as f:
        config = yaml.safe_load(f)
    return Pipeline(**config)


def main():
    args = Args.from_args()
    pipeline = load_pipeline(args)
    etl_runner = EtlFactory.create_etl(pipeline.engine)
    etl_runner.run(pipeline)


if __name__ == "__main__":
    main()
