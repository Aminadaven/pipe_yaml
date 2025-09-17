from typing import Type

import yaml


def load_yaml_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    return config


def create_steps_from_list(step_type: Type, step_list: list[dict[str, str]]):
    return [step_type(**step) for step in step_list]
