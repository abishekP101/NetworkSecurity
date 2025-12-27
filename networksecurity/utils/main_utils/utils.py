import yaml
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
import os , sys
import pickle
import numpy as np

def read_yaml_file(file_path : str) -> dict:
    try:
        with open(file_path, "rb") as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise NetworkSecurityException(e , sys) from e
    

def write_yaml_file(file_path: str, content: dict) -> None:
    try:
        with open(file_path, "w") as yaml_file:
            yaml.dump(
                content,
                yaml_file,
                default_flow_style=False,
                sort_keys=False
            )

    except Exception as e:
        raise NetworkSecurityException(e, sys)
