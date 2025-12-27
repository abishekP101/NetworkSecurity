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
    

def save_numpy_array(file_path: str , array:  np.array):
    """
    Docstring for save_numpy_array
    
    :param file_path: Description
    :type file_path: str
    :param array: Description
    :type array: np.array
    """
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path , exist_ok=True)
        with open(file_path , "wb") as file_obj:
            np.save(file_obj , array)
    except Exception as e:
        raise NetworkSecurityException(e , sys) from e
    
def save_object(file_path: str , obj: object) -> None:
    try:
        logging.info("Entered the save_object method of MainUtils class")
        os.makedirs(os.path.dirname(file_path) , exist_ok=True)
        with open(file_path , "wb") as file_obj:
            pickle.dump(obj, file_obj)
        logging.info("Exited the save_object method of MainUtils class")

    except Exception as e:
        raise NetworkSecurityException(e ,sys) from e
