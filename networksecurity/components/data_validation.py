from networksecurity.entity.artifact_entity import DataIngestionArtifact , DataValidationArtifact
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.logging.logger import logging
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.constants.training_pipeline import SCHEMA_FILE_PATH
from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file
from scipy.stats import ks_2samp
import pandas as pd
import os , sys

class DataValidation:
    def __init__(self , data_ingestion_artifact: DataIngestionArtifact, data_validation_config: DataValidationConfig):
        try:
            self.data_ingestion_artifact=data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self.schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise NetworkSecurityException(e , sys)
        
    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e , sys)

    def validate_number_of_columns(self , dataframe: pd.DataFrame) -> bool:
        try:
            number_of_columns = len(self.schema_config)
            logging.info(f"Required number of colums: {number_of_columns}")
            logging.info(f"Dataframe has columns: {len(dataframe.columns)}")
            if len(dataframe.columns)==number_of_columns:
                return True
            else:
                return False
        except Exception as e:
            raise NetworkSecurityException(e , sys)
        
    def detect_data_drift(self, base_df, current_df, threshold=0.05) -> bool:
        try:
            drift_report = {}

            for col in base_df.columns:
                if pd.api.types.is_numeric_dtype(base_df[col]):
                    ks_stat, p_value = ks_2samp(
                        base_df[col],
                        current_df[col]
                    )

                    drift_report[col] = {
                        "ks_statistic": float(ks_stat),
                        "p_value": float(p_value),
                        "drift_detected": p_value < threshold
                    }

            drifted_features = [
                col for col, report in drift_report.items()
                if report["drift_detected"]
            ]

            # Save drift report (ALWAYS save, drift or no drift)
            drift_report_file_path = self.data_validation_config.drift_report_file_path
            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path, exist_ok=True)

            write_yaml_file(
                file_path=drift_report_file_path,
                content=drift_report
            )

            if drifted_features:
                logging.warning(
                    f"Data drift detected in columns: {drifted_features}"
                )
                return False

            logging.info("No data drift detected")
            return True

        except Exception as e:
            raise NetworkSecurityException(e, sys)


    def initiate_data_validation(self)-> DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            # read the data from train and test

            train_dataframe = DataValidation.read_data(train_file_path)
            test_dataframe = DataValidation.read_data(test_file_path)

            ## validate number of columns
            status = self.validate_number_of_columns(dataframe=train_dataframe)
            if not status:
                error_message = f"Train dataframe does not contain all columns. \n"
            status = self.validate_number_of_columns(dataframe=test_dataframe)
            if not status:
                error_message = f"Test dataframe does not contain all the columns. \n"

            status = self.detect_data_drift(base_df=train_dataframe , current_df=test_dataframe)
            dir_path= os.path.dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path , exist_ok=True)

            train_dataframe.to_csv(
                self.data_validation_config.valid_train_file_path , index=False , header=True
            )
            test_dataframe.to_csv(
                self.data_validation_config.valid_test_file_path , index=False , header=True
            )

            data_validation_artifact = DataValidationArtifact(
                validation_status= status,
                valid_train_file_path=self.data_ingestion_artifact.trained_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_test_file_path=None,
                invalid_train_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )
            return data_validation_artifact
        except Exception as e:
            raise NetworkSecurityException(e , sys)
        

