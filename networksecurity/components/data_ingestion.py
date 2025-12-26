import os
import sys
import numpy as np
import pandas as pd
import pymongo
from sklearn.model_selection import train_test_split
from dotenv import load_dotenv

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.entity.config_entity import DataIngestionConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact
from networksecurity.logging.logger import logging

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")


class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        try:
            self.config = data_ingestion_config
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def export_collection_as_dataframe(self) -> pd.DataFrame:
        """
        Reads MongoDB collection and returns a cleaned DataFrame.
        """
        try:
            logging.info("Connecting to MongoDB")

            with pymongo.MongoClient(MONGO_DB_URL) as client:
                collection = client[self.config.database_name][
                    self.config.collection_name
                ]
                data = list(collection.find())

            if not data:
                raise ValueError("MongoDB collection is empty")

            df = pd.DataFrame(data)

            if "_id" in df.columns:
                df.drop(columns=["_id"], inplace=True)

            df.replace({"na": np.nan}, inplace=True)

            logging.info("Successfully exported MongoDB collection to DataFrame")
            return df

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def export_data_into_feature_store(self, dataframe: pd.DataFrame) -> None:
        """
        Saves DataFrame into feature store as CSV.
        """
        try:
            feature_store_path = self.config.feature_store_file_path
            os.makedirs(os.path.dirname(feature_store_path), exist_ok=True)

            dataframe.to_csv(feature_store_path, index=False)
            logging.info(f"Feature store saved at {feature_store_path}")

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def split_data_as_train_test(self, dataframe: pd.DataFrame) -> None:
        """
        Splits data into train and test sets and saves them.
        """
        try:
            logging.info("Performing train-test split")

            train_set, test_set = train_test_split(
                dataframe,
                test_size=self.config.train_test_split_ratio,
                random_state=42
            )

            os.makedirs(
                os.path.dirname(self.config.training_file_path), exist_ok=True
            )
            os.makedirs(
                os.path.dirname(self.config.testing_file_path), exist_ok=True
            )

            train_set.to_csv(
                self.config.training_file_path, index=False
            )
            test_set.to_csv(
                self.config.testing_file_path, index=False
            )

            logging.info("Train-test split completed and files saved")

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        """
        Orchestrates the complete data ingestion process.
        """
        try:
            logging.info("Starting data ingestion process")

            dataframe = self.export_collection_as_dataframe()
            self.export_data_into_feature_store(dataframe)
            self.split_data_as_train_test(dataframe)

            artifact = DataIngestionArtifact(
                trained_file_path=self.config.training_file_path,
                test_file_path=self.config.testing_file_path,
            )

            logging.info("Data ingestion completed successfully")
            return artifact

        except Exception as e:
            raise NetworkSecurityException(e, sys)
