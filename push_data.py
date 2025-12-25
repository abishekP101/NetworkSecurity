import os
import sys
import json
from dotenv import load_dotenv

import certifi
import pandas as pd
import pymongo

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

# Load environment variables
load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
ca = certifi.where()


class NetworkDataExtract:
    def __init__(self):
        try:
            if MONGO_DB_URL is None:
                raise ValueError("MONGO_DB_URL is not set in environment variables")
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def csv_to_json(self, file_path: str):
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"CSV file not found: {file_path}")

            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)

            if data.empty:
                raise ValueError("CSV file is empty")

            records = json.loads(data.to_json(orient="records"))
            logging.info(f"Converted CSV to JSON with {len(records)} records")

            return records

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def insert_data_mongodb(self, records, database: str, collection: str):
        try:
            if not records:
                raise ValueError("No records provided for insertion")

            mongo_client = pymongo.MongoClient(
                MONGO_DB_URL,
                tlsCAFile=ca
            )

            db = mongo_client[database]
            col = db[collection]

            result = col.insert_many(records)
            inserted_count = len(result.inserted_ids)

            logging.info(
                f"Inserted {inserted_count} records into {database}.{collection}"
            )

            return inserted_count

        except Exception as e:
            raise NetworkSecurityException(e, sys)


if __name__ == "__main__":
    FILE_PATH = os.path.join("Network_Data", "phisingData.csv")
    DATABASE = "Abishek"
    COLLECTION = "NetworkData"

    network = NetworkDataExtract()
    records = network.csv_to_json(FILE_path := FILE_PATH)
    no_of_records = network.insert_data_mongodb(
        records=records,
        database=DATABASE,
        collection=COLLECTION
    )

    print(f"Inserted {no_of_records} records successfully")
