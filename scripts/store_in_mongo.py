import pymongo
import logging
import os
from fetch_market_data import fetch_market_data
from config_loader import load_config

# Load configuration
config = load_config()
MONGO_URI = config["mongodb"]["uri"]
DB_NAME = config["mongodb"]["database"]
COLLECTION_NAME = config["mongodb"]["collections"]["market_prices"]

# Configure logging
log_file = os.path.expanduser("~/Desktop/projects/stock_price_prediction/logs/log_script.log")
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format="[%(asctime)s] %(levelname)s - %(message)s")

def store_in_mongo(**kwargs):
    """Stores stock market data in MongoDB Atlas, avoiding duplicates."""
    logging.info("Connecting to MongoDB Atlas")
    
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # Retrieve market data from XCom or directly fetch if not using Airflow
        ti = kwargs.get("ti")
        market_data = ti.xcom_pull(task_ids="fetch_market_data_task") if ti else fetch_market_data()

        if not market_data:
            logging.warning("No market data found to insert")
            return
        
        # Avoid inserting duplicates
        existing_symbols = {doc["symbol"] for doc in collection.find({}, {"symbol": 1})}
        new_data = [record for record in market_data if record["symbol"] not in existing_symbols]

        if new_data:
            collection.insert_many(new_data)
            logging.info(f"Inserted {len(new_data)} new records into MongoDB Atlas")
        else:
            logging.info("No new records to insert, avoiding duplicates.")

        logging.info("MongoDB Atlas storage process completed")

    except pymongo.errors.ConnectionError as ce:
        logging.error(f"MongoDB Connection Error: {ce}")
    except Exception as e:
        logging.error(f"Error in store_in_mongo: {e}")
