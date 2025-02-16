import pymongo
import logging
import os
from fetch_market_data import fetch_market_data
from config_loader import load_config

# Load configuration
config = load_config()
MONGO_URI = config["mongodb"]["uri"]
DB_NAME = config["mongodb"]["database"]
COLLECTION_NAME = config["mongodb"]["collection"]

# Configure logging
log_file = os.path.expanduser("~/Desktop/projects/stock_price_prediction/logs/log_script.log")
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format="[%(asctime)s] %(levelname)s - %(message)s")

def store_in_mongo(**kwargs):
    """Stores stock market data in MongoDB Atlas."""
    logging.info("Connecting to MongoDB Atlas")
    
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # Fetch Airflow task instance if available
        ti = kwargs.get("ti")
        
        # Retrieve market data from XCom if available
        market_data = ti.xcom_pull(task_ids="fetch_market_data_task") if ti else fetch_market_data()

        if not market_data:
            logging.warning("No market data found to insert")
            return
        
        collection.insert_many(market_data)
        logging.info(f"Inserted {len(market_data)} records into MongoDB Atlas")
        logging.info("MongoDB Atlas storage process completed")
    
    except pymongo.errors.ConnectionError as ce:
        logging.error(f"MongoDB Connection Error: {ce}")
    except Exception as e:
        logging.error(f"Error in store_in_mongo: {e}")
