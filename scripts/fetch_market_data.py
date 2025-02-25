import json
import logging
import sys
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import Connection
import yaml
import os


# Configure logging
log_file = "/Users/bera/Desktop/projects/stock_price_prediction/logs/log_script.log"
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format="[%(asctime)s] %(levelname)s - %(message)s")

def fetch_market_data(**kwargs):
    """
    Fetches daily stock market data from MarketStack API using HttpHook.
    """
    logging.info("Fetching stock market data from MarketStack API")

    try:
        # Initialize HttpHook
        http_hook = HttpHook(method="GET", http_conn_id="http_default")
        logging.info("HttpHook initialized successfully")

        config_path = os.path.expanduser("~/Desktop/projects/stock_price_prediction/config/config.yml")

        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                api_key = config.get('marketstack', {}).get('api_key')
        except Exception as config_error:
            logging.error(f"Failed to read API key from config file: {config_error}")
            return []

        if not api_key:
            logging.error("API Key not found in config file.")
            return []

        logging.info(f"Retrieved API key: {api_key[:4]}********")

        # API parameters
        params = {
            "access_key": api_key,
            "symbols": "TSLA,AAPL,BA,GOOGL,GBTC",  # Tesla, Apple, Boeing, Google, Bitcoin Trust
            "date_from": "2025-01-01",  # Starting date
            "date_to": "2025-02-01",    # End date
            "limit": 1000               # Number of records to return
        }
        logging.info(f"Using parameters: {params}")

        # Make API request
        response = http_hook.run(endpoint="eod", data=params)
        logging.info(f"HTTP response status: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            records = data.get("data", [])
            logging.info(f"Successfully fetched {len(records)} records from MarketStack API")

            # Ensure 'ti' exists before attempting XCom push
            ti = kwargs.get("ti")
            if ti:
                ti.xcom_push(key="market_prices", value=records)
                logging.info("Pushed market data to XCom successfully.")
            else:
                logging.warning("No task instance ('ti') found, skipping XCom push.")

            return records

        else:
            logging.error(f"Error fetching data: {response.status_code} - {response.text}")
            return []

    except Exception as e:
        logging.error(f"Exception occurred: {str(e)}")
        raise
