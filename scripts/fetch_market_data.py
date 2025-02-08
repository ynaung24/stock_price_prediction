import json
import logging
import sys
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import Connection

# Configure logging
log_file = "/Users/bera/Desktop/projects/stock_price_prediction/logs/log_script.log"
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format="[%(asctime)s] %(levelname)s - %(message)s")

logging.info("Fetching stock market data from MarketStack API")
sys.stdout.flush()
sys.stderr.flush()

def fetch_market_data(**kwargs):
    """
    Fetches daily stock market data from MarketStack API using HttpHook.
    """
    logging.info("Fetching stock market data from MarketStack API")

    try:
        # Initialize HttpHook
        http_hook = HttpHook(method="GET", http_conn_id="http_default")
        logging.info("HttpHook initialized successfully")

        # Retrieve API key from the connection's extra field
        conn = Connection.get_connection_from_secrets("http_default")
        extra = conn.extra_dejson
        api_key = extra.get("access_key")  # Correct way to get the API key
        logging.info(f"Retrieved API key: {api_key[:4]}********")

        if not api_key:
            logging.error("API Key not found in Airflow connection.")
            return []

        # API parameters
        params = {
            "access_key": api_key,  # Use the correct key
            "symbols": "AAPL,GOOGL,MSFT"
        }
        logging.info(f"Using parameters: {params}")

        # Make API request
        response = http_hook.run(endpoint="eod", data=params)
        logging.info(f"HTTP response status: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            records = data.get("data", [])
            logging.info(f"Successfully fetched {len(records)} records from MarketStack API")

            # Push data to XCom
            kwargs["ti"].xcom_push(key="market_data", value=records)
            return records
        else:
            logging.error(f"Error fetching data: {response.status_code} - {response.text}")
            return []

    except Exception as e:
        logging.error(f"Exception occurred: {str(e)}")
        raise
