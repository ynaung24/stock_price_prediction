import requests
import logging
import os
from scripts.config_loader import load_config

# Load configuration
config = load_config()
MARKETSTACK_API_KEY = config["marketstack"]["api_key"]

# Configure logging
log_file = os.path.expanduser("~/Desktop/projects/dds_t11/logs/log_script.log")
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format="[%(asctime)s] %(levelname)s - %(message)s")

def fetch_market_data():
    """Fetches daily stock market data from MarketStack API."""
    logging.info("Fetching stock market data from MarketStack API")

    url = f"http://api.marketstack.com/v1/eod?access_key={MARKETSTACK_API_KEY}&symbols=AAPL,GOOGL,MSFT"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        logging.info(f"Successfully fetched {len(data.get('data', []))} records from MarketStack API")
        return data.get("data", [])
    else:
        logging.error(f"Error fetching data: {response.status_code} - {response.text}")
        return []
