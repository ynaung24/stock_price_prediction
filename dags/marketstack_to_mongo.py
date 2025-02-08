import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
import logging

# Define base paths
SCRIPTS_PATH = os.path.expanduser("~/Desktop/projects/stock_price_prediction/scripts")
LOG_PATH = os.path.expanduser("~/Desktop/projects/stock_price_prediction/logs/log_script.log")

# Verify paths exist
if not os.path.exists(SCRIPTS_PATH):
    raise ValueError(f"Scripts directory not found at: {SCRIPTS_PATH}")

if not os.path.exists(os.path.dirname(LOG_PATH)):
    os.makedirs(os.path.dirname(LOG_PATH))

# Add scripts path to Python path
sys.path.append(SCRIPTS_PATH)

# Import functions AFTER adding path
from fetch_market_data import fetch_market_data
from store_in_mongo import store_in_mongo

# Configure logging
logger = LoggingMixin().log
logging.basicConfig(filename=LOG_PATH, level=logging.INFO,
                   format="[%(asctime)s] %(levelname)s - %(message)s")


# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "marketstack_to_mongo",
    default_args=default_args,
    description="Fetch daily stock market data and store in MongoDB",
    schedule_interval="@daily",
    catchup=False,
)

# Task to Fetch Data
fetch_task = PythonOperator(
    task_id="fetch_market_data_task",
    python_callable=fetch_market_data,
    provide_context=True,  # Allows passing Airflow's context (XCom)
    dag=dag,
    execution_timeout=timedelta(minutes=5),
)

# Task to Store Data
store_task = PythonOperator(
    task_id="store_market_data_task",
    python_callable=store_in_mongo,
    provide_context=True,  # Allows receiving data from XCom
    dag=dag,
)

# Set Dependencies
fetch_task >> store_task

logging.info("DAG marketstack_to_mongo successfully loaded")
