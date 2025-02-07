from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import os
from scripts.store_in_mongo import store_in_mongo

# Configure logging for DAG
log_file = os.path.expanduser("~/Desktop/projects/dds_t11/logs/log_script.log")
logging.basicConfig(filename=log_file, level=logging.INFO,
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
    schedule="0 17 * * *",  # Runs every day at 5 PM UTC
    catchup=False,
)

# Task to fetch and store market data
store_task = PythonOperator(
    task_id="fetch_and_store_market_data",
    python_callable=store_in_mongo,
    dag=dag,
)

logging.info("DAG marketstack_to_mongo successfully loaded")

store_task
