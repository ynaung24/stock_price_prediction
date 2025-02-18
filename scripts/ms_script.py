import os
import requests
import pandas as pd
from dotenv import load_dotenv

# Load API key from .env file
load_dotenv()
API_KEY = os.getenv("MARKETSTACK_API_KEY")

# Base URL
BASE_URL = "https://api.marketstack.com/v2"

def fetch_stock_data(symbol, start_date, end_date):
    """Fetch historical stock data from MarketStack API"""
    
    endpoint = f"{BASE_URL}/eod"
    params = {
        "access_key": API_KEY,
        "symbols": symbol, # change stock ticker
        "date_from": start_date, # change start date
        "date_to": end_date, # change end date
        "limit": 365
    }
    
    response = requests.get(endpoint, params=params)
    
    if response.status_code == 200:
        data = response.json()
        return data.get("data", [])
    else:
        print("Error fetching data:", response.json())
        return None

def save_to_csv(stock_data, symbol, start_date, end_date):
    """Save stock data to a CSV file"""
    if not stock_data:
        print("No data to save.")
        return
    
    df = pd.DataFrame(stock_data)
    
    # Keep only relevant columns
    df = df[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
    
    # Convert date to datetime format
    df['date'] = pd.to_datetime(df['date'])
    
    # Use existing data_MS directory
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data_MS')
    
    # Save to CSV with date range in filename
    filename = f"{symbol}_{start_date}_to_{end_date}_stock_data.csv"
    filepath = os.path.join(data_dir, filename)
    df.to_csv(filepath, index=False)
    print(f"Data saved to {filepath}")

if __name__ == "__main__":
    # User Input
    stock_symbol = input("Enter stock symbol (e.g., AAPL): ").strip().upper()
    start_date = input("Enter start date (YYYY-MM-DD): ").strip()
    end_date = input("Enter end date (YYYY-MM-DD): ").strip()

    # Fetch and Save Data
    stock_data = fetch_stock_data(stock_symbol, start_date, end_date)
    if stock_data:
        save_to_csv(stock_data, stock_symbol, start_date, end_date)
    else:
        print("No data found.")