import requests
import json
import yaml
from pymongo import MongoClient
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_and_store_news(**context):
    """
    Fetch news for all tickers and store in MongoDB
    Returns True if successful, False if any errors occurred
    """
    # Define tickers inside the function
    tickers = ['AAPL', 'TSLA', 'BA', 'CRYPTO:BTC']
    overall_success = True
    
    try:
        # Load configuration
        with open('config/config.yml', 'r') as file:
            config = yaml.safe_load(file)

        # Connect to MongoDB
        connection_string = config['mongodb']['uri']
        client = MongoClient(connection_string)
        db = client['stock_data']
        stock_news_collection = db['stock_news']
        
        # Test connection
        client.admin.command('ping')
        logging.info("Successfully connected to MongoDB")

        # Process each ticker
        for ticker in tickers:
            logging.info(f"Processing news for {ticker}")
            try:
                # Define API Key and Endpoint from config
                API_KEY = config['alpha_vantage']['api_key']
                url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&apikey={API_KEY}"

                # Make API Request
                response = requests.get(url)
                
                if response.status_code != 200:
                    logging.error(f"Error fetching news data for {ticker}: {response.status_code}")
                    overall_success = False
                    continue

                # Parse JSON Data
                data = response.json()
                news_articles = data.get("feed", [])
                logging.info(f"Fetched {len(news_articles)} articles for {ticker}")

                # Insert articles
                articles_added = 0
                for article in news_articles:
                    try:
                        # Add metadata
                        article['stored_at'] = datetime.utcnow()
                        article['ticker'] = ticker
                        
                        # Check for duplicates
                        existing = stock_news_collection.find_one({'title': article['title']})
                        if existing:
                            logging.debug(f"Article already exists: {article['title']}")
                            continue
                            
                        # Insert the document
                        result = stock_news_collection.insert_one(article)
                        articles_added += 1
                        logging.debug(f"Inserted article with ID: {result.inserted_id}")

                    except Exception as e:
                        logging.error(f"Error inserting article for {ticker}: {e}")
                        overall_success = False

                logging.info(f"Added {articles_added} new articles for {ticker}")

            except Exception as e:
                logging.error(f"Error processing {ticker}: {e}")
                overall_success = False

    except Exception as e:
        logging.error(f"Major error in news collection process: {e}")
        overall_success = False

    finally:
        if 'client' in locals():
            client.close()
            logging.info("MongoDB connection closed")

    return overall_success