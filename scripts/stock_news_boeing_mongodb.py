import requests
import json
import yaml
from pymongo import MongoClient
from datetime import datetime

# Load configuration
with open('config/config.yml', 'r') as file:
    config = yaml.safe_load(file)

# Define API Key and Endpoint from config
API_KEY = config['alpha_vantage']['api_key']
ticker = 'BA'

url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&apikey={API_KEY}"

# Make API Request
response = requests.get(url)

# Parse JSON Data
if response.status_code == 200:
    data = response.json()
    news_articles = data.get("feed", [])

    # Print News Articles
    # for article in news_articles:
    #     print(f"Title: {article['title']}")
    #     print(f"Published Date: {article['time_published']}")
    #     print(f"Summary: {article['summary']}")
    #     print(f"Sentiment Score: {article['overall_sentiment_score']}")
else:
    print("Error fetching news data")

# Use connection string from config
connection_string = config['mongodb']['uri']
database_name = config['mongodb']['database']
collection_name = config['mongodb']['collection']

# Connect to MongoDB
try:
    client = MongoClient(connection_string)
    db = client[database_name]
    stock_news_collection = db[collection_name]
    boeing_news_collection = db['boeing_news']
    
    # Test connection
    client.admin.command('ping')
    print("Successfully connected to MongoDB")

    # Insert each article into MongoDB
    for article in news_articles:        
        try:
            # Add timestamp and check for duplicates
            article['stored_at'] = datetime.utcnow()
            article['ticker'] = ticker
            
            # Check if article already exists
            existing = stock_news_collection.find_one({'title': article['title']})
            if existing:
                print(f"Article already exists: {article['title']}")
                continue
                
            # Insert the document
            result = stock_news_collection.insert_one(article)
            print(f"Inserted article with ID: {result.inserted_id}")
        except Exception as e:
            print(f"Error inserting article: {e}")

    print("Finished uploading articles to MongoDB")

except Exception as e:
    print(f"Error: {e}")

finally:
    client.close()
    print("MongoDB connection closed.")