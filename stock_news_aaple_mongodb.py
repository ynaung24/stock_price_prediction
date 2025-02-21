import requests
import json
import datetime
from pymongo import MongoClient
from datetime import datetime

# Define API Key and Endpoint
API_KEY = "2G8Q3N81G8L5U7ZE"
ticker = 'AAPL'

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


# Use the connection string directly since it contains all necessary credentials
connection_string = "mongodb+srv://matt:5OdZeJDF1lXUAld3@makertdata.iinrc.mongodb.net/?retryWrites=true&w=majority&appName=makertdata"

# Connect to MongoDB
try:
    client = MongoClient(connection_string)
    db = client['stock_data']
    stock_news_collection = db['stock_news']
    aapl_news_collection = db['aapl_news']
    
    # Test connection
    client.admin.command('ping')
    print("Successfully connected to MongoDB")

    # Insert each article into MongoDB
    for article in news_articles:        
        try:
            # Add timestamp and check for duplicates
            article['stored_at'] = datetime.utcnow()
            
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