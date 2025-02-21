from pymongo import MongoClient
from datetime import datetime

# MongoDB connection string
connection_string = "mongodb+srv://matt:5OdZeJDF1lXUAld3@makertdata.iinrc.mongodb.net/?retryWrites=true&w=majority&appName=makertdata"
TICKERS = ['TSLA', 'AAPL', 'BA', 'CRYPTO:BTC']

def process_stock_news():
    try:
        # Connect to MongoDB
        client = MongoClient(connection_string)
        db = client['stock_data']
        stock_news_collection = db['stock_news']
        
        # Collections for specific stocks
        tsla_news_collection = db['tsla_news']
        aapl_news_collection = db['aapl_news']
        boeing_news_collection = db['boeing_news']
        btc_news_collection = db['btc_news']
        
        # Test connection
        client.admin.command('ping')
        print("Successfully connected to MongoDB")

        # Process each ticker
        for ticker in TICKERS:
            print(f"\nProcessing {ticker} news...")
            if ticker == 'TSLA':
                target_collection = tsla_news_collection
            elif ticker == 'AAPL':
                target_collection = aapl_news_collection
            elif ticker == 'BA':
                target_collection = boeing_news_collection
            elif ticker == 'CRYPTO:BTC':
                target_collection = btc_news_collection
            
            # Find all articles for this ticker in the main collection
            filtered_articles = stock_news_collection.find(
                {"ticker_sentiment.ticker": ticker},
                {
                    "title": 1,
                    "time_published": 1,
                    "ticker_sentiment.$": 1,
                    "_id": 0
                }
            )

            filtered_articles_list = list(filtered_articles)
            
            if filtered_articles_list:
                # Check for duplicates before inserting
                for article in filtered_articles_list:
                    existing = target_collection.find_one({'title': article['title']})
                    if not existing:
                        target_collection.insert_one(article)
                        print(f"Inserted new article: {article['title']}")
                    else:
                        print(f"Skipped duplicate article: {article['title']}")
                
                print(f"Finished processing {ticker} articles")
            else:
                print(f"No {ticker}-related articles found in main collection.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        client.close()
        print("MongoDB connection closed.")

if __name__ == "__main__":
    process_stock_news()