import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict
import logging
from urllib.parse import urlparse
import os
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='news_scraper.log'
)

class NewsAPIConfig:
    API_KEY = os.getenv('NEWS_API_KEY')
    BASE_URL = "https://newsapi.org/v2/everything"
    
    # Keywords to improve relevance
    COMPANY_KEYWORDS = [
        "Apple Inc",
        "AAPL",
        "Tim Cook",
        "iPhone",
        "MacBook",
        "iOS",
        "Apple technology"
    ]
    
    # Keywords to exclude irrelevant content
    EXCLUDE_KEYWORDS = [
        "fruit",
        "apple tree",
        "apple pie",
        "apple cider",
        "cooking"
    ]
    
    # Trusted financial news sources
    TRUSTED_SOURCES = [
        "reuters.com",
        "bloomberg.com",
        "wsj.com",
        "cnbc.com",
        "ft.com",
        "fool.com",
        "marketwatch.com",
        "finance.yahoo.com"
    ]

class NewsCollector:
    def __init__(self, config: NewsAPIConfig):
        self.config = config
        self.data_dir = 'collected_news'
        os.makedirs(self.data_dir, exist_ok=True)

    def is_relevant_article(self, article: Dict) -> bool:
        """Validate if the article is relevant to Apple Inc."""
        text = f"{article['title']} {article['description']}".lower()
        
        # Check for excluded keywords
        if any(kw.lower() in text for kw in self.config.EXCLUDE_KEYWORDS):
            return False
            
        # Validate source domain
        source_domain = urlparse(article['url']).netloc
        if not any(trusted in source_domain for trusted in self.config.TRUSTED_SOURCES):
            return False
            
        # Ensure article is about the company
        relevant_keywords_count = sum(1 for kw in self.config.COMPANY_KEYWORDS 
                                   if kw.lower() in text)
        return relevant_keywords_count >= 2

    def get_stock_news(self, days_back: int = 7) -> List[Dict]:
        """Get news articles about Apple Inc."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # More inclusive query for Apple Inc.
        base_query = (
            '(AAPL OR "Apple" OR "Tim Cook" OR "iPhone" OR "MacBook")'
            ' AND (tech OR technology OR stock OR market OR business OR company)'
            ' -fruit -recipe -food'
        )
        
        params = {
            'q': base_query,
            'from': start_date.strftime('%Y-%m-%d'),
            'to': end_date.strftime('%Y-%m-%d'),
            'language': 'en',
            'sortBy': 'publishedAt',
            # Expanded list of domains
            'domains': 'reuters.com,bloomberg.com,cnbc.com,finance.yahoo.com,marketwatch.com,fool.com,investors.com,ft.com,wsj.com,seekingalpha.com,barrons.com,businessinsider.com,techcrunch.com,appleinsider.com,macrumors.com',
            'apiKey': self.config.API_KEY
        }
        
        try:
            response = requests.get(self.config.BASE_URL, params=params)
            response.raise_for_status()
            
            # Print the actual query for debugging
            print(f"API URL (without key): {response.url.split('apiKey=')[0]}")
            
            articles = response.json()['articles']
            print(f"Total articles before filtering: {len(articles)}")
            
            processed_articles = []
            for article in articles:
                # Slightly relaxed validation
                text = f"{article['title']} {article['description']}".lower()
                
                # Must contain at least one of these terms
                apple_terms = ['apple', 'aapl', 'iphone', 'ipad', 'macbook', 'tim cook', 'ios']
                if any(term in text.lower() for term in apple_terms):
                    article_data = {
                        'title': article['title'],
                        'published_at': article['publishedAt'],
                        'source': article['source']['name'],
                        'source_domain': urlparse(article['url']).netloc,
                        'description': article['description'],
                        'url': article['url'],
                        'collected_at': datetime.now().isoformat()
                    }
                    processed_articles.append(article_data)
                    
            print(f"Articles after filtering: {len(processed_articles)}")
            logging.info(f"Processed {len(processed_articles)} relevant articles")
            return processed_articles
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching news: {e}")
            print("API Request Error:", str(e))
            return []

    def save_news_locally(self, articles: List[Dict], format: str = 'both') -> None:
        """Save the collected news articles locally in specified format(s)"""
        if not articles:
            logging.warning("No articles to save")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = f"apple_news_{timestamp}"

        if format in ['json', 'both']:
            json_path = os.path.join(self.data_dir, f"{base_filename}.json")
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(articles, f, indent=4, ensure_ascii=False)
            logging.info(f"Saved {len(articles)} articles to {json_path}")

        # if format in ['csv', 'both']:
        #     csv_path = os.path.join(self.data_dir, f"{base_filename}.csv")
        #     df = pd.DataFrame(articles)
        #     df.to_csv(csv_path, index=False, encoding='utf-8')
        #     logging.info(f"Saved articles to {csv_path}")

def main():
    # Initialize components
    config = NewsAPIConfig()
    collector = NewsCollector(config)
    
    # Collect and save news
    articles = collector.get_stock_news()
    collector.save_news_locally(articles)
    
    # Print summary
    print(f"\nCollection Summary:")
    print(f"Total articles collected: {len(articles)}")
    print(f"Saved to directory: {collector.data_dir}")
    print(f"Files saved with timestamp: {datetime.now().strftime('%Y%m%d_%H%M%S')}")

if __name__ == "__main__":
    main()