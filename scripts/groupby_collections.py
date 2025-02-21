import os
from pymongo import MongoClient
import yaml

# Get the directory containing the script
script_dir = os.path.dirname(os.path.abspath(__file__))
# Construct the path to config.yml in the config directory
config_path = os.path.join(script_dir, '..', 'config', 'config.yml')

# Load configuration
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

# Connect to MongoDB using configuration
client = MongoClient(config['mongodb']['uri'])
db = client[config['mongodb']['database']]
original_collection = db[config['mongodb']['collection']]

def group_and_insert():
    # Fetch all documents from the original collection
    all_documents = list(original_collection.find({}))

    grouped_data = {}
    # Group documents by 'symbol'
    for doc in all_documents:
        symbol = doc.get("symbol")  # Get the stock symbol
        if symbol:
            if symbol not in grouped_data:
                grouped_data[symbol] = []
            grouped_data[symbol].append(doc)

    # Insert grouped data into new collections
    for symbol, documents in grouped_data.items():
        collection_name = f"{symbol.lower()}_prices"  # Naming format for new collections
        new_collection = db[collection_name]
        
        # Insert data into the new collection
        new_collection.insert_many(documents)
        print(f"Inserted {len(documents)} documents into {collection_name}")

    print("Grouping and insertion completed!")

# Run the function
group_and_insert()

# Close the MongoDB connection
client.close()