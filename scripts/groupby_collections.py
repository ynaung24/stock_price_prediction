from pymongo import MongoClient

client = MongoClient("mongodb+srv://yan:lnK87PLgiY9rjkpy@makertdata.iinrc.mongodb.net/?retryWrites=true&w=majority&appName=makertdata")  # Update if using MongoDB Atlas
db = client["stock_data"]  # Replace with your database name
original_collection = db["market_prices"]  # Replace with your collection name

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
        collection_name = f"stocks_{symbol.lower()}"  # Naming format for new collections
        new_collection = db[collection_name]
        
        # Insert data into the new collection
        new_collection.insert_many(documents)
        print(f"Inserted {len(documents)} documents into {collection_name}")

    print("Grouping and insertion completed!")

# Run the function
group_and_insert()

# Close the MongoDB connection
client.close()