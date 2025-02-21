import os
import pandas as pd
import pymongo
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Connect to MongoDB Atlas
# We can copy the URI directly from Compass, pressing the three dots next to 
# the cluster, then clicking on Copy connection string. Put this in your .env file
# since it also contains your username and password. Put it within quotations
# MONGO_URI = "connection_string"
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = ""   # MONGO DB name within quotation
MONGO_COLLECTION = ""   # MONGO Collection name within quotation

client = pymongo.MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

# Load CSV into Pandas DataFrame
df = pd.read_csv("")    # location of file

# Convert DataFrame to a dictionary format for MongoDB
data = df.to_dict(orient="records")

# Insert into MongoDB
collection.insert_many(data)

print(f"Successfully uploaded {len(data)} documents to MongoDB!")
