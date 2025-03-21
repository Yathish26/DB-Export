from pymongo import MongoClient
import json
import json
import os
from dotenv import load_dotenv

load_dotenv()

mongo_uri = os.getenv("MONGO_URI")

# Enter your MongoDB connection URI and database name
connection_string = mongo_uri
database_name = "Customers"  

try:
    # Connect to the MongoDB server
    client = MongoClient(connection_string)

    # Access the specified database
    db = client[database_name]

    # Export all collections
    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        data = list(collection.find({}))

        # Serialize ObjectId and save to a JSON file
        for doc in data:
            doc["_id"] = str(doc["_id"])
        
        with open(f"{collection_name}.json", "w") as json_file:
            json.dump(data, json_file, indent=4)

    print("Data exported successfully.")
except Exception as e:
    print("An error occurred:", str(e))
