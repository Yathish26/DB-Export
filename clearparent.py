from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB Connection
mongo_uri = os.getenv("MONGO_URI")
database_name = "Customers"
collection_name = "CData"

try:
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]
except Exception as e:
    print("Database connection error:", str(e))
    exit()

def clear_invalid_reference_ids():
    """Clear invalid or blank referenceId, excluding customer 'Hope' with ID SS0000000002."""

    # Step 1: Get all valid customer IDs from the database
    print("Fetching valid customer IDs...")
    all_ids = set(
        doc["customerID"]
        for doc in collection.find({}, {"customerID": 1})
        if "customerID" in doc
    )

    # Step 2: Build the query to find invalid referenceIds (excluding 'Hope')
    query = {
        "$and": [
            {
                "$or": [
                    {"referenceId": {"$exists": False}},
                    {"referenceId": ""},
                    {"referenceId": {"$nin": list(all_ids)}}
                ]
            },
            {
                "$nor": [
                    {"customerID": "SS0000000002"},
                    {"name": "Hope"}
                ]
            }
        ]
    }

    # Step 3: Perform the update to set referenceId = ""
    print("Updating records with invalid reference IDs...")
    result = collection.update_many(query, {"$set": {"referenceId": ""}})
    print(f"✅ Cleared invalid referenceId for {result.modified_count} customer(s).")

if __name__ == "__main__":
    clear_invalid_reference_ids()
