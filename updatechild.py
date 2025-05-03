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

# 🔵 Provide your reference ID list here
reference_ids_to_use = []

def update_blank_reference_ids(ref_list):
    """Update blank or missing referenceId fields with IDs from the provided list."""
    # Find customers with blank or missing referenceId
    query = {
        "$or": [
            {"referenceId": {"$exists": False}},
            {"referenceId": ""}
        ]
    }

    customers_to_update = list(collection.find(query, {"_id": 1}))

    print(f"Found {len(customers_to_update)} customers with blank/missing referenceId.")
    print(f"Updating {min(len(ref_list), len(customers_to_update))} of them...")

    updated_count = 0
    for customer, ref_id in zip(customers_to_update, ref_list):
        result = collection.update_one(
            {"_id": customer["_id"]},
            {"$set": {"referenceId": ref_id}}
        )
        if result.modified_count:
            updated_count += 1

    print(f"Successfully updated {updated_count} customers.")

if __name__ == "__main__":
    update_blank_reference_ids(reference_ids_to_use)
