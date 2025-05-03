from pymongo import MongoClient
import os
from dotenv import load_dotenv
from tabulate import tabulate  # pip install tabulate

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

def find_invalid_reference_ids():
    """Find customers with blank or invalid referenceId."""
    # Step 1: Get all valid customer IDs
    all_ids = set(doc["customerID"] for doc in collection.find({}, {"customerID": 1}))

    # Step 2: Find customers where referenceId is blank or not in all_ids
    query = {
        "$or": [
            {"referenceId": {"$exists": False}},
            {"referenceId": ""},
            {"referenceId": {"$nin": list(all_ids)}}
        ]
    }
    return list(collection.find(query, {"name": 1, "customerID": 1, "referenceId": 1}))

def print_invalid_references(customers):
    """Print invalid reference entries to the terminal."""
    if not customers:
        print("No invalid or blank reference IDs found.")
        return

    table = []
    for idx, user in enumerate(customers, start=1):
        table.append([
            idx,
            user.get("name", "N/A"),
            user.get("customerID", "N/A"),
            user.get("referenceId", "N/A")
        ])
    
    headers = ["S.No", "Name", "Customer ID", "Reference ID"]
    print(tabulate(table, headers, tablefmt="grid"))

if __name__ == "__main__":
    invalid_customers = find_invalid_reference_ids()
    print_invalid_references(invalid_customers)
