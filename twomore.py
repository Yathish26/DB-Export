from pymongo import MongoClient
from dotenv import load_dotenv
import os
from collections import defaultdict

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

def find_over_referred_users():
    """Find customerIDs that have more than 2 users referring to them."""
    reference_map = defaultdict(list)

    # Collect referenceId counts
    for user in collection.find():
        ref_id = user.get("referenceId")
        if ref_id:
            reference_map[ref_id].append(user)

    print("\n🔍 Users who referred more than 2 people:\n")
    found = False
    for ref_id, users in reference_map.items():
        if len(users) > 2:
            found = True
            referrer = collection.find_one({"customerID": ref_id})
            referrer_name = referrer.get("name") if referrer else "Unknown"
            print(f"❗ {ref_id} | {referrer_name} referred {len(users)} users:")
            for u in users:
                print(f"   - {u.get('customerID')} | {u.get('name')}")
            print()

    if not found:
        print("✅ No customer has referred more than 2 users.")

if __name__ == "__main__":
    find_over_referred_users()
