from pymongo import MongoClient
import os
from dotenv import load_dotenv
from bson.objectid import ObjectId
from datetime import datetime
from colorama import Fore, Back, Style, init

# Initialize colorama
init(autoreset=True)

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

def find_descendants(customer_id):
    """Find all child and grandchild users under a given customer ID."""
    descendants = []
    queue = [customer_id]

    while queue:
        current_id = queue.pop(0)
        for user in collection.find({"referenceId": current_id}):
            descendants.append(user)
            queue.append(user.get("customerID"))    

    return descendants

def get_joined_date_from_objectid(oid):
    """Extract joined date from MongoDB ObjectId."""
    try:
        return ObjectId(oid).generation_time
    except Exception:
        return None

def print_descendants(descendants, root_user, root_customer_id):
    print("=" * 80)
    print(f"Downline Members of {root_user.get('name', 'Unknown')} (ID: {root_customer_id})")
    print(f"Total Members: {len(descendants)}")
    print("=" * 80)

    # Table Header
    header = f"{'Customer ID':<15} {'Name':<20} {'Mobile':<15} {'Place':<15} {'Joined Date':<12}"
    print(header)
    print("-" * len(header))

    # Table Data
    for user in descendants:
        customer_id = user.get("customerID", "N/A")
        name = user.get("name", "N/A")
        mobile = user.get("mobile", "N/A")
        place = user.get("place", "N/A")
        joined_date = get_joined_date_from_objectid(user.get("_id"))
        joined_str = joined_date.strftime("%Y-%m-%d") if joined_date else "N/A"

        if joined_date and joined_date.month in [3, 4]:
            print(Fore.BLACK + Back.YELLOW + f"{customer_id:<15} {name:<20} {mobile:<15} {place:<15} {joined_str:<12}")
        else:
            print(f"{customer_id:<15} {name:<20} {mobile:<15} {place:<15} {joined_str:<12}")

if __name__ == "__main__":
    root_customer_id = input("Enter the root customer ID: ")
    root_user = collection.find_one({"customerID": root_customer_id})

    if not root_user:
        print(f"No user found with customer ID: {root_customer_id}")
    else:
        descendants = find_descendants(root_customer_id)
        print_descendants(descendants, root_user, root_customer_id)
