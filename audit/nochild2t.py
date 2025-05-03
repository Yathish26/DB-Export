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

def get_network(customer_id):
    """Recursively collect the downline network starting from customer_id."""
    network = []

    def dfs(cid):
        user = collection.find_one({"customerID": cid})
        if not user:
            return
        network.append(user)
        for child_key in ["child1", "child2"]:
            child_id = user.get(child_key)
            if child_id:
                dfs(child_id)

    dfs(customer_id)
    return network

def find_leaf_customer_ids(network):
    """Return customerIDs of users with no child1 and child2."""
    return [
        user.get("customerID")
        for user in network
        if not user.get("child1") and not user.get("child2")
    ]

if __name__ == "__main__":
    root_id = input("Enter the customerID to start from: ").strip()
    full_network = get_network(root_id)

    if not full_network:
        print("No network found for this ID.")
    else:
        leaf_ids = find_leaf_customer_ids(full_network)
        if not leaf_ids:
            print("No leaf nodes found — all users have children.")
        else:
            print("\nCustomerIDs with no children (leaf nodes):")
            for cid in leaf_ids:
                print(cid)
