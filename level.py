from pymongo import MongoClient
import os
from dotenv import load_dotenv
from collections import deque

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

def get_all_descendants_flat(customer_id):
    """Fetch all descendant users (unordered) under a given root customer."""
    all_users = []
    visited = set()
    queue = deque([customer_id])

    while queue:
        current = queue.popleft()
        referred = list(collection.find({"referenceId": current}))
        for user in referred:
            user_id = user.get("customerID")
            if user_id and user_id not in visited:
                all_users.append(user)
                visited.add(user_id)
                queue.append(user_id)
    return all_users

def rebuild_pyramid(root_customer_id):
    root_user = collection.find_one({"customerID": root_customer_id})
    if not root_user:
        print(f"No user found with customerID: {root_customer_id}")
        return

    # Fetch all existing descendants
    all_descendants = get_all_descendants_flat(root_customer_id)
    if not all_descendants:
        print("No descendants to reorganize.")
        return

    # Clear child fields in all users
    for user in all_descendants + [root_user]:
        collection.update_one({"customerID": user["customerID"]}, {"$set": {
            "child1": "",
            "child2": ""
        }})

    # Rebuild the tree
    queue = deque([root_user])
    index = 0

    while queue and index < len(all_descendants):
        parent = queue.popleft()
        parent_id = parent["customerID"]
        parent_name = parent["name"]

        # Assign up to 2 children
        children = all_descendants[index:index+2]
        child_ids = [c["customerID"] for c in children]

        # Update parent node with child1 and child2
        update_fields = {
            "child1": child_ids[0] if len(child_ids) > 0 else "",
            "child2": child_ids[1] if len(child_ids) > 1 else ""
        }
        collection.update_one({"customerID": parent_id}, {"$set": update_fields})

        # Update children with new referenceId and referenceCustomer
        for child in children:
            collection.update_one(
                {"customerID": child["customerID"]},
                {"$set": {
                    "referenceId": parent_id,
                    "referenceCustomer": parent_name
                }}
            )
            queue.append(child)

        index += 2

    print("✅ Pyramid restructuring complete.")

if __name__ == "__main__":
    root_id = input("Enter the root customerID: ").strip()
    rebuild_pyramid(root_id)
