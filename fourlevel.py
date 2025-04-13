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

def assign_level_branch(root, users_queue, max_levels=4):
    """
    Assign up to 'max_levels' deep tree from a root.
    Remaining users go into next branch.
    """
    level = 0
    queue = deque([(root, 0)])  # (user, level)
    branch_nodes = {0: [root]}  # track nodes by level

    while queue and users_queue:
        current_user, current_level = queue.popleft()
        if current_level >= max_levels:
            continue

        children = []
        for _ in range(2):  # max 2 children
            if not users_queue:
                break
            child = users_queue.popleft()
            child_id = child["customerID"]
            collection.update_one(
                {"customerID": current_user["customerID"]},
                {"$set": {
                    "child1" if len(children) == 0 else "child2": child_id
                }}
            )
            collection.update_one(
                {"customerID": child_id},
                {"$set": {
                    "referenceId": current_user["customerID"],
                    "referenceCustomer": current_user["name"],
                    "child1": "",
                    "child2": ""
                }}
            )
            children.append(child)
            queue.append((child, current_level + 1))
            branch_nodes.setdefault(current_level + 1, []).append(child)

        level = max(level, current_level + 1)

    # Return the first person in the last level for next branch
    return branch_nodes.get(level, [None])[0]

def rebuild_pyramid_limited_levels(root_customer_id, max_levels=4):
    root_user = collection.find_one({"customerID": root_customer_id})
    if not root_user:
        print(f"No user found with customerID: {root_customer_id}")
        return

    all_descendants = get_all_descendants_flat(root_customer_id)
    if not all_descendants:
        print("No descendants to reorganize.")
        return

    # Clear existing tree
    for user in all_descendants + [root_user]:
        collection.update_one({"customerID": user["customerID"]}, {
            "$set": {
                "child1": "",
                "child2": "",
                "referenceId": "",
                "referenceCustomer": ""
            }
        })

    users_queue = deque(all_descendants)
    current_root = root_user

    while users_queue:
        current_root = assign_level_branch(current_root, users_queue, max_levels)

    print("🌲 Limited-level pyramid built successfully.")

if __name__ == "__main__":
    root_id = input("Enter the root customerID: ").strip()
    rebuild_pyramid_limited_levels(root_id, max_levels=4)
