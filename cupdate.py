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

def get_descendants(customer_id, level=1):
    """Recursively find all descendants."""
    descendants = []
    referred_users = list(collection.find({"referenceId": customer_id}))

    for user in referred_users:
        user_id = user.get("customerID")
        if user_id:
            user["level"] = level
            descendants.append(user)
            descendants += get_descendants(user_id, level + 1)

    return descendants

def update_children_ids(customer_id):
    """Set child1 and child2 fields to customerIDs of first two direct referrals."""
    children = list(collection.find({"referenceId": customer_id}))
    child_ids = [child["customerID"] for child in children[:2]]

    update_fields = {
        "child1": child_ids[0] if len(child_ids) > 0 else "",
        "child2": child_ids[1] if len(child_ids) > 1 else ""
    }

    collection.update_one(
        {"customerID": customer_id},
        {"$set": update_fields}
    )

    # Return the updated user
    return collection.find_one({"customerID": customer_id})

if __name__ == "__main__":
    root_customer_id = input("Enter the root customerID: ").strip()

    # Fetch root user
    root_user = collection.find_one({"customerID": root_customer_id})
    if not root_user:
        print(f"No user found with customerID: {root_customer_id}")
    else:
        # Update root's child1 and child2 to customerIDs
        updated_root = update_children_ids(root_customer_id)

        # Get full downline
        downline = get_descendants(root_customer_id)

        print(f"\n✅ Total members in the downline of {root_customer_id}: {len(downline)}\n")
        print(f"- {updated_root.get('customerID')} | {updated_root.get('name')} | {updated_root.get('mobile')} | {updated_root.get('place')} | {updated_root.get('referenceId')} | {updated_root.get('referenceCustomer')} | {updated_root.get('child1')} | {updated_root.get('child2')}")

        # Update and print each downline user
        for member in downline:
            updated_member = update_children_ids(member["customerID"])
            print(f"- {updated_member.get('customerID')} | {updated_member.get('name')} | {updated_member.get('mobile')} | {updated_member.get('place')} | {updated_member.get('referenceId')} | {updated_member.get('referenceCustomer')} | {updated_member.get('child1')} | {updated_member.get('child2')}")
