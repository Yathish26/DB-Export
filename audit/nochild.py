from pymongo import MongoClient
import os
from dotenv import load_dotenv
from fpdf import FPDF

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

def find_customers_with_missing_children(network):
    """Filter users missing one or both children."""
    return [
        user for user in network
        if not user.get("child1") or not user.get("child2")
    ]

def generate_missing_children_pdf(customers, output_path, root_id):
    """Generate a PDF report of customers with missing children."""
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", "B", 14)
    pdf.cell(200, 10, txt=f"Missing Children Report (Under {root_id})", ln=True, align="C")
    pdf.ln(10)

    # Table headers
    pdf.set_font("Arial", "B", 10)
    pdf.cell(10, 8, "S.No", 1)
    pdf.cell(50, 8, "Name", 1)
    pdf.cell(50, 8, "Customer ID", 1)
    pdf.cell(40, 8, "Child1", 1)
    pdf.cell(40, 8, "Child2", 1)
    pdf.ln()

    # Table rows
    pdf.set_font("Arial", size=10)
    for idx, user in enumerate(customers, start=1):
        pdf.cell(10, 8, str(idx), 1)
        pdf.cell(50, 8, user.get("name", "N/A"), 1)
        pdf.cell(50, 8, user.get("customerID", "N/A"), 1)
        pdf.cell(40, 8, user.get("child1", "None"), 1)
        pdf.cell(40, 8, user.get("child2", "None"), 1)
        pdf.ln()

    pdf.output(output_path)
    print(f"PDF saved as {output_path}")

if __name__ == "__main__":
    root_id = input("Enter the customerID to search network from: ").strip()
    full_network = get_network(root_id)
    if not full_network:
        print("No network found for this ID.")
    else:
        incomplete_customers = find_customers_with_missing_children(full_network)
        if not incomplete_customers:
            print("All users in the network have both children.")
        else:
            output_path = f"missing_children_report_{root_id}.pdf"
            generate_missing_children_pdf(incomplete_customers, output_path, root_id)
