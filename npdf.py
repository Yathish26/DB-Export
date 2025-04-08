from pymongo import MongoClient
import os
from dotenv import load_dotenv
from fpdf import FPDF

# Load environment variables
load_dotenv()

# MongoDB Connection
mongo_uri = os.getenv("MONGO_URI")
database_name = "Customers"
collection_name = "CData"  # Change this to the correct collection name

try:
    # Connect to MongoDB
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

def generate_pdf(descendants, root_user, output_path):
    """Generate a PDF report from the descendants data."""
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", size=12)

    root_name = root_user.get("name", "Unknown")

    # Add Title
    pdf.cell(200, 10, txt=f"Downline Members of {root_name}", ln=True, align="C")
    pdf.cell(200, 10, txt=f"Total Members: {len(descendants)}", ln=True, align="C")
    
    pdf.ln(10)

    # Table Header
    pdf.set_font("Arial", "B", 10)
    pdf.cell(40, 10, "Customer ID", 1)
    pdf.cell(50, 10, "Name", 1)
    pdf.cell(40, 10, "Mobile Number", 1)
    pdf.cell(40, 10, "Place", 1)
    pdf.ln()

    # Table Data
    pdf.set_font("Arial", size=10)
    for user in descendants:
        pdf.cell(40, 10, user.get("customerID", "N/A"), 1)
        pdf.cell(50, 10, user.get("name", "N/A"), 1)
        pdf.cell(40, 10, user.get("mobile", "N/A"), 1)
        pdf.cell(40, 10, user.get("place", "N/A"), 1)
        pdf.ln()

    # Save PDF
    pdf.output(output_path)
    print(f"PDF saved as {output_path}")

if __name__ == "__main__":
    root_customer_id = "SS1587977060"  # Change this as needed

    # Fetch root user details
    root_user = collection.find_one({"customerID": root_customer_id})
    
    if not root_user:
        print(f"No user found with customer ID: {root_customer_id}")
    else:
        descendants = find_descendants(root_customer_id)
        output_pdf_path = f"downline_{root_customer_id}.pdf"
        generate_pdf(descendants, root_user, output_pdf_path)
