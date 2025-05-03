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

def generate_invalid_reference_pdf(customers, output_path):
    """Generate a PDF report of customers with blank or invalid reference IDs, including serial numbers."""
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", "B", 14)
    pdf.cell(200, 10, txt="Invalid or Blank Reference ID Report", ln=True, align="C")
    pdf.ln(10)

    # Table headers
    pdf.set_font("Arial", "B", 10)
    pdf.cell(10, 8, "S.No", 1)
    pdf.cell(55, 8, "Name", 1)
    pdf.cell(60, 8, "Customer ID", 1)
    pdf.cell(60, 8, "Reference ID", 1)
    pdf.ln()

    # Table rows
    pdf.set_font("Arial", size=10)
    for idx, user in enumerate(customers, start=1):
        pdf.cell(10, 8, str(idx), 1)
        pdf.cell(55, 8, user.get("name", "N/A"), 1)
        pdf.cell(60, 8, user.get("customerID", "N/A"), 1)
        pdf.cell(60, 8, user.get("referenceId", "N/A"), 1)
        pdf.ln()

    pdf.output(output_path)
    print(f"PDF saved as {output_path}")

if __name__ == "__main__":
    invalid_customers = find_invalid_reference_ids()
    if not invalid_customers:
        print("No invalid or blank reference IDs found.")
    else:
        output_pdf_path = "invalid_reference_ids_report.pdf"
        generate_invalid_reference_pdf(invalid_customers, output_pdf_path)
