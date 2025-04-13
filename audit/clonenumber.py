from pymongo import MongoClient
import os
from dotenv import load_dotenv
from fpdf import FPDF
from collections import defaultdict

# Load environment variables
load_dotenv()

# MongoDB Connection
mongo_uri = os.getenv("MONGO_URI")
database_name = "Customers"
collection_name = "CData"  # Make sure this is correct

try:
    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]
except Exception as e:
    print("Database connection error:", str(e))
    exit()

def find_duplicate_mobiles():
    """Find mobile numbers that are shared by multiple customer IDs."""
    pipeline = [
        {"$group": {
            "_id": "$mobile",
            "count": {"$sum": 1},
            "users": {
                "$push": {
                    "name": "$name",
                    "customerID": "$customerID"
                }
            }
        }},
        {"$match": {"count": {"$gt": 1}}}
    ]
    return list(collection.aggregate(pipeline))

def generate_mobile_duplicate_pdf(duplicates, output_path):
    """Generate a PDF showing duplicate mobile numbers with associated names and IDs."""
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", "B", 14)
    pdf.cell(200, 10, txt="Duplicate Mobile Number Report", ln=True, align="C")
    pdf.ln(10)

    pdf.set_font("Arial", size=10)

    for entry in duplicates:
        mobile = entry["_id"]
        users = entry["users"]

        # Header per mobile number
        pdf.set_font("Arial", "B", 12)
        pdf.cell(200, 10, txt=f"Mobile Number: {mobile}", ln=True)
        pdf.set_font("Arial", "B", 10)
        pdf.cell(60, 8, "Name", 1)
        pdf.cell(60, 8, "Customer ID", 1)
        pdf.ln()

        # List users under the same mobile
        pdf.set_font("Arial", size=10)
        for user in users:
            pdf.cell(60, 8, user.get("name", "N/A"), 1)
            pdf.cell(60, 8, user.get("customerID", "N/A"), 1)
            pdf.ln()

        pdf.ln(5)  # space between groups

    pdf.output(output_path)
    print(f"PDF saved as {output_path}")

if __name__ == "__main__":
    duplicates = find_duplicate_mobiles()
    if not duplicates:
        print("No duplicate mobile numbers found.")
    else:
        output_pdf_path = "duplicate_mobiles_report.pdf"
        generate_mobile_duplicate_pdf(duplicates, output_pdf_path)
