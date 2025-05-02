from pymongo import MongoClient
import os
from dotenv import load_dotenv
from fpdf import FPDF

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

def find_duplicate_names_and_numbers():
    """Find name and mobile number pairs that are shared by multiple customer IDs."""
    pipeline = [
        {"$group": {
            "_id": {"name": "$name", "mobile": "$mobile"},
            "count": {"$sum": 1},
            "users": {
                "$push": {
                    "customerID": "$customerID"
                }
            }
        }},
        {"$match": {"count": {"$gt": 1}}}
    ]
    return list(collection.aggregate(pipeline))

def generate_name_and_number_duplicate_pdf(duplicates, output_path):
    """Generate a PDF showing duplicate name and mobile number pairs with associated customer IDs."""
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", "B", 14)
    pdf.cell(200, 10, txt="Duplicate Name and Mobile Number Report", ln=True, align="C")
    pdf.ln(10)

    pdf.set_font("Arial", size=10)

    for entry in duplicates:
        name = entry["_id"]["name"]
        mobile = entry["_id"]["mobile"]
        users = entry["users"]

        # Header per name and mobile number
        pdf.set_font("Arial", "B", 12)
        pdf.cell(200, 10, txt=f"Name: {name}, Mobile: {mobile}", ln=True)
        pdf.set_font("Arial", "B", 10)
        pdf.cell(100, 8, "Customer ID", 1)
        pdf.ln()

        # List users with the same name and mobile number
        pdf.set_font("Arial", size=10)
        for user in users:
            pdf.cell(100, 8, user.get("customerID", "N/A"), 1)
            pdf.ln()

        pdf.ln(5)  # space between groups

    pdf.output(output_path)
    print(f"PDF saved as {output_path}")

if __name__ == "__main__":
    duplicates = find_duplicate_names_and_numbers()
    if not duplicates:
        print("No duplicate name and mobile number pairs found.")
    else:
        output_pdf_path = "duplicate_name_number_report.pdf"
        generate_name_and_number_duplicate_pdf(duplicates, output_pdf_path)
