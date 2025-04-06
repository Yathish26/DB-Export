import json
from fpdf import FPDF

def load_json(file_path):
    """Load JSON data from a file."""
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)

def find_descendants(customer_id, data):
    """Find all child and grandchild users under a given customer ID."""
    descendants = []
    queue = [customer_id]  # Start with the given customer ID

    while queue:
        current_id = queue.pop(0)
        for user in data:
            if user.get("referenceId") == current_id:
                descendants.append(user)
                queue.append(user.get("customerID"))  # Add child to the queue
    
    return descendants

def generate_pdf(descendants, output_path):
    """Generate a PDF report from the descendants data."""
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    
    root_user = next((user for user in data if user.get("customerID") == root_customer_id), {})
    root_name = root_user.get("name", "Unknown")


    # Add Title
    pdf.cell(200, 10, txt=f"Downline Members of {root_name}", ln=True, align="C")
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
    json_file_path = "CData.json"  # Replace with your JSON file path
    output_pdf_path = "downline.pdf"  # Output file
    root_customer_id = "SS2250012213"  # Change as needed

    data = load_json(json_file_path)
    descendants = find_descendants(root_customer_id, data)
    generate_pdf(descendants, output_pdf_path)
