import json

# Load the JSON file
file_path = "CData.json"  # Update with actual path
with open(file_path, "r") as file:
    data = json.load(file)

# Find faulty mobile numbers that are not exactly 10 digits
faulty_mobiles = [
    {"name": entry["name"], "customerID": entry["customerID"],"mobile": entry["mobile"]}
    for entry in data
    if not entry["mobile"].isdigit() or len(entry["mobile"]) != 10
]

# Save the faulty mobile numbers to a new JSON file
output_file_path = "FaultyMobiles.json"  # Update with desired path
with open(output_file_path, "w") as output_file:
    json.dump(faulty_mobiles, output_file, indent=4)

print(f"Faulty mobile numbers saved to {output_file_path}")
