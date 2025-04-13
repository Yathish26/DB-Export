import json
import requests
import time 

URL = "http://localhost:5000/api/mlm/addtonetworknoepin"
JSON_FILE = "data.json"

def send_post_requests():
    try:
        with open(JSON_FILE, 'r') as file:
            data = json.load(file)

            if not isinstance(data, list):
                print("The JSON data must be a list of objects.")
                return

            for i, entry in enumerate(data, 1):
                response = requests.post(URL, json=entry)
                print(f"[{i}] Sent: {entry['name']} -> Status: {response.status_code}")
                print("Response:", response.json())
                time.sleep(0.5)  # optional: 500ms delay between requests

    except FileNotFoundError:
        print(f"File '{JSON_FILE}' not found.")
    except json.JSONDecodeError:
        print("Invalid JSON format in the file.")
    except Exception as e:
        print("An error occurred:", str(e))

if __name__ == "__main__":
    send_post_requests()
