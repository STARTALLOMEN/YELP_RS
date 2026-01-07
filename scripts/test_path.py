import os
from dotenv import load_dotenv

load_dotenv()
path = os.getenv("BRONZE_SOURCE_PATH")
print(f"Checking path: {path}")

if os.path.exists(path):
    print("Directory exists.")
    file_path = os.path.join(path, "yelp_academic_dataset_business.json")
    if os.path.exists(file_path):
        print(f"File found: {file_path}")
        print(f"Size: {os.path.getsize(file_path)} bytes")
    else:
        print(f"File NOT found: {file_path}")
else:
    print("Directory does NOT exist.")
