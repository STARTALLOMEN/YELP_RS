import os

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
data_dir = os.path.join(root, "data")
bronze_business = os.path.join(data_dir, "bronze_delta", "business")

print(f"Root: {root}")
print(f"Checking: {bronze_business}")

if os.path.exists(bronze_business):
    print("Directory Exists.")
    print("Contents:", os.listdir(bronze_business))
else:
    print("Directory DOES NOT exist.")
