import json
import os
import random
from datetime import datetime

def generate_mock_data(base_dir):
    os.makedirs(base_dir, exist_ok=True)
    
    # 1. Mock Business Data
    businesses = []
    categories = ["Restaurants", "Food", "Nightlife", "Bar", "Travel", "Hotels"]
    cities = ["San Francisco", "New York", "Austin", "Portland"]
    
    for i in range(1, 21): # 20 businesses
        businesses.append({
            "business_id": f"b_{i}",
            "name": f"Mock Business {i}",
            "address": f"{i} Mock St",
            "city": random.choice(cities),
            "state": "CA",
            "postal_code": "90001",
            "latitude": 37.77 + random.uniform(-0.1, 0.1),
            "longitude": -122.41 + random.uniform(-0.1, 0.1),
            "stars": random.choice([3.0, 3.5, 4.0, 4.5, 5.0]),
            "review_count": random.randint(10, 500),
            "is_open": 1,
            "attributes": {
                "WiFi": random.choice(["'Free'", "'No'", "'Paid'"]),
                "BusinessParking": "{'garage': False, 'street': True}",
                "RestaurantsPriceRange2": str(random.randint(1, 4)),
                "OutdoorSeating": random.choice(["True", "False"])
            },
            "categories": ", ".join(random.sample(categories, k=random.randint(1, 3))),
            "hours": {"Monday": "10:0-22:0", "Tuesday": "10:0-22:0"}
        })
    
    with open(os.path.join(base_dir, "yelp_academic_dataset_business.json"), 'w') as f:
        for b in businesses:
            f.write(json.dumps(b) + "\n")

    # 2. Mock User Data
    users = []
    for i in range(1, 11): # 10 users
        users.append({
            "user_id": f"u_{i}",
            "name": f"User {i}",
            "review_count": random.randint(5, 50),
            "yelping_since": "2020-01-01 00:00:00",
            "useful": random.randint(0, 100),
            "funny": random.randint(0, 100),
            "cool": random.randint(0, 100),
            "elite": "2020,2021" if random.random() > 0.8 else "",
            "friends": "u_2, u_3, u_4",
            "fans": random.randint(0, 10),
            "average_stars": random.uniform(3.0, 5.0)
        })
        
    with open(os.path.join(base_dir, "yelp_academic_dataset_user.json"), 'w') as f:
        for u in users:
            f.write(json.dumps(u) + "\n")

    # 3. Mock Review Data
    reviews = []
    for i in range(1, 101): # 100 reviews
        reviews.append({
            "review_id": f"r_{i}",
            "user_id": f"u_{random.randint(1, 10)}",
            "business_id": f"b_{random.randint(1, 20)}",
            "stars": random.randint(1, 5),
            "useful": random.randint(0, 5),
            "funny": 0,
            "cool": 0,
            "text": "This is a mock review text. " + random.choice(["Great place!", "Terrible service.", "Okay food."]),
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    with open(os.path.join(base_dir, "yelp_academic_dataset_review.json"), 'w') as f:
        for r in reviews:
            f.write(json.dumps(r) + "\n")

    print(f"Mock data generated in {base_dir}")

if __name__ == "__main__":
    # Get absolute path to data/bronze
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    bronze_dir = os.path.join(project_root, "data", "bronze")
    generate_mock_data(bronze_dir)
