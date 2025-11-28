import requests
import time
import random
from faker import Faker

fake = Faker()

API_URL = "http://localhost:8000/orders/" 

# Số lượng message muốn gửi
NUM_MESSAGES = 50

for _ in range(NUM_MESSAGES):
    data = {
        "product": fake.word(), 
        "quantity": random.randint(1, 5) 
    }

    try:
        response = requests.post(API_URL, json=data)
        print(response.status_code, response.json())
    except Exception as e:
        print("Error:", e)
    
    # delay ngẫu nhiên 1-3 giây
    time.sleep(random.uniform(1, 3))
