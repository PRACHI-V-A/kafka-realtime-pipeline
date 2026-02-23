from kafka import KafkaProducer
import json
import time
import random
import uuid
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

categories = ["Electronics", "Clothing", "Books", "Grocery"]
payment_methods = ["UPI", "Credit Card", "Debit Card", "Net Banking"]

while True:
    event = {
        "event_id": str(uuid.uuid4()),
        "order_id": random.randint(10000, 99999),
        "user_id": random.randint(100, 999),
        "product_category": random.choice(categories),
        "price": round(random.uniform(100, 20000), 2),
        "quantity": random.randint(1, 3),
        "payment_method": random.choice(payment_methods),
        "order_status": "SUCCESS",
        "event_timestamp": datetime.utcnow().isoformat()
    }

    producer.send("ecommerce.orders.events", event)
    print("Sent:", event)

    time.sleep(0.1)



