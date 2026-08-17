from kafka import KafkaProducer
import avro.schema, json, time, uuid
from datetime import datetime
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    batch_size=65536,
    linger_ms=10,
    compression_type='lz4'
)

def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 100000)}",
        "event_type": random.choice(["click", "purchase", "add_to_cart"]),
        "product_id": f"prod_{random.randint(1, 5000)}",
        "price": round(random.uniform(5.0, 500.0), 2),
        "timestamp": int(datetime.now().timestamp() * 1000),
        "category": random.choice(["electronics", "clothing", "home"])
    }
