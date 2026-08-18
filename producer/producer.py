import six
import sys
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaProducer
import json, uuid, random
from datetime import datetime

TOPIC = 'user-events'

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None,
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

def produce_events():
    count = 0
    try:
        while True:
            event = generate_event()
            producer.send(TOPIC, key=event["event_id"], value=event)
            count += 1
            if count % 10000 == 0:
                print(f"Produced {count} events")
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()

if __name__ == '__main__':
    produce_events()
