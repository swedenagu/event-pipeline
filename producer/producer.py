from confluent_kafka import Producer
import json, uuid, random
from datetime import datetime

TOPIC = 'user-events'

conf = {
    'bootstrap.servers': 'kafka:29092',
    'broker.address.family': 'v4',
    'batch.size': 65536,
    'linger.ms': 10,
    'compression.type': 'lz4',
}
producer = Producer(conf)

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

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed: {err}')

def produce_events():
    count = 0
    try:
        while True:
            event = generate_event()
            producer.produce(
                TOPIC,
                key=event["event_id"],
                value=json.dumps(event),
                callback=delivery_report
            )
            producer.poll(0)  # triggers delivery callbacks for prior sends — non-blocking
            count += 1
            if count % 10000 == 0:
                print(f"Produced {count} events")
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()

if __name__ == '__main__':
    produce_events()
