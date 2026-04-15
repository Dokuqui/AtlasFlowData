import os
import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = "retail_events"


def create_producer():
    """Initializes the official Confluent Kafka Producer."""
    return Producer({"bootstrap.servers": KAFKA_BROKER})


def generate_event():
    """Simulates a user interacting with the e-commerce website."""
    actions = ["view_item", "add_to_cart", "remove_from_cart", "checkout"]
    return {
        "event_id": f"evt_{random.randint(10000, 99999)}",
        "user_id": random.randint(1, 100),
        "product_id": random.randint(1, 50),
        "action": random.choices(actions, weights=[0.6, 0.2, 0.1, 0.1])[0],
        "timestamp": datetime.utcnow().isoformat(),
    }


def main():
    print(f"Connecting to Kafka Broker at {KAFKA_BROKER}...")
    producer = create_producer()
    print(f"Successfully connected! Streaming events to topic: '{TOPIC_NAME}'...\n")

    try:
        while True:
            event = generate_event()
            event_bytes = json.dumps(event).encode("utf-8")

            producer.produce(TOPIC_NAME, value=event_bytes)
            producer.poll(0)

            print(f"Sent -> {event}")
            time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        print("\nStopped streaming gracefully.")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
