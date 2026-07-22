#!/usr/bin/env python3
"""
Fake Tweet Producer
-------------------
Simulates a Twitter stream by sending random tweets to the Kafka topic
'twitter-topic' every 1-2 seconds.

Requirements:
    pip install kafka-python
"""

import time
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Fake tweets ──────────────────────────────────────────────────────────────
FAKE_TWEETS = [
    # Positive
    "I love AI and machine learning so much!",
    "This is an awesome day, feeling great!",
    "Spark is fantastic for big data processing",
    "I am so happy with the new Python release",
    "This coffee is wonderful, best morning ever",
    "What an amazing concert last night, loved it!",
    "The new phone is great, excellent camera quality",
    "Feeling happy and positive today, good vibes only",
    "Just got a promotion! This is wonderful news",
    "The food here is excellent, absolutely love this place",

    # Negative
    "I hate traffic jams so much, this is the worst",
    "This laptop is terrible, worst purchase I ever made",
    "Feeling really sad and tired today",
    "The customer service was awful and horrible",
    "I hate when apps crash with no warning",
    "This is the worst movie I have ever seen",
    "Bad weather ruined our entire trip, so sad",
    "Disgusting food quality, would not recommend",
    "The network is down again, this is terrible",
    "I hate Mondays, such a bad start to the week",

    # Neutral
    "Just read an interesting article about space",
    "Going to the gym later today",
    "Watched a documentary about oceans",
    "Trying out a new recipe tonight",
    "The meeting is rescheduled to Thursday",
]

TOPIC = "twitter-topic"
BOOTSTRAP_SERVERS = "localhost:9092"


def create_producer() -> KafkaProducer:
    """Create and return a KafkaProducer, retrying until Kafka is ready."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: v.encode("utf-8"),
            )
            print(f"[✓] Connected to Kafka at {BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print("[!] Kafka not reachable yet – retrying in 3 s …")
            time.sleep(3)


def main():
    print("=" * 60)
    print("  Fake Tweet Producer → Kafka")
    print(f"  Topic : {TOPIC}")
    print(f"  Broker: {BOOTSTRAP_SERVERS}")
    print("=" * 60)

    producer = create_producer()
    sent = 0

    try:
        while True:
            tweet = random.choice(FAKE_TWEETS)
            producer.send(TOPIC, value=tweet)
            producer.flush()
            sent += 1
            print(f"[{sent:>4}] Sent → {tweet}")
            time.sleep(random.uniform(0.8, 2.0))   # simulate human posting speed

    except KeyboardInterrupt:
        print(f"\n[✓] Producer stopped. Total tweets sent: {sent}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
