"""
Kafka Producer — Simulates IoT / clickstream events at configurable throughput.
Usage: python producer.py --topic events --rate 500 --broker kafka:9092
"""
import os, json, time, uuid, random, argparse, logging
from datetime import datetime, timezone
from confluent_kafka import Producer
from prometheus_client import Counter, Histogram, start_http_server

logging.basicConfig(level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}')
logger = logging.getLogger("kafka-producer")

EVENTS_PRODUCED = Counter("producer_events_total", "Events produced", ["topic", "event_type"])
PRODUCE_LATENCY = Histogram("producer_send_seconds", "Enqueue time",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1])
PRODUCE_ERRORS = Counter("producer_errors_total", "Failed produces", ["topic"])

EVENT_TYPES = ["page_view", "click", "add_to_cart", "purchase", "sensor_reading"]
PAGES = ["/home", "/products", "/checkout", "/about", "/pricing"]
DEVICES = ["web", "mobile-ios", "mobile-android", "iot-sensor", "tablet"]
REGIONS = ["us-east", "us-west", "eu-west", "ap-south", "sa-east"]


def generate_event() -> dict:
    event_type = random.choices(EVENT_TYPES, weights=[40, 25, 15, 10, 10], k=1)[0]
    event = {
        "event_id": str(uuid.uuid4()), "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": f"user-{random.randint(1, 10000):05d}",
        "session_id": str(uuid.uuid4())[:8],
        "device": random.choice(DEVICES), "region": random.choice(REGIONS),
    }
    if event_type in ("page_view", "click"):
        event["page"] = random.choice(PAGES)
        event["duration_ms"] = random.randint(100, 30000)
    elif event_type == "add_to_cart":
        event["product_id"] = f"SKU-{random.randint(1000, 9999)}"
        event["quantity"] = random.randint(1, 5)
        event["price_cents"] = random.randint(499, 49999)
    elif event_type == "purchase":
        event["order_id"] = f"ORD-{random.randint(100000, 999999)}"
        event["total_cents"] = random.randint(999, 199999)
    elif event_type == "sensor_reading":
        event["sensor_id"] = f"sensor-{random.randint(1, 200):03d}"
        event["temperature_c"] = round(random.uniform(-10, 45), 2)
        event["humidity_pct"] = round(random.uniform(20, 95), 1)
    return event


def delivery_callback(err, msg):
    if err:
        PRODUCE_ERRORS.labels(msg.topic()).inc()
        logger.error(f"Delivery failed: {err}")
    else:
        EVENTS_PRODUCED.labels(msg.topic(), "delivered").inc()


def run_producer(broker: str, topic: str, rate: int):
    producer = Producer({
        "bootstrap.servers": broker,
        "client.id": f"producer-{uuid.uuid4().hex[:8]}",
        "acks": "all", "retries": 3, "linger.ms": 10,
        "batch.size": 16384, "compression.type": "lz4",
    })
    interval = 1.0 / rate if rate > 0 else 0.01
    total_sent = 0
    logger.info(f"Starting producer: broker={broker} topic={topic} rate={rate}/s")

    try:
        while True:
            event = generate_event()
            start = time.perf_counter()
            producer.produce(topic=topic, key=event["user_id"].encode(),
                           value=json.dumps(event).encode(), callback=delivery_callback)
            PRODUCE_LATENCY.observe(time.perf_counter() - start)
            total_sent += 1
            if total_sent % 1000 == 0:
                logger.info(f"Produced {total_sent} events")
                producer.flush(timeout=5)
            producer.poll(0)
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info(f"Shutting down. Total: {total_sent}")
    finally:
        producer.flush(timeout=30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "events"))
    parser.add_argument("--rate", type=int, default=int(os.getenv("PRODUCE_RATE", "100")))
    parser.add_argument("--metrics-port", type=int, default=8001)
    args = parser.parse_args()
    start_http_server(args.metrics_port)
    run_producer(args.broker, args.topic, args.rate)
