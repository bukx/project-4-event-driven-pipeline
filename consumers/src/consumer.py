"""
Kafka Consumer — Processes events and writes to PostgreSQL in batches.
Runs on Kubernetes with HPA based on consumer lag.
"""
import os
import json
import time
import logging
import psycopg2
from psycopg2.extras import execute_batch
from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, Histogram, Gauge, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
logger = logging.getLogger("kafka-consumer")

EVENTS_CONSUMED = Counter("consumer_events_total", "Events consumed", ["topic", "event_type"])
CONSUME_LATENCY = Histogram(
    "consumer_process_seconds",
    "Per-event processing time",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)
BATCH_SIZE = Histogram(
    "consumer_batch_size",
    "Events per batch",
    buckets=[1, 5, 10, 25, 50, 100],
)
CONSUMER_LAG = Gauge("consumer_lag_messages", "Consumer lag", ["partition"])
DB_WRITE_DURATION = Histogram(
    "consumer_db_write_seconds",
    "DB write time",
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0],
)
ERRORS = Counter("consumer_errors_total", "Processing errors", ["error_type"])


class PostgresWriter:
    INSERT_SQL = (
        "INSERT INTO events (event_id, event_type, timestamp, user_id, region, payload) "
        "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING"
    )

    def __init__(self, dsn: str, batch_size: int = 50):
        self.batch_size = batch_size
        self.buffer = []
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = False
        self._ensure_table()

    def _ensure_table(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    event_id UUID PRIMARY KEY,
                    event_type VARCHAR(50) NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    user_id VARCHAR(20) NOT NULL,
                    region VARCHAR(20),
                    payload JSONB NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
                CREATE INDEX IF NOT EXISTS idx_events_ts ON events(timestamp);
            """)
            self.conn.commit()

    def add(self, event: dict):
        self.buffer.append((
            event["event_id"],
            event["event_type"],
            event["timestamp"],
            event["user_id"],
            event.get("region", "unknown"),
            json.dumps(event),
        ))
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.buffer:
            return
        start = time.perf_counter()
        try:
            with self.conn.cursor() as cur:
                execute_batch(cur, self.INSERT_SQL, self.buffer, page_size=100)
            self.conn.commit()
            DB_WRITE_DURATION.observe(time.perf_counter() - start)
            BATCH_SIZE.observe(len(self.buffer))
            logger.info(f"Flushed {len(self.buffer)} events to PostgreSQL")
        except Exception as e:
            self.conn.rollback()
            ERRORS.labels("db_write").inc()
            logger.error(f"DB write failed: {e}")
        finally:
            self.buffer.clear()


def run_consumer():
    broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "events")
    group_id = os.getenv("CONSUMER_GROUP", "event-processor")
    db_dsn = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/events")

    consumer = Consumer({
        "bootstrap.servers": broker,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    writer = PostgresWriter(db_dsn, batch_size=50)
    logger.info(f"Consumer started: broker={broker} topic={topic} group={group_id}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                writer.flush()
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                ERRORS.labels("kafka").inc()
                continue

            start = time.perf_counter()
            try:
                event = json.loads(msg.value().decode("utf-8"))
                EVENTS_CONSUMED.labels(topic, event.get("event_type", "unknown")).inc()
                writer.add(event)
                consumer.commit(asynchronous=False)
            except json.JSONDecodeError:
                ERRORS.labels("json_parse").inc()
            except Exception as e:
                ERRORS.labels("processing").inc()
                logger.error(f"Error: {e}")
            CONSUME_LATENCY.observe(time.perf_counter() - start)
    except KeyboardInterrupt:
        logger.info("Shutting down")
    finally:
        writer.flush()
        consumer.close()


if __name__ == "__main__":
    start_http_server(int(os.getenv("METRICS_PORT", "8002")))
    run_consumer()
