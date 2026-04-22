from __future__ import annotations

import sys

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


def main() -> int:
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})

    topics_to_create = [
        NewTopic("orders", num_partitions=3, replication_factor=1),
        NewTopic("inventory", num_partitions=1, replication_factor=1),
        NewTopic("alerts", num_partitions=1, replication_factor=1),
    ]

    # Fast pre-check so we can print "already exists" clearly.
    try:
        metadata = admin.list_topics(timeout=10)
        existing = set(metadata.topics.keys())
    except KafkaException as e:
        print(f"failed: could not fetch topic metadata: {e}", file=sys.stderr)
        return 1

    pending = [t for t in topics_to_create if t.topic not in existing]
    for t in topics_to_create:
        if t.topic in existing:
            print(f"{t.topic}: already exists")

    if not pending:
        return 0

    futures = admin.create_topics(pending, request_timeout=15)
    for topic, fut in futures.items():
        try:
            fut.result()
            print(f"{topic}: created")
        except KafkaException as e:
            # If a topic got created concurrently, treat as idempotent success.
            if "TOPIC_ALREADY_EXISTS" in str(e):
                print(f"{topic}: already exists")
            else:
                print(f"{topic}: failed ({e})", file=sys.stderr)
                return 1
        except Exception as e:  # pragma: no cover
            print(f"{topic}: failed ({e})", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

