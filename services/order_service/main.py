from __future__ import annotations

import json
from datetime import datetime, timezone

from confluent_kafka import Producer


def to_json_bytes(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def main() -> int:
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    topic = "orders"

    sample_orders = [
        {
            "event_type": "order_created",
            "order_id": "order_1001",
            "user_id": "user_42",
            "product_id": "product_abc",
            "region": "US",
            "price": 19.99,
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
        {
            "event_type": "order_created",
            "order_id": "order_1002",
            "user_id": "user_7",
            "product_id": "product_xyz",
            "region": "EU",
            "price": 249.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
        {
            "event_type": "order_created",
            "order_id": "order_1003",
            "user_id": "user_99",
            "product_id": "product_123",
            "region": "Africa",
            "price": 5.5,
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
    ]

    def on_delivery(err, msg) -> None:
        order_id = msg.key().decode("utf-8") if msg.key() else "<no-key>"
        if err is not None:
            print(f"{order_id}: delivery failed ({err})")
        else:
            print(
                f"{order_id}: delivered to {msg.topic()} "
                f"[partition={msg.partition()} offset={msg.offset()}]"
            )

    for order in sample_orders:
        order_id = order["order_id"]
        producer.produce(
            topic=topic,
            key=order_id.encode("utf-8"),
            value=to_json_bytes(order),
            on_delivery=on_delivery,
        )
        producer.poll(0)

    producer.flush(10)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

