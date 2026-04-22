from __future__ import annotations

import json

from confluent_kafka import Consumer


def main() -> int:
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "restock-group",
            "auto.offset.reset": "earliest",
        }
    )

    alerts_topic = "alerts"
    consumer.subscribe([alerts_topic])
    print(f"restock_service: consuming from {alerts_topic}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"restock_service: consumer error ({msg.error()})")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"restock_service: failed to parse JSON ({e})")
                continue

            if payload.get("event_type") != "low_inventory":
                continue

            product_id = payload.get("product_id")
            print(f"restock_service: received low inventory alert for product {product_id}")
            print(f"restock_service: restock requested for product {product_id}")
    except KeyboardInterrupt:
        print("restock_service: stopping (Ctrl+C)")
    finally:
        consumer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

