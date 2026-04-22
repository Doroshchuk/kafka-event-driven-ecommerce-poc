from __future__ import annotations

import json
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer


def to_json_bytes(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def main() -> int:
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "inventory-group",
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    orders_topic = "orders"
    inventory_topic = "inventory"

    stock_by_product: dict[str, int] = {}
    processed_order_ids: set[str] = set()

    consumer.subscribe([orders_topic])
    print(f"inventory_service: consuming from {orders_topic}, producing to {inventory_topic}")

    def on_delivery(err, msg) -> None:
        if err is not None:
            print(f"inventory_service: produce failed ({err})")
        else:
            print(
                "inventory_service: produced inventory event "
                f"to {msg.topic()} [partition={msg.partition()} offset={msg.offset()}]"
            )

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"inventory_service: consumer error ({msg.error()})")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"inventory_service: failed to parse JSON ({e})")
                continue

            if payload.get("event_type") != "order_created":
                continue

            order_id = payload.get("order_id")
            product_id = payload.get("product_id")
            if not order_id or not product_id:
                print("inventory_service: missing order_id/product_id; skipping")
                continue

            print(f"inventory_service: received order {order_id} for product {product_id}")

            if order_id in processed_order_ids:
                print(f"inventory_service: skipping duplicate order {order_id}")
                continue

            remaining_stock = stock_by_product.get(product_id, 10) - 1
            stock_by_product[product_id] = remaining_stock
            print(f"inventory_service: updated stock {product_id} -> {remaining_stock}")
            processed_order_ids.add(order_id)

            inventory_event = {
                "event_type": "inventory_updated",
                "order_id": order_id,
                "product_id": product_id,
                "remaining_stock": remaining_stock,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

            producer.produce(
                topic=inventory_topic,
                key=str(product_id).encode("utf-8"),
                value=to_json_bytes(inventory_event),
                on_delivery=on_delivery,
            )
            producer.poll(0)
    except KeyboardInterrupt:
        print("inventory_service: stopping (Ctrl+C)")
    finally:
        producer.flush(10)
        consumer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

