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
            "group.id": "alert-group",
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    inventory_topic = "inventory"
    alerts_topic = "alerts"
    threshold = 9

    consumer.subscribe([inventory_topic])
    print(f"alert_service: consuming from {inventory_topic}, producing to {alerts_topic}")

    def on_delivery(err, msg) -> None:
        if err is not None:
            print(f"alert_service: produce failed ({err})")
        else:
            product_id = "<unknown>"
            try:
                v = msg.value()
                if v is not None:
                    obj = json.loads(v.decode("utf-8"))
                    product_id = str(obj.get("product_id", product_id))
            except Exception:
                pass
            print(
                "alert_service: delivered low_inventory for product "
                f"{product_id} to {msg.topic()} "
                f"[partition={msg.partition()} offset={msg.offset()}]"
            )

    try:
        while True:
            producer.poll(0)
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"alert_service: consumer error ({msg.error()})")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"alert_service: failed to parse JSON ({e})")
                continue

            if payload.get("event_type") != "inventory_updated":
                continue

            product_id = payload.get("product_id")
            remaining_stock = payload.get("remaining_stock")
            print(
                f"alert_service: received inventory event product={product_id} "
                f"remaining_stock={remaining_stock}"
            )

            if not product_id or remaining_stock is None:
                print("alert_service: missing product_id/remaining_stock; skipping")
                continue

            if int(remaining_stock) < threshold:
                print(f"alert_service: threshold crossed (< {threshold}) for {product_id}")
                alert_event = {
                    "event_type": "low_inventory",
                    "product_id": product_id,
                    "remaining_stock": int(remaining_stock),
                    "threshold": threshold,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
                producer.produce(
                    topic=alerts_topic,
                    key=str(product_id).encode("utf-8"),
                    value=to_json_bytes(alert_event),
                    on_delivery=on_delivery,
                )
                print(
                    f"alert_service: queued low_inventory for product {product_id}"
                )
            else:
                print(f"alert_service: threshold not crossed for {product_id}")
    except KeyboardInterrupt:
        print("alert_service: stopping (Ctrl+C)")
    finally:
        producer.flush(10)
        consumer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

