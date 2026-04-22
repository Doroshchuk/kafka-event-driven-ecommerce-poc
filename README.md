# Kafka Event-Driven E-commerce POC

Event-driven architecture demo using Kafka, Python, and Docker.

## What this project demonstrates

- event-driven architecture
- Kafka producer / consumer
- consumer groups
- event chains
- idempotency (duplicate handling)
- async processing

## Architecture overview

Event flow:

order_created → inventory_updated → low_inventory → restock_requested

Services:

- **order_service** (producer): emits `order_created` events to the `orders` topic
- **inventory_service** (consumer + producer): consumes `orders`, updates in-memory stock, emits `inventory_updated` to `inventory` (skips duplicate `order_id`s)
- **alert_service** (consumer + producer): consumes `inventory`, emits `low_inventory` to `alerts` when stock is below threshold
- **restock_service** (consumer): consumes `alerts` and prints a “restock requested” action

## Tech stack

- Python
- Apache Kafka (KRaft mode)
- Docker
- confluent-kafka

## How to run

1) Start Docker Desktop

2) Start Kafka + Kafka UI:

```bash
docker compose up -d
```

- Kafka: `localhost:9092`
- Kafka UI: `http://localhost:8080`

3) Create topics:

```bash
python scripts/create_topics.py
```

4) Start the services (separate terminals):

```bash
python services/inventory_service/main.py
python services/alert_service/main.py
python services/restock_service/main.py
```

5) Produce sample orders:

```bash
python services/order_service/main.py
```

## Demo flow

- `order_service` publishes 3 `order_created` events (with unique `order_id`s per run)
- `inventory_service` logs each received order, decrements stock per `product_id`, and publishes `inventory_updated`
- `alert_service` logs inventory updates and publishes `low_inventory` when the threshold is crossed
- `restock_service` logs the low-inventory alert and prints a restock request
- Use Kafka UI (`http://localhost:8080`) to inspect topics and messages (`orders`, `inventory`, `alerts`)

## Notes

- This is a POC / demo project
- State is in-memory (not production ready)
- Idempotency is in-memory only (no persistence across restarts)

