# Kafka Event-Driven E-commerce POC

Event-driven architecture demo using Kafka, Python, and Docker.

## What it demonstrates

- producer/consumer
- consumer groups
- event chains
- replay
- stream processing

## Event Flow

This project demonstrates a simple event-driven architecture using Kafka, where one event triggers a chain of independent services.

orders (order_created) → inventory (inventory_updated) → alerts (low_inventory)

## Local setup

- Start: `docker compose up -d`
- Kafka: `localhost:9092`
- Kafka UI: `http://localhost:8080`

