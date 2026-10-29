"""Consume retail events with independent sales and inventory consumer groups."""

import argparse
import json
from collections import Counter, defaultdict

from confluent_kafka import Consumer, KafkaError

from streaming.settings import BOOTSTRAP_SERVERS, INVENTORY_TOPIC, SALES_TOPIC

GROUPS = {
    "sales": ("retail-sales-processor-v1", SALES_TOPIC),
    "inventory": ("retail-inventory-processor-v1", INVENTORY_TOPIC),
}


class SalesProcessor:
    """Validate sale-line events and maintain a process-local sales summary."""

    def __init__(self) -> None:
        self.count = 0
        self.gross_sales = 0.0
        self.by_store = Counter()

    def process(self, event: dict) -> None:
        if event.get("event_type") != "SaleLine":
            raise ValueError(f"Unexpected sales event: {event.get('event_type')}")
        payload = event["payload"]
        amount = float(payload["line_total"])
        if amount < 0:
            raise ValueError("Sale line total cannot be negative")
        self.count += 1
        self.gross_sales += amount
        self.by_store[payload["store_id"]] += amount

    def report(self) -> dict:
        return {
            "processor": "sales",
            "sale_lines": self.count,
            "gross_sales": round(self.gross_sales, 2),
            "gross_sales_by_store": {
                store_id: round(total, 2)
                for store_id, total in sorted(self.by_store.items())
            },
        }


class InventoryProcessor:
    """Apply signed stock movements to process-local store/product balances."""

    def __init__(self) -> None:
        self.count = 0
        self.by_type = Counter()
        self.on_hand = defaultdict(int)

    def process(self, event: dict) -> None:
        if event.get("event_type") != "InventoryMovement":
            raise ValueError(
                f"Unexpected inventory event: {event.get('event_type')}"
            )
        payload = event["payload"]
        key = (payload["store_id"], payload["product_id"])
        quantity = int(payload["quantity"])
        self.count += 1
        self.by_type[payload["movement_type"]] += 1
        self.on_hand[key] += quantity

    def report(self) -> dict:
        return {
            "processor": "inventory",
            "movements": self.count,
            "movement_types": dict(sorted(self.by_type.items())),
            "on_hand_by_store_product": [
                {
                    "store_id": store_id,
                    "product_id": product_id,
                    "quantity": quantity,
                }
                for (store_id, product_id), quantity in sorted(self.on_hand.items())
            ],
        }


def run_consumer(
    stream: str,
    bootstrap_servers: str = BOOTSTRAP_SERVERS,
    max_messages: int | None = None,
    report_every: int = 100,
) -> None:
    """Run one processor using a stable Kafka consumer group."""
    group_id, topic = GROUPS[stream]
    processor = SalesProcessor() if stream == "sales" else InventoryProcessor()
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumed = 0
    consumer.subscribe([topic])
    print(f"[{stream}] group={group_id} topic={topic}; press Ctrl+C to stop")

    try:
        while max_messages is None or consumed < max_messages:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Kafka consumer error: {message.error()}")

            event = json.loads(message.value().decode("utf-8"))
            if event.get("schema_version") != 1:
                raise ValueError(
                    f"Unsupported event schema version: {event.get('schema_version')}"
                )
            processor.process(event)
            consumer.commit(message=message, asynchronous=False)
            consumed += 1
            if consumed % report_every == 0:
                print(json.dumps(processor.report(), ensure_ascii=False))
    except KeyboardInterrupt:
        print(f"\n[{stream}] stopping on user request")
    finally:
        consumer.close()
        print(json.dumps(processor.report(), ensure_ascii=False))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stream", choices=GROUPS)
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    parser.add_argument("--max-messages", type=int)
    parser.add_argument("--report-every", type=int, default=100)
    args = parser.parse_args()
    if args.max_messages is not None and args.max_messages < 1:
        parser.error("--max-messages must be at least 1")
    if args.report_every < 1:
        parser.error("--report-every must be at least 1")
    run_consumer(
        args.stream,
        bootstrap_servers=args.bootstrap_servers,
        max_messages=args.max_messages,
        report_every=args.report_every,
    )


if __name__ == "__main__":
    main()
