"""Replay operational CSV data into versioned Kafka topics."""

import argparse
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer

from config import LANDING_DIR
from streaming.settings import (
    BOOTSTRAP_SERVERS,
    INVENTORY_TOPIC,
    PRODUCT_TOPIC,
    SALES_TOPIC,
    STORE_TOPIC,
    SUPPLIER_TOPIC,
)

MASTER_TABLES = (
    ("store_master.csv", STORE_TOPIC, "StoreUpsert", "store_id"),
    ("product_master.csv", PRODUCT_TOPIC, "ProductUpsert", "product_id"),
    ("supplier_master.csv", SUPPLIER_TOPIC, "SupplierUpsert", "supplier_id"),
)


def latest_batch() -> Path:
    """Return the newest generated operational data batch."""
    batches = sorted(
        (path.parent for path in (LANDING_DIR / "operational").glob("*/manifest.json")),
        key=lambda path: path.name,
    )
    if not batches:
        raise FileNotFoundError(
            "No operational data batches found. Run python -m sources.operational first."
        )
    return batches[-1]


def read_csv(path: Path) -> list[dict[str, str]]:
    """Read one operational CSV as dictionaries."""
    with path.open("r", newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def make_event(
    event_id: str,
    event_type: str,
    event_time: str | None,
    payload: dict,
    source_batch: str,
) -> dict:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "schema_version": 1,
        "event_time": event_time,
        "source": "operational_csv",
        "source_batch": source_batch,
        "payload": payload,
    }


def _number(value: str, kind):
    try:
        return kind(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Expected {kind.__name__} value, got {value!r}") from exc


def build_events(batch: Path):
    """Yield reference records followed by sales and inventory events."""
    source_batch = batch.name
    emitted_at = datetime.now(timezone.utc).isoformat()

    stores = {row["store_id"]: row for row in read_csv(batch / "store_master.csv")}
    products = {
        row["product_id"]: row for row in read_csv(batch / "product_master.csv")
    }
    transactions = {
        row["transaction_id"]: row
        for row in read_csv(batch / "pos_transactions.csv")
    }

    for filename, topic, event_type, id_field in MASTER_TABLES:
        for row in read_csv(batch / filename):
            yield (
                topic,
                row[id_field],
                make_event(
                    f"{event_type.lower()}:{row[id_field]}",
                    event_type,
                    emitted_at,
                    row,
                    source_batch,
                ),
            )

    sales = []
    for line in read_csv(batch / "pos_transaction_lines.csv"):
        transaction_id = line["transaction_id"]
        if transaction_id not in transactions:
            raise ValueError(f"No transaction header for {transaction_id}")
        transaction = transactions[transaction_id]
        product_id = line["product_id"]
        if product_id not in products:
            raise ValueError(f"No product master row for {product_id}")
        store_id = transaction["store_id"]
        if store_id not in stores:
            raise ValueError(f"No store master row for {store_id}")

        payload = {
            "transaction_id": transaction_id,
            "line_number": _number(line["line_number"], int),
            "store_id": store_id,
            "store": stores[store_id],
            "transaction_ts": transaction["transaction_ts"],
            "cashier_name": transaction["cashier_name"],
            "payment_method": transaction["payment_method"],
            "product_id": product_id,
            "product": products[product_id],
            "quantity": _number(line["quantity"], int),
            "unit_price": _number(line["unit_price"], float),
            "line_total": _number(line["line_total"], float),
        }
        sales.append(
            (
                transaction["transaction_ts"],
                SALES_TOPIC,
                transaction_id,
                make_event(
                    f"sale:{transaction_id}:{line['line_number']}",
                    "SaleLine",
                    transaction["transaction_ts"],
                    payload,
                    source_batch,
                ),
            )
        )

    for _, topic, key, event in sorted(sales, key=lambda item: item[0]):
        yield topic, key, event

    movements = []
    for row in read_csv(batch / "inventory_movements.csv"):
        payload = {
            "movement_id": row["movement_id"],
            "store_id": row["store_id"],
            "product_id": row["product_id"],
            "movement_type": row["movement_type"],
            "quantity": _number(row["quantity"], int),
            "unit_cost": _number(row["unit_cost"], float),
            "movement_ts": row["movement_ts"],
        }
        movements.append(
            (
                row["movement_ts"],
                f"{row['store_id']}:{row['product_id']}",
                make_event(
                    f"inventory:{row['movement_id']}",
                    "InventoryMovement",
                    row["movement_ts"],
                    payload,
                    source_batch,
                ),
            )
        )

    for _, key, event in sorted(movements, key=lambda item: item[0]):
        yield INVENTORY_TOPIC, key, event


def publish_batch(
    batch: Path,
    bootstrap_servers: str = BOOTSTRAP_SERVERS,
    delay: float = 0,
    max_events: int | None = None,
) -> int:
    """Publish a batch, optionally throttling it to simulate live traffic."""
    if delay < 0:
        raise ValueError("delay must be zero or greater")
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    delivery_errors = []
    sent = 0

    def on_delivery(error, message) -> None:
        if error is not None:
            delivery_errors.append(error)

    for topic, key, event in build_events(batch):
        if max_events is not None and sent >= max_events:
            break
        value = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
        while True:
            try:
                producer.produce(
                    topic,
                    key=key,
                    value=value,
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                producer.poll(1)
        producer.poll(0)
        sent += 1
        if delay:
            time.sleep(delay)

    remaining = producer.flush(30)
    if remaining:
        raise TimeoutError(f"{remaining} Kafka message(s) were not delivered.")
    if delivery_errors:
        raise RuntimeError(
            f"Kafka failed to deliver {len(delivery_errors)} message(s): "
            f"{delivery_errors[0]}"
        )
    return sent


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--batch",
        type=Path,
        help="Operational batch folder (defaults to the newest landing batch).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0,
        help="Seconds between messages to simulate a live event stream.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        help="Optional cap for a small test replay.",
    )
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    args = parser.parse_args()

    batch = args.batch or latest_batch()
    if not batch.is_dir():
        parser.error(f"Batch directory does not exist: {batch}")
    if args.max_events is not None and args.max_events < 1:
        parser.error("--max-events must be at least 1")

    print(f"[producer] replaying operational batch {batch}")
    sent = publish_batch(
        batch,
        bootstrap_servers=args.bootstrap_servers,
        delay=args.delay,
        max_events=args.max_events,
    )
    print(f"[producer] delivered {sent:,} event(s)")


if __name__ == "__main__":
    main()
