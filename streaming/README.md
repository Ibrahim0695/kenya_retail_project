# Operational Kafka event pipeline

This pipeline replays operational landing-zone CSVs to versioned Kafka topics.
It is a local development/demo workflow: it simulates live POS and inventory
events from synthetic data; it does not connect to a real retailer's POS.

## Start Kafka (PowerShell)

From the project root:

```powershell
docker compose up -d
```

Wait for the broker health check to pass, then create the topics:

```powershell
python -m streaming.topics
```

## Run a replay

Replay the newest operational batch:

```powershell
python -m streaming.producer
```

Throttle output to simulate live traffic, or replay a small test sample:

```powershell
python -m streaming.producer --delay 0.05
python -m streaming.producer --max-events 100
```

The producer emits compacted master-data updates first, then sales and
chronologically ordered inventory movements. It joins each sales line with its
transaction, product, and store records. Events use a JSON envelope containing
`event_id`, `event_type`, `schema_version`, `event_time`, `source`,
`source_batch`, and `payload`.

## Consumer groups

Run each consumer in a separate terminal:

```powershell
python -m streaming.consumer sales
python -m streaming.consumer inventory
```

The sales processor uses consumer group `retail-sales-processor-v1`; the
inventory processor uses `retail-inventory-processor-v1`. Start another
instance with the same stream argument to add a worker to that stream's group.
Consumers start at the earliest offset if the group has not consumed before,
and manually commit each successfully processed message.

## Topics

| Topic | Key | Contents |
|---|---|---|
| `retail.stores.v1` | `store_id` | Compacted store master updates |
| `retail.products.v1` | `product_id` | Compacted product master updates |
| `retail.suppliers.v1` | `supplier_id` | Compacted supplier master updates |
| `retail.sales.v1` | `transaction_id` | Enriched transaction line events |
| `retail.inventory.v1` | `store_id:product_id` | Stock movements, in event-time order |

The versioned raw topics are the durable integration boundary for a future
Spark Structured Streaming job. Spark can read those topics independently of
these demo consumer groups. The current consumer summaries and inventory
balances are process-local; a production stateful processor should checkpoint
state or persist projections before being relied on after restart.

Stop the local broker with `docker compose down`. This demo keeps Kafka data
inside the container, so removing/recreating the container also removes its
topic data. Replay the producer batch after restarting the broker.
