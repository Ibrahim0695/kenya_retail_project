"""Create the retail Kafka topics used by the operational event pipeline."""

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from streaming.settings import (
    BOOTSTRAP_SERVERS,
    INVENTORY_TOPIC,
    PRODUCT_TOPIC,
    SALES_TOPIC,
    STORE_TOPIC,
    SUPPLIER_TOPIC,
)


def create_topics(bootstrap_servers: str = BOOTSTRAP_SERVERS) -> None:
    """Create versioned event topics and compacted master-data topics."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    topics = [
        NewTopic(STORE_TOPIC, num_partitions=1, replication_factor=1,
                 config={"cleanup.policy": "compact"}),
        NewTopic(PRODUCT_TOPIC, num_partitions=1, replication_factor=1,
                 config={"cleanup.policy": "compact"}),
        NewTopic(SUPPLIER_TOPIC, num_partitions=1, replication_factor=1,
                 config={"cleanup.policy": "compact"}),
        NewTopic(SALES_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(INVENTORY_TOPIC, num_partitions=3, replication_factor=1),
    ]

    for topic, future in admin.create_topics(topics).items():
        try:
            future.result()
            print(f"[topics] created {topic}")
        except KafkaException as exc:
            error = exc.args[0]
            if error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"[topics] already exists: {topic}")
            else:
                raise RuntimeError(f"Could not create Kafka topic {topic}") from exc


def main() -> None:
    create_topics()


if __name__ == "__main__":
    main()
