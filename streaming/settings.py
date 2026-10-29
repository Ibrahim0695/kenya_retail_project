"""Shared Kafka topic and broker settings."""

BOOTSTRAP_SERVERS = "localhost:29092"

STORE_TOPIC = "retail.stores.v1"
PRODUCT_TOPIC = "retail.products.v1"
SUPPLIER_TOPIC = "retail.suppliers.v1"
SALES_TOPIC = "retail.sales.v1"
INVENTORY_TOPIC = "retail.inventory.v1"

REFERENCE_TOPICS = (STORE_TOPIC, PRODUCT_TOPIC, SUPPLIER_TOPIC)
EVENT_TOPICS = (SALES_TOPIC, INVENTORY_TOPIC)
