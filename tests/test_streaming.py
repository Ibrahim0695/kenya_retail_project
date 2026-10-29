import unittest
from unittest.mock import patch

from streaming.consumer import InventoryProcessor, SalesProcessor
from streaming.producer import build_events, latest_batch, publish_batch
from streaming.settings import INVENTORY_TOPIC, SALES_TOPIC


class StreamingEventTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            batch = latest_batch()
        except FileNotFoundError as exc:
            raise unittest.SkipTest("Operational sample batch is not available") from exc
        cls.events = list(build_events(batch))

    def test_producer_emits_versioned_sales_and_inventory_envelopes(self):
        sales = [event for topic, _, event in self.events if topic == SALES_TOPIC]
        inventory = [
            event for topic, _, event in self.events if topic == INVENTORY_TOPIC
        ]
        self.assertGreater(len(sales), 0)
        self.assertGreater(len(inventory), 0)
        for event in (sales[0], inventory[0]):
            self.assertEqual(event["schema_version"], 1)
            self.assertIn("event_id", event)
            self.assertIn("event_time", event)
            self.assertIn("payload", event)

    def test_producer_publishes_with_delivery_callbacks(self):
        class FakeProducer:
            def __init__(self, config):
                self.messages = []

            def produce(self, topic, key, value, on_delivery):
                self.messages.append((topic, key, value))
                on_delivery(None, None)

            def poll(self, timeout):
                return 0

            def flush(self, timeout):
                return 0

        with patch("streaming.producer.Producer", FakeProducer):
            sent = publish_batch(latest_batch(), max_events=5)
        self.assertEqual(sent, 5)

    def test_sales_processor_summarizes_line_totals(self):
        processor = SalesProcessor()
        processor.process(
            {
                "event_type": "SaleLine",
                "payload": {"line_total": 12.5, "store_id": "ST001"},
            }
        )
        self.assertEqual(processor.report()["sale_lines"], 1)
        self.assertEqual(processor.report()["gross_sales"], 12.5)

    def test_inventory_processor_applies_signed_movements(self):
        processor = InventoryProcessor()
        for quantity in (20, 5, -3):
            processor.process(
                {
                    "event_type": "InventoryMovement",
                    "payload": {
                        "store_id": "ST001",
                        "product_id": "P01",
                        "movement_type": "TEST",
                        "quantity": quantity,
                    },
                }
            )
        balance = processor.report()["on_hand_by_store_product"]
        self.assertEqual(balance[0]["quantity"], 22)


if __name__ == "__main__":
    unittest.main()
