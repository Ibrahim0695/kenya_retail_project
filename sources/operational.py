"""Operational data: stores, products, suppliers, sales and inventory.

This is synthetic (no real retailer would share its POS data). We use Python's
built-in `random` module with a fixed seed, so the data is realistic but the
same every time you run it.
"""

import random
from datetime import date, timedelta

import pandas as pd

from config import SEED, START_DATE
from landing import save_many_to_landing

STORES = [
    {"store_id": "ST001", "store_name": "Nairobi CBD", "county": "Nairobi", "store_format": "Supermarket"},
    {"store_id": "ST002", "store_name": "Nairobi West", "county": "Nairobi", "store_format": "Mini Mart"},
    {"store_id": "ST003", "store_name": "Mombasa Town", "county": "Mombasa", "store_format": "Supermarket"},
    {"store_id": "ST004", "store_name": "Nakuru Centre", "county": "Nakuru", "store_format": "Mini Mart"},
    {"store_id": "ST005", "store_name": "Kisumu Main", "county": "Kisumu", "store_format": "Supermarket"},
    {"store_id": "ST006", "store_name": "Eldoret Plaza", "county": "Uasin Gishu", "store_format": "Mini Mart"},
]

PRODUCTS = [
    {"product_id": "P01", "product_name": "Maize Flour 2kg", "category": "Cereals & Grains", "base_price": 96.0},
    {"product_id": "P02", "product_name": "Rice 1kg", "category": "Cereals & Grains", "base_price": 190.0},
    {"product_id": "P03", "product_name": "Wheat Flour 1kg", "category": "Cereals & Grains", "base_price": 62.0},
    {"product_id": "P04", "product_name": "Cooking Oil 1L", "category": "Cooking Fats & Oils", "base_price": 280.0},
    {"product_id": "P05", "product_name": "Sugar 1kg", "category": "Sugar & Tea", "base_price": 155.0},
    {"product_id": "P06", "product_name": "Tea Leaves 500g", "category": "Sugar & Tea", "base_price": 240.0},
    {"product_id": "P07", "product_name": "Fresh Milk 1L", "category": "Dairy", "base_price": 55.0},
    {"product_id": "P08", "product_name": "Tomatoes 1kg", "category": "Fresh Produce", "base_price": 95.0},
    {"product_id": "P09", "product_name": "Onions 1kg", "category": "Fresh Produce", "base_price": 85.0},
    {"product_id": "P10", "product_name": "Bar Soap", "category": "Household", "base_price": 210.0},
    {"product_id": "P11", "product_name": "Cooking Fat 1kg", "category": "Cooking Fats & Oils", "base_price": 160.0},
    {"product_id": "P12", "product_name": "Bread 400g", "category": "Cereals & Grains", "base_price": 60.0},
]

SUPPLIERS = [
    {"supplier_id": "SUP001", "supplier_name": "Nairobi Wholesale Ltd", "county": "Nairobi"},
    {"supplier_id": "SUP002", "supplier_name": "Coast Distributors", "county": "Mombasa"},
    {"supplier_id": "SUP003", "supplier_name": "Rift Valley Supplies", "county": "Nakuru"},
    {"supplier_id": "SUP004", "supplier_name": "Western Traders", "county": "Kisumu"},
    {"supplier_id": "SUP005", "supplier_name": "National FMCG Ltd", "county": "Nairobi"},
]

CASHIERS = [
    "Wanjiku Mwangi",
    "Otieno Ochieng",
    "Amina Hassan",
    "Kiptoo Kipchoge",
    "Njeri Wambui",
    "Mutua Mutiso",
    "Halima Ali",
    "Brian Odhiambo",
    "Faith Njoroge",
    "Kamau Abdi",
]

PAYMENT_METHODS = ["M-PESA", "CARD", "CASH"]
PAYMENT_WEIGHTS = [60, 15, 25]


def _days(start: date, end: date):
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)


def make_sales(start, end, stores, products, rng):
    """Create POS transactions and their line items."""
    transactions = []
    lines = []

    for day in _days(start, end):
        # Weekends are busier.
        factor = 1.4 if day.weekday() >= 5 else 1.0
        for _, store in stores.iterrows():
            n_transactions = int(rng.randint(4, 8) * factor)
            for i in range(n_transactions):
                transaction_id = f"TX{day:%Y%m%d}{store['store_id']}{i:03d}"
                transactions.append(
                    {
                        "transaction_id": transaction_id,
                        "store_id": store["store_id"],
                        "transaction_ts": (
                            f"{day.isoformat()}T"
                            f"{rng.randint(8, 19):02d}:{rng.randint(0, 59):02d}:00"
                        ),
                        "cashier_name": rng.choice(CASHIERS),
                        "payment_method": rng.choices(
                            PAYMENT_METHODS, weights=PAYMENT_WEIGHTS
                        )[0],
                    }
                )

                n_items = rng.randint(1, 4)
                chosen = rng.sample(range(len(products)), n_items)
                for line_number, index in enumerate(chosen, start=1):
                    product = products.iloc[index]
                    quantity = rng.randint(1, 3)
                    unit_price = round(
                        float(product["base_price"]) * rng.uniform(0.97, 1.03), 2
                    )
                    lines.append(
                        {
                            "transaction_id": transaction_id,
                            "line_number": line_number,
                            "product_id": product["product_id"],
                            "quantity": quantity,
                            "unit_price": unit_price,
                            "line_total": round(unit_price * quantity, 2),
                        }
                    )

    return pd.DataFrame(transactions), pd.DataFrame(lines)


def make_inventory(start, end, stores, products, transactions, lines, rng):
    """Opening stock, weekly deliveries and daily sale deductions."""
    rows = []
    counter = 0

    def add(ts, store_id, product_id, movement_type, quantity, unit_cost):
        nonlocal counter
        counter += 1
        rows.append(
            {
                "movement_id": f"MOV{counter:07d}",
                "movement_ts": ts,
                "store_id": store_id,
                "product_id": product_id,
                "movement_type": movement_type,
                "quantity": quantity,
                "unit_cost": unit_cost,
            }
        )

    # 1. Opening stock for every store/product.
    for _, store in stores.iterrows():
        for _, product in products.iterrows():
            add(
                f"{start.isoformat()}T07:00:00",
                store["store_id"],
                product["product_id"],
                "OPENING",
                rng.randint(20, 80),
                round(float(product["base_price"]) * 0.70, 2),
            )

    # 2. Weekly deliveries.
    day = start
    while day <= end:
        for _, store in stores.iterrows():
            for index in rng.sample(range(len(products)), 3):
                product = products.iloc[index]
                add(
                    f"{day.isoformat()}T08:00:00",
                    store["store_id"],
                    product["product_id"],
                    "DELIVERY",
                    rng.randint(20, 100),
                    round(float(product["base_price"]) * 0.70, 2),
                )
        day += timedelta(days=7)

    # 3. Daily sale deductions (one row per store/product/day).
    sold = lines.merge(
        transactions[["transaction_id", "store_id", "transaction_ts"]],
        on="transaction_id",
    )
    sold["sale_date"] = pd.to_datetime(sold["transaction_ts"]).dt.date
    daily = sold.groupby(
        ["store_id", "sale_date", "product_id"], as_index=False
    )["quantity"].sum()

    cost_by_product = dict(zip(products["product_id"], products["base_price"]))
    for _, row in daily.iterrows():
        add(
            f"{row['sale_date']}T22:00:00",
            row["store_id"],
            row["product_id"],
            "SALE",
            -int(row["quantity"]),
            round(float(cost_by_product[row["product_id"]]) * 0.70, 2),
        )

    return pd.DataFrame(rows)


def ingest(start: str | None = None, end: str | None = None) -> None:
    """Generate all operational tables and save them to the landing zone."""
    start_date = date.fromisoformat(start or START_DATE)
    end_date = date.fromisoformat(end) if end else date.today()
    rng = random.Random(SEED)

    stores = pd.DataFrame(STORES)
    products = pd.DataFrame(PRODUCTS)
    suppliers = pd.DataFrame(SUPPLIERS)

    transactions, lines = make_sales(start_date, end_date, stores, products, rng)
    movements = make_inventory(
        start_date,
        end_date,
        stores,
        products,
        transactions,
        lines,
        rng,
    )

    tables = {
        "store_master.csv": stores,
        "product_master.csv": products,
        "supplier_master.csv": suppliers,
        "pos_transactions.csv": transactions,
        "pos_transaction_lines.csv": lines,
        "inventory_movements.csv": movements,
    }
    save_many_to_landing(
        tables, "operational", start_date.isoformat(), end_date.isoformat()
    )
    print(
        f"[operational] synthetic data for {start_date} -> {end_date} "
        f"({len(transactions):,} transactions)"
    )

def main():
    print("[operational] starting operational data generation...")
    ingest()

if __name__ == "__main__":
    main()