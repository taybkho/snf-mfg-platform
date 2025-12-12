"""
generate_mock_data.py

This script generates realistic fake manufacturing data as CSV files.
Tables:
- materials.csv
- batches.csv
- production_orders.csv

Each file will have at least 10,000 rows (you can adjust the numbers below).

How to run:
1. Make sure your virtual environment is activated: (.venv)
2. In VS Code terminal, run:
   python ingestion/python/generate_mock_data.py
3. The CSV files will be created under: data/raw/source_system/
"""

import os                      # For working with folders and file paths
import random                  # For random numbers and choices
from datetime import datetime, timedelta  # For working with dates

import pandas as pd            # For creating tables (DataFrames) and saving to CSV
from faker import Faker        # For generating fake but realistic data
from dateutil.relativedelta import relativedelta  # For relative date calculations

# ------------- Global settings ------------- #

# Number of rows per table
NUM_MATERIALS = 500            # fewer materials
NUM_BATCHES = 10000            # 10k+ batches
NUM_ORDERS = 10000             # 10k+ production orders

# Random seed to make results reproducible (same fake data every run)
RANDOM_SEED = 42

# Output folder for the generated CSVs
OUTPUT_DIR = os.path.join("data", "raw", "source_system")

# Initialize Faker with an English locale
fake = Faker("en_US")


def ensure_output_dir_exists() -> None:
    """
    Ensure that the output directory exists.
    If it does not exist, create it.
    """
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)


def generate_materials(num_rows: int) -> pd.DataFrame:
    """
    Generate a materials dimension-like table.

    Columns:
    - material_id: unique code
    - material_name: fake name
    - material_type: API / EXCIPIENT / FINISHED_PRODUCT
    - unit_of_measure: e.g. 'kg', 'L', 'pcs'
    - status: ACTIVE / PHASE_OUT / DEV
    - created_at: date when material was created
    """
    random.seed(RANDOM_SEED)
    material_types = ["API", "EXCIPIENT", "FINISHED_PRODUCT"]
    uoms = ["kg", "g", "L", "mL", "pcs"]
    statuses = ["ACTIVE", "PHASE_OUT", "DEV"]

    materials = []

    for i in range(1, num_rows + 1):
        # Generate a deterministic-looking ID, e.g. MAT-00001
        material_id = f"MAT-{i:05d}"

        # Use Faker for a somewhat realistic product-like name
        material_name = fake.catch_phrase()

        # Pick a random type, UOM, and status
        material_type = random.choice(material_types)
        unit_of_measure = random.choice(uoms)
        status = random.choice(statuses)

        # Created_at: between 5 years ago and today
        days_ago = random.randint(0, 5 * 365)
        created_at = datetime.today() - timedelta(days=days_ago)

        materials.append(
            {
                "material_id": material_id,
                "material_name": material_name,
                "material_type": material_type,
                "unit_of_measure": unit_of_measure,
                "status": status,
                "created_at": created_at.date().isoformat(),
            }
        )

    df = pd.DataFrame(materials)
    return df


def generate_batches(num_rows: int, materials_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate a batches table.

    Columns:
    - batch_id: unique batch/lot number
    - material_id: foreign key to materials
    - manufacturing_site: plant name
    - manufacture_date: when the batch was manufactured
    - expiry_date: manufacture_date + shelf life in months
    - quantity: produced quantity
    - batch_status: RELEASED / QUARANTINE / REJECTED / REWORK
    """
    random.seed(RANDOM_SEED + 1)

    # We will randomly link batches to existing materials
    material_ids = materials_df["material_id"].tolist()

    sites = ["FR_LYON_01", "FR_LYON_02", "ES_BAR_01", "HU_BUD_01"]
    statuses = ["RELEASED", "QUARANTINE", "REJECTED", "REWORK"]

    batches = []

    for i in range(1, num_rows + 1):
        batch_id = f"BAT-{i:07d}"
        material_id = random.choice(material_ids)
        manufacturing_site = random.choice(sites)

        # Manufacture date between 3 years ago and today
        days_ago = random.randint(0, 3 * 365)
        manufacture_date = datetime.today() - timedelta(days=days_ago)

        # Shelf life between 6 and 36 months
        shelf_life_months = random.choice([6, 12, 18, 24, 36])
        expiry_date = manufacture_date + relativedelta(months=shelf_life_months)

        # Quantity between 100 and 50000 (units depend on material)
        quantity = random.randint(100, 50000)

        batch_status = random.choices(
            statuses,
            weights=[0.7, 0.15, 0.1, 0.05],  # mostly RELEASED, few rejected
            k=1,
        )[0]

        batches.append(
            {
                "batch_id": batch_id,
                "material_id": material_id,
                "manufacturing_site": manufacturing_site,
                "manufacture_date": manufacture_date.date().isoformat(),
                "expiry_date": expiry_date.date().isoformat(),
                "quantity": quantity,
                "batch_status": batch_status,
            }
        )

    df = pd.DataFrame(batches)
    return df


def generate_production_orders(num_rows: int, batches_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate a production_orders table.

    Columns:
    - order_id: production order identifier
    - batch_id: which batch this order produced
    - order_type: PP01 / PP02 / PILOT / TEST
    - planned_start_date
    - planned_end_date
    - actual_start_date
    - actual_end_date
    - order_status: PLANNED / IN_PROCESS / COMPLETED / CANCELLED
    - line_id: manufacturing line code
    """
    random.seed(RANDOM_SEED + 2)

    batch_ids = batches_df["batch_id"].tolist()

    order_types = ["PP01", "PP02", "PILOT", "TEST"]
    statuses = ["PLANNED", "IN_PROCESS", "COMPLETED", "CANCELLED"]
    lines = ["LINE_A", "LINE_B", "LINE_C", "LINE_D"]

    orders = []

    for i in range(1, num_rows + 1):
        order_id = f"ORD-{i:07d}"
        batch_id = random.choice(batch_ids)

        order_type = random.choice(order_types)
        line_id = random.choice(lines)

        # Planned start between 3 years ago and 1 month in future
        days_offset = random.randint(-3 * 365, 30)
        planned_start = datetime.today() + timedelta(days=days_offset)

        # Planned duration between 1 and 10 days
        planned_duration_days = random.randint(1, 10)
        planned_end = planned_start + timedelta(days=planned_duration_days)

        # Simulate actuals with some variability
        # Sometimes orders are still in future or cancelled
        status = random.choices(
            statuses,
            weights=[0.1, 0.1, 0.7, 0.1],
            k=1,
        )[0]

        if status == "PLANNED":
            actual_start = None
            actual_end = None
        elif status == "IN_PROCESS":
            # Started but not ended yet
            actual_start = planned_start + timedelta(days=random.randint(-1, 1))
            actual_end = None
        elif status == "COMPLETED":
            actual_start = planned_start + timedelta(days=random.randint(-1, 1))
            # actual end some days after start
            actual_end = actual_start + timedelta(days=planned_duration_days + random.randint(-2, 3))
        else:  # CANCELLED
            actual_start = None
            actual_end = None

        orders.append(
            {
                "order_id": order_id,
                "batch_id": batch_id,
                "order_type": order_type,
                "planned_start_date": planned_start.date().isoformat(),
                "planned_end_date": planned_end.date().isoformat(),
                "actual_start_date": actual_start.date().isoformat() if actual_start else None,
                "actual_end_date": actual_end.date().isoformat() if actual_end else None,
                "order_status": status,
                "line_id": line_id,
            }
        )

    df = pd.DataFrame(orders)
    return df


def main() -> None:
    """
    Main function:
    - Ensure output folder exists
    - Generate materials, batches, and production_orders
    - Save them as CSV files
    """
    print(f"[INFO] Ensuring output directory exists: {OUTPUT_DIR}")
    ensure_output_dir_exists()

    print(f"[INFO] Generating {NUM_MATERIALS} materials...")
    materials_df = generate_materials(NUM_MATERIALS)
    materials_path = os.path.join(OUTPUT_DIR, "materials.csv")
    materials_df.to_csv(materials_path, index=False)
    print(f"[OK] Wrote materials to {materials_path}")

    print(f"[INFO] Generating {NUM_BATCHES} batches...")
    batches_df = generate_batches(NUM_BATCHES, materials_df)
    batches_path = os.path.join(OUTPUT_DIR, "batches.csv")
    batches_df.to_csv(batches_path, index=False)
    print(f"[OK] Wrote batches to {batches_path}")

    print(f"[INFO] Generating {NUM_ORDERS} production orders...")
    orders_df = generate_production_orders(NUM_ORDERS, batches_df)
    orders_path = os.path.join(OUTPUT_DIR, "production_orders.csv")
    orders_df.to_csv(orders_path, index=False)
    print(f"[OK] Wrote production orders to {orders_path}")

    print("[DONE] Mock data generation complete.")


if __name__ == "__main__":
    main()
