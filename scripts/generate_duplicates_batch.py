import csv
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

NUM_ACCOUNTS = 500
NUM_TRANSACTIONS = 1_000
DUPLICATE_RATE = 0.05
CURRENCIES = ["USD", "EUR", "GBP", "CAD", "AUD"]
TRANSACTION_TYPES = ["purchase", "refund", "transfer", "withdrawal", "deposit"]
OUT_DIR = Path("data")

# identical construction to generate_transactions.py -- same seed, same
# order, so masked hashes stay consistent with every other batch in the repo
accounts = {
    f"ACC{i:05d}": {
        "customer_name": fake.name(),
        "ssn_last4": f"{random.randint(0, 9999):04d}",
    }
    for i in range(1, NUM_ACCOUNTS + 1)
}
account_ids = list(accounts.keys())

# reseed for batch-specific draws, same convention as every prior batch generator
random.seed(408)

BATCH_ANCHOR = datetime(2026, 8, 12, 9, 0, 0, tzinfo=timezone.utc)


def random_amount() -> float:
    amount = random.lognormvariate(mu=6.0, sigma=1.6)
    return round(min(max(amount, 0.01), 1_000_000), 2)


def make_base_row() -> dict:
    account_id = random.choice(account_ids)
    identity = accounts[account_id]
    event_ts = BATCH_ANCHOR - timedelta(seconds=random.randint(0, 86_400))
    ingestion_ts = event_ts + timedelta(seconds=random.randint(1, 7_200))
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id,
        "customer_name": identity["customer_name"],
        "ssn_last4": identity["ssn_last4"],
        "amount": random_amount(),
        "currency": random.choice(CURRENCIES),
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "event_timestamp": event_ts.isoformat(),
        "ingestion_timestamp": ingestion_ts.isoformat(),
    }


rows = [make_base_row() for _ in range(NUM_TRANSACTIONS)]

n_dupes = int(NUM_TRANSACTIONS * DUPLICATE_RATE)
dupe_originals = random.sample(rows, k=n_dupes)

duplicate_rows = []
for orig in dupe_originals:
    event_ts = datetime.fromisoformat(orig["event_timestamp"])
    # resent later -- same event, corrected amount, later ingestion
    later_ingestion = event_ts + timedelta(seconds=random.randint(7_201, 14_400))
    dup = dict(orig)
    dup["amount"] = random_amount()  # the "corrected" value
    dup["ingestion_timestamp"] = later_ingestion.isoformat()
    duplicate_rows.append(dup)

all_rows = rows + duplicate_rows
random.shuffle(all_rows)

out_path = OUT_DIR / "transactions_batch3_duplicates.csv"
with out_path.open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=all_rows[0].keys())
    writer.writeheader()
    writer.writerows(all_rows)

unique_ids = len(set(r["transaction_id"] for r in all_rows))
print(f"wrote {len(all_rows)} rows to {out_path}")
print(f"unique transaction_ids: {unique_ids} (expect {NUM_TRANSACTIONS})")
print(f"duplicated ids: {n_dupes} (expect {n_dupes}, {DUPLICATE_RATE*100:.0f}%)")
