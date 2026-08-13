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
NUM_BATCH_ROWS = 1_000
POISON_RATE = 0.15
CURRENCIES = ["USD", "EUR", "GBP", "CAD", "AUD"]
TRANSACTION_TYPES = ["purchase", "refund", "transfer", "withdrawal", "deposit"]
OUT_DIR = Path("data")

# identical construction to generate_transactions.py -- same seed, same
# order, so masked hashes for these accounts stay consistent with what's
# already in Bronze. accounts don't get new identities for a new batch.
accounts = {
    f"ACC{i:05d}": {
        "customer_name": fake.name(),
        "ssn_last4": f"{random.randint(0, 9999):04d}",
    }
    for i in range(1, NUM_ACCOUNTS + 1)
}
account_ids = list(accounts.keys())

# reseed here -- continuing the seed-42 stream would replay batch 1's
# first 1,000 rows almost exactly (same account picks, same amounts).
# a distinct seed keeps this batch independently reproducible.
random.seed(105)

BATCH_ANCHOR = datetime(2026, 8, 6, 9, 0, 0, tzinfo=timezone.utc)


def random_amount() -> float:
    amount = random.lognormvariate(mu=6.0, sigma=1.6)
    return round(min(max(amount, 0.01), 1_000_000), 2)


def random_event_timestamp() -> datetime:
    # new batch arriving today -- last 24h, not batch 1's 90-day window
    seconds_back = random.randint(0, 86_400)
    return BATCH_ANCHOR - timedelta(seconds=seconds_back)


def generate_row(poison: bool) -> dict:
    account_id = random.choice(account_ids)
    identity = accounts[account_id]
    event_ts = random_event_timestamp()
    ingestion_ts = event_ts + timedelta(seconds=random.randint(1, 7_200))

    if poison:
        amount = None if random.random() < 0.5 else -round(random.uniform(1, 5000), 2)
    else:
        amount = random_amount()

    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id,
        "customer_name": identity["customer_name"],
        "ssn_last4": identity["ssn_last4"],
        "amount": amount,
        "currency": random.choice(CURRENCIES),
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "event_timestamp": event_ts.isoformat(),
        "ingestion_timestamp": ingestion_ts.isoformat(),
    }


def main():
    n_poison = int(NUM_BATCH_ROWS * POISON_RATE)
    poison_indices = set(random.sample(range(NUM_BATCH_ROWS), k=n_poison))

    rows = [generate_row(poison=i in poison_indices) for i in range(NUM_BATCH_ROWS)]

    OUT_DIR.mkdir(exist_ok=True)
    csv_path = OUT_DIR / "transactions_batch2_poisoned.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    null_count = sum(1 for r in rows if r["amount"] is None)
    neg_count = sum(1 for r in rows if r["amount"] is not None and r["amount"] < 0)
    print(f"wrote {len(rows)} rows to {csv_path}")
    print(f"unique transaction_ids: {len(set(r['transaction_id'] for r in rows))}")
    print(f"poisoned: {null_count + neg_count} ({(null_count + neg_count)/len(rows)*100:.1f}%) "
          f"-- {null_count} null, {neg_count} negative")


if __name__ == "__main__":
    main()
