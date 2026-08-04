import csv
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

NUM_TRANSACTIONS = 10_000
NUM_ACCOUNTS = 500  # matches Day 6 account_snapshots population
CURRENCIES = ["USD", "EUR", "GBP", "CAD", "AUD"]
TRANSACTION_TYPES = ["purchase", "refund", "transfer", "withdrawal", "deposit"]
OUT_DIR = Path("data")

# one identity per account, looked up per transaction -- an account
# belongs to one customer, it shouldn't reroll on every row
accounts = {
    f"ACC{i:05d}": {
        "customer_name": fake.name(),
        "ssn_last4": f"{random.randint(0, 9999):04d}",
    }
    for i in range(1, NUM_ACCOUNTS + 1)
}
account_ids = list(accounts.keys())


def random_amount() -> float:
    # log-normal: most transactions small, long tail into high-value
    # territory, clipped to the Day 9 GE contract range
    amount = random.lognormvariate(mu=6.0, sigma=1.6)
    return round(min(max(amount, 0.01), 1_000_000), 2)


def random_event_timestamp() -> datetime:
    days_back = random.randint(0, 90)
    seconds_back = random.randint(0, 86_400)
    return datetime.now() - timedelta(days=days_back, seconds=seconds_back)


def generate_row() -> dict:
    account_id = random.choice(account_ids)
    identity = accounts[account_id]
    event_ts = random_event_timestamp()
    # normal-case ingestion lag -- late arrival is Day 7's problem, not today's
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


def main():
    rows = [generate_row() for _ in range(NUM_TRANSACTIONS)]
    OUT_DIR.mkdir(exist_ok=True)

    csv_path = OUT_DIR / "transactions.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    json_path = OUT_DIR / "transactions.json"
    with json_path.open("w") as f:
        json.dump(rows, f, indent=2)

    amounts = [r["amount"] for r in rows]
    over_10k = sum(1 for a in amounts if a > 10_000)
    print(f"wrote {len(rows)} rows to {csv_path} and {json_path}")
    print(f"unique transaction_ids: {len(set(r['transaction_id'] for r in rows))}")
    print(f"unique accounts touched: {len(set(r['account_id'] for r in rows))}")
    print(f"amount min/mean/max: {min(amounts):.2f} / {sum(amounts)/len(amounts):.2f} / {max(amounts):.2f}")
    print(f"transactions > $10,000: {over_10k} ({over_10k/len(rows)*100:.1f}%)")


if __name__ == "__main__":
    main()
