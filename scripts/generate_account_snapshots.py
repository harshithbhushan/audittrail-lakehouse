import csv
import random
from pathlib import Path

random.seed(42)

NUM_ACCOUNTS = 500
CHANGE_RATE = 0.20
CREDIT_TIERS = [1000, 2500, 5000, 10000, 15000, 25000, 50000]
STATUSES = ["active", "suspended", "frozen", "closed"]
STATUS_WEIGHTS = [0.90, 0.05, 0.03, 0.02]
OUT_DIR = Path("data")

account_ids = [f"ACC{i:05d}" for i in range(1, NUM_ACCOUNTS + 1)]

# initial state: one credit tier + status per account, as of account opening
initial_state = {
    acc: {
        "account_status": random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
        "credit_limit": random.choice(CREDIT_TIERS),
    }
    for acc in account_ids
}

# 20% get a real change -- one attribute per account, not both, so it's
# easy to trace exactly what changed during verification
changed_accounts = set(random.sample(account_ids, k=int(NUM_ACCOUNTS * CHANGE_RATE)))

updated_state = {}
for acc in account_ids:
    current = initial_state[acc]
    if acc not in changed_accounts:
        updated_state[acc] = dict(current)
        continue

    if random.random() < 0.5:
        # credit review: bump to a different tier than current
        other_tiers = [t for t in CREDIT_TIERS if t != current["credit_limit"]]
        updated_state[acc] = {
            "account_status": current["account_status"],
            "credit_limit": random.choice(other_tiers),
        }
    else:
        other_statuses = [s for s in STATUSES if s != current["account_status"]]
        updated_state[acc] = {
            "account_status": random.choice(other_statuses),
            "credit_limit": current["credit_limit"],
        }


def write_batch(state: dict, effective_date: str, path: Path):
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["account_id", "account_status", "credit_limit", "effective_date"])
        for acc in account_ids:
            row = state[acc]
            writer.writerow([acc, row["account_status"], row["credit_limit"], effective_date])


OUT_DIR.mkdir(exist_ok=True)
write_batch(initial_state, "2026-01-01", OUT_DIR / "account_snapshots_initial.csv")
write_batch(updated_state, "2026-08-11", OUT_DIR / "account_snapshots_update.csv")

actually_changed = sum(1 for acc in account_ids if initial_state[acc] != updated_state[acc])
print(f"accounts: {len(account_ids)}")
print(f"flagged as changed: {len(changed_accounts)} ({len(changed_accounts)/NUM_ACCOUNTS*100:.0f}%)")
print(f"actually different between initial and update: {actually_changed}")
