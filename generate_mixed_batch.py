import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(308)  # distinct from every prior generator's seed

CREDIT_TIERS = [1000, 2500, 5000, 10000, 15000, 25000, 50000]
STATUSES = ["active", "suspended", "frozen", "closed"]
OUT_DIR = Path("data")

with open(OUT_DIR / "account_snapshots_initial.csv") as f:
    initial = {r["account_id"]: r for r in csv.DictReader(f)}
with open(OUT_DIR / "account_snapshots_update.csv") as f:
    updated = {r["account_id"]: r for r in csv.DictReader(f)}

changed_by_s6 = [
    acc for acc in initial
    if initial[acc]["account_status"] != updated[acc]["account_status"]
    or initial[acc]["credit_limit"] != updated[acc]["credit_limit"]
]
unchanged_by_s6 = [acc for acc in initial if acc not in changed_by_s6]

rows = []

# late portion: 15 accounts S6 already changed, dated inside their
# original (pre-S6) window -- same shape as Session 7
for acc in random.sample(changed_by_s6, k=15):
    days_offset = random.randint(10, 200)
    effective = date(2026, 1, 1) + timedelta(days=days_offset)
    orig_status = initial[acc]["account_status"]
    orig_limit = int(initial[acc]["credit_limit"])
    rows.append({
        "account_id": acc,
        "account_status": random.choice([s for s in STATUSES if s != orig_status]),
        "credit_limit": random.choice([t for t in CREDIT_TIERS if t != orig_limit]),
        "effective_date": effective.isoformat(),
    })

# normal portion: 15 accounts S6 never touched, dated AFTER S6 (a genuine
# new forward-moving change) -- tests the ordinary close-current path
for acc in random.sample(unchanged_by_s6, k=15):
    days_offset = random.randint(1, 10)
    effective = date(2026, 8, 11) + timedelta(days=days_offset)
    orig_status = initial[acc]["account_status"]
    orig_limit = int(initial[acc]["credit_limit"])
    rows.append({
        "account_id": acc,
        "account_status": random.choice([s for s in STATUSES if s != orig_status]),
        "credit_limit": random.choice([t for t in CREDIT_TIERS if t != orig_limit]),
        "effective_date": effective.isoformat(),
    })

random.shuffle(rows)  # mixed order in the file -- the code shouldn't care

out_path = OUT_DIR / "account_snapshots_mixed_batch.csv"
with out_path.open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["account_id", "account_status", "credit_limit", "effective_date"])
    writer.writeheader()
    writer.writerows(rows)

print(f"wrote {len(rows)} rows to {out_path} (15 late, 15 normal, shuffled)")
