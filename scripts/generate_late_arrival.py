import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(207)  # distinct from every prior generator's seed

CREDIT_TIERS = [1000, 2500, 5000, 10000, 15000, 25000, 50000]
STATUSES = ["active", "suspended", "frozen", "closed"]
OUT_DIR = Path("data")

# pull from accounts Session 6 already changed -- late arrivals only matter
# for accounts with more than one historical window to potentially misplace
with open(OUT_DIR / "account_snapshots_initial.csv") as f:
    initial = {r["account_id"]: r for r in csv.DictReader(f)}
with open(OUT_DIR / "account_snapshots_update.csv") as f:
    updated = {r["account_id"]: r for r in csv.DictReader(f)}

changed_accounts = [
    acc for acc in initial
    if initial[acc]["account_status"] != updated[acc]["account_status"]
    or initial[acc]["credit_limit"] != updated[acc]["credit_limit"]
]

sample = random.sample(changed_accounts, k=20)

rows = []
for acc in sample:
    # a date strictly inside the original 2026-01-01 -> 2026-08-11 window
    days_offset = random.randint(10, 200)
    effective = date(2026, 1, 1) + timedelta(days=days_offset)

    # deliberately a THIRD distinct state -- different from both the
    # 2026-01-01 original and the 2026-08-11 Session 6 value, so a correct
    # gap-insertion is unambiguous, not coincidentally matching a bookend
    orig_status = initial[acc]["account_status"]
    orig_limit = int(initial[acc]["credit_limit"])
    new_status = random.choice([s for s in STATUSES if s != orig_status])
    new_limit = random.choice([t for t in CREDIT_TIERS if t != orig_limit])

    rows.append({
        "account_id": acc,
        "account_status": new_status,
        "credit_limit": new_limit,
        "effective_date": effective.isoformat(),
    })

out_path = OUT_DIR / "account_snapshots_late_arrival.csv"
with out_path.open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["account_id", "account_status", "credit_limit", "effective_date"])
    writer.writeheader()
    writer.writerows(rows)

print(f"wrote {len(rows)} late-arriving records to {out_path}")
print(f"date range: {min(r['effective_date'] for r in rows)} to {max(r['effective_date'] for r in rows)}")
print(f"sample: {rows[0]}")
