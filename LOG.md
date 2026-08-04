# LOG

## Day 1 — 2026-08-04 — Environment Setup + Synthetic Data Generation

**Goal**
Stand up the Databricks Free Edition workspace, build a reproducible synthetic transaction generator (10,000 rows), and get the repo scaffolded and live on GitHub.

**Outcome**
Go. All three Day 1 check criteria met: working serverless workspace confirmed, 10,000-row synthetic dataset generated and verified byte-for-byte reproducible across an independent test run and the local Windows run, repo live and public with generator, README, .gitignore, and requirements.txt committed — no raw data leaked into version control.

**Actions**
- Signed up for Databricks Free Edition, created a notebook, confirmed serverless compute auto-attaches on first cell execution (`spark.range(5).show()` → 5-row DataFrame, no manual cluster config).
- Built `generate_transactions.py`: Faker-based generator for 10,000 synthetic transactions across 500 unique accounts.
- Ran locally (Windows/PowerShell, `faker==40.36.0`, seeded): 10,000 rows, 10,000 unique `transaction_id`s, 500/500 accounts touched, no nulls, amount min/mean/max $1.83 / $1,398.62 / $143,279.11, 2.2% of transactions above the $10,000 threshold.
- Created GitHub repo `audittrail-lakehouse` (public, empty init), pushed generator + README + `.gitignore` + `requirements.txt` in one commit.
- Verified the live repo directly: 4 files present, `data/` correctly absent from tracked files.

**🏗️ Architectural Decisions & Key Concepts**
- **Account pool (500) fixed at generation time, sampled by transactions** — ties Day 1's dataset directly to Day 6's `account_snapshots` population instead of producing two disconnected synthetic datasets.
- **Customer identity (`customer_name`, `ssn_last4`) keyed per `account_id`, looked up per transaction rather than regenerated per row** — an account needs one stable identity for SCD Type 2 (Day 6) to mean anything; regenerating identity per row would make "account history" incoherent.
- **Amount distribution: log-normal (μ=6.0, σ=1.6), clipped to [0.01, 1,000,000], not uniform** — uniform sampling across that range would put ~99% of transactions above $10,000, making Day 12's SAR high-value flag meaningless. Verified empirically at 2.2% above threshold — a real minority signal instead.
- **`event_timestamp`/`ingestion_timestamp` given a small realistic lag (seconds–2hrs) today, not backdated** — late-arrival skew is deliberately reserved for Day 7's chaos scenario; Day 1's dataset is the clean baseline.
- **`transaction_id` as UUID4, not sequential** — mirrors production-style surrogate keys rather than implying meaningful ordering.
- **Seeded RNG (`Faker.seed(42)`, `random.seed(42)`) + pinned `faker==40.36.0` in `requirements.txt`** — validated today: an independent test run and the local Windows run produced byte-for-byte identical output.
- **`data/` excluded via `.gitignore`** — raw data, even synthetic PII-shaped fields, never enters version control; it's a regenerable build artifact, not a repo asset.
- **Key concept (Free Edition):** workspace is Unity-Catalog-enabled by default; serverless is the only compute path — no cluster sizing/config decisions exist in this tier.

**⚠️ Technical Challenges & Troubleshooting**
None today — Databricks compute attach, the generator run, and the repo push all succeeded on the first attempt. The one thing worth flagging isn't a failure but a near-miss caught before it shipped: an initial uniform amount distribution would have silently broken Day 12's SAR threshold logic. Caught by testing the actual output distribution before handing the script over, rather than trusting the range constraint alone.
