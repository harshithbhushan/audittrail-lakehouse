## Session 1 — 2026-08-04 — Environment Setup + Synthetic Data Generation

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


## Session 2 — 2026-08-04 — Data Contract (Schema + Quality Enforcement)

**Goal**
Write and verify a data contract for the transactions schema: pass on clean data, fail with a specific field-level error on corrupted data.

**Outcome**
Go, plus one real correction to Day 1's record. Contract passes 28/28 checks against `transactions.csv` and 29/29 against `transactions.json`. A deliberately corrupted record (lowercased `currency`) correctly produces exactly one failing check, isolated to the `currency` field, with exit code 1. Along the way, found and fixed a genuine reproducibility bug in Day 1's generator — see below.

**Actions**
- Installed `datacontract-cli`; discovered the tool now natively uses the **Open Data Contract Standard (ODCS) v3.1.0**, not the legacy `models`/`fields` format the original roadmap assumed. Built `datacontract.yaml` against current ODCS syntax (`schema`/`properties`/`logicalTypeOptions`).
- Ran `datacontract test` against clean CSV data: 28/28 checks passed.
- Corrupted one record's `currency` (`USD` → `usd`), reran: 27 passed, 1 failed, isolated to the currency regex check, non-zero exit code — confirmed this is what Day 10's CI step will key off of.
- Extended the contract with a second server pointing at `transactions.json`, to make that previously-unused file actually earn its place in the repo.
- JSON testing initially failed structurally through three attempts (bare array misread as a dict, an object-wrapped array still misread, JSON Lines misread) before finding the actual fix: the local JSON connector needs an explicit `delimiter: array` key, not documented in the quickstart.
- Once readable, JSON validation surfaced a real data quality gap CSV had never caught: `event_timestamp`/`ingestion_timestamp` lacked timezone info, failing JSON Schema's strict `date-time` format check — a check that never ran against CSV, since that column is typed `VARCHAR` there.
- Traced the root cause to `generate_transactions.py` using unseeded `datetime.now()` for timestamps — meaning Day 1's "byte-for-byte identical" reproducibility claim was only ever verified at the aggregate-stats level (row count, uniqueness, amount distribution), never as a full file diff. Corrected that record here rather than silently editing yesterday's entry.
- Fixed the generator: replaced `datetime.now()` with a fixed, explicit UTC `GENERATION_ANCHOR`. Regenerated data — amount/currency/account stats came back identical (same seed, same random call order, unaffected by the change), timestamps now genuinely reproducible and timezone-aware.
- Reverified both servers end to end: CSV 28/28, JSON 29/29.

**🏗️ Architectural Decisions & Key Concepts**
- **ODCS v3.1.0, not the legacy Data Contract Specification** — a real deviation from the roadmap, forced by an upstream tool change, not a design choice. Documented so future-me doesn't wonder why the file doesn't match the roadmap's original example.
- **`exclusiveMinimum: 0` (not `minimum: 0`) on `amount`, `pattern: "^[A-Z]{3}$"` (not just length) on `currency`** — both map to the contract's literal rule ("amount > 0", "3-letter currency"), not an approximation of it.
- **Different backends enforce different depths of checking** — CSV is validated only through DuckDB/SQL field checks; JSON gets that same layer *plus* a separate whole-file JSON Schema pass. "The contract passed" isn't a uniform guarantee across formats without knowing which engine ran it — a genuinely useful thing to be able to say in an interview.
- **`GENERATION_ANCHOR`: a fixed, explicit UTC datetime, replacing `datetime.now()`** — true reproducibility requires every input to generation to be seeded or fixed, not just calls that go through the `random` module. Wall-clock time is an easy one to miss.
- **Multi-server contract (`local_csv` + `local_json`)** — not added for its own sake; direct response to realizing a committed file had no actual role in the pipeline.

**⚠️ Technical Challenges & Troubleshooting**
Real ones today, unlike Day 1.
1. Windows blocked `pydantic-core`'s compiled DLL on first install (`Application Control policy has blocked this file` — Smart App Control flagging an unsigned native extension). Resolved on a clean retry; exact cause unconfirmed (likely a reputation-check timing issue on Microsoft's side), noted as a watch-item if `duckdb` or `pyarrow` hit the same wall later.
2. The local JSON connector's `delimiter: array` requirement isn't in the quickstart docs — found by testing three structural hypotheses (bare array, wrapped object, JSON Lines) against the actual error messages before landing on the right config key.
3. Found and fixed a real correctness bug carried over from Day 1: `datetime.now()` wasn't covered by the random seed, so timestamps were never actually reproducible across runs, despite yesterday's log claiming otherwise. Root-caused and corrected today.