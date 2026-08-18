# LOG

## Session 1 — 2026-08-04 — Environment Setup + Synthetic Data Generation (Roadmap Day 1)

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


## Session 2 — 2026-08-04 — Data Contract (Schema + Quality Enforcement) (Roadmap Day 2)

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


## Session 3 — 2026-08-05 — Bronze Layer + PII Masking (Roadmap Day 3)

**Goal**
Ingest the Day 1 CSV into a governed Bronze Delta table with PII masked before write, and confirm Delta's version history exists.

**Outcome**
Go. `workspace.bronze.transactions` live, 10,000/10,000 rows written, masking verified deterministic across two independent environments, first `DESCRIBE HISTORY` entry confirmed (version 0, `numOutputRows: 10000`).

**Actions**
- Set up `workspace.landing.raw_files` as a Volume separate from `workspace.bronze` — a distinct "arrived, unprocessed" zone from the governed layer. Uploaded `transactions.csv` via the workspace UI (Free Edition restricts outbound internet access, so UI upload is required, not a code-based fetch).
- Verified the masking transformation logic locally with plain PySpark before running it against Databricks (Delta's JVM jar wasn't reachable from the sandbox's network policy, so the Delta write itself couldn't be tested there — the masking logic, the part with actual bug risk, could and was): confirmed explicit-schema read matches contract types, zero data loss, zero nulls introduced, and hash determinism (same `account_id` → same hashed `customer_name` across every transaction for that account).
- Ran the real notebook against Databricks: explicit schema on read, `customer_name` → `sha2(..., 256)`, `ssn_last4` → partial mask (`XX` + last two digits), `account_id` untouched, written to `workspace.bronze.transactions` as Delta.
- Cross-checked two accounts' masked `customer_name` hashes between the local test run and the live Databricks table — byte-for-byte identical, confirming the masking is genuinely deterministic across two separate environments, not just "ran once and looked right."
- Ran `DESCRIBE HISTORY`: version 0, `CREATE OR REPLACE TABLE AS SELECT`, `numOutputRows: 10000` (exact match to source), `isBlindAppend: false`.

**🏗️ Architectural Decisions & Key Concepts**
- **Landing zone (`workspace.landing.raw_files`) kept separate from `workspace.bronze`** — "arrived" and "governed" are different states; worth being able to point at two distinct places, not one folder doing both jobs.
- **`sha2()` as a native Spark SQL function, not a Python UDF wrapped around `hashlib`** — avoids crossing the JVM↔Python serialization boundary row by row; the function runs natively inside the same query plan as everything else.
- **Two different masking techniques for two different needs** — `customer_name` gets a one-way hash (identity never needs recovering, just consistent matching); `ssn_last4` gets a partial mask (`XX` + last two), keeping enough utility for fraud/ops pattern-matching without exposing the real value. One technique doesn't fit both jobs.
- **`account_id` left unmasked** — internal surrogate key, not PII on its own, and every join in this project depends on it.
- **Explicit schema on read, not `inferSchema`** — matches Day 2's contract types exactly instead of letting Spark guess from a sample.
- **`DESCRIBE HISTORY`'s `numOutputRows` used as an independent data-loss check**, not just confirmation the write didn't error.

**⚠️ Technical Challenges & Troubleshooting**
Couldn't fully test the Delta write + `DESCRIBE HISTORY` mechanics ahead of time — the environment used to verify code before handing it over couldn't reach Maven Central to resolve Delta's JVM dependency. Worked around it by isolating what actually had bug risk (the masking transformation logic) and testing that with plain Spark, while treating the Delta write itself as low-risk native platform behavior. Confirmed correct once run against the real workspace — but worth naming honestly as a case where full pre-verification wasn't possible, unlike Days 1 and 2.


## Session 4 — 2026-08-05 — Silver Layer + Dead Letter Routing (Roadmap Day 4)

**Goal**
Add a validation layer on top of Bronze that separates valid transactions from invalid ones, with a documented reason for every rejection, instead of silently dropping or failing on bad records.

**Outcome**
Go. `workspace.silver.transactions` and `workspace.silver.transactions_dead_letter` both exist and are independently queryable. Today's result is 10,000 valid / 0 dead letter — the correct outcome given clean baseline data, not an untested code path; the routing logic itself was separately verified against deliberately injected bad records before running against the real table.

**Actions**
- Created the `workspace.silver` schema.
- Verified the validate/split logic locally before running it for real: injected three deliberately bad rows into a local test copy (null amount, negative amount + lowercase currency simultaneously, null transaction_id) and confirmed correct routing — including that a record failing two rules at once gets tagged with both reasons (`"amount_invalid; currency_invalid"`), not just the first one matched.
- Ran the real notebook: read `workspace.bronze.transactions`, built a per-rule reason array (amount not null/>0, currency matches `^[A-Z]{3}$`, transaction_id not null), concatenated multi-rule failures, split on reason count, wrote both as Delta tables.
- Hit and fixed a real copy-paste bug: a backslash line-continuation broke when pasted into the Databricks notebook cell (trailing whitespace after `\` is a silent syntax error). Rewrote using parenthesized multi-line chaining instead — not sensitive to trailing whitespace — and reverified identical behavior before handing the fix back.
- Confirmed final counts: total 10,000, valid 10,000, dead_letter 0. Independently confirmed both tables queryable via SQL.

**🏗️ Architectural Decisions & Key Concepts**
- **Silver validation is a different layer than Day 2's data contract, not a repeat of it.** The contract is a boundary check on the raw file — one pass/fail signal, before the data is trusted at all. Silver validation runs inside the pipeline and physically *routes* each record: valid data flows forward, invalid data is preserved with a documented reason, never silently dropped.
- **`rejection_reason` concatenates every rule a record fails**, not just the first match — a record wrong in two ways keeps both reasons on record for triage, rather than losing information to an arbitrary precedence order.
- **`silver.transactions` carries no `rejection_reason` column at all** (every row there passed by definition); only the dead-letter table carries it. Deliberately different schemas rather than one shared schema with a nullable/empty column on the valid side.
- **Backslash line continuation avoided in favor of parenthesized multi-line chains** — more robust to copy-paste across tools, since a backslash-continuation breaks silently on any trailing whitespace after it.

**⚠️ Technical Challenges & Troubleshooting**
Backslash line-continuation syntax broke on paste into the Databricks notebook cell — invisible trailing whitespace after `\` is a real, silent syntax error. Root-caused and fixed with parenthesized multi-line expressions; reverified the rewrite behaves identically before handing it back. Also worth stating plainly: today's 0 dead-letter records is the *expected* result of clean input, not an untested code path — the actual stress test is Day 5.


## Session 5 — 2026-08-08 — Chaos 1: Poison Pill (Roadmap Day 5)

**Goal**
Prove the dead-letter routing built in Session 4 actually catches bad data under a real chaos scenario — and along the way, fix a more fundamental gap discovered in the process: the pipeline had no way to distinguish an already-ingested batch from a new one.

**Outcome**
Go. Bronze correctly accumulated to 11,000 rows across two runs (append, not overwrite or duplicate). Silver's dead-letter routing caught all 150 poisoned records in the new batch (15.0%, matching the injected poison rate exactly) with correct rejection reasons, while the original 10,000 clean records stayed completely untouched.

**Actions**
- Reasoned through, before writing any code, what `.mode("overwrite")` on Bronze/Silver would do to a second batch, and separately what a naive wildcard-read "fix" would do — correctly concluded overwrite alone loses the prior batch, and overwrite+wildcard together would re-ingest already-processed files, landing at 21,000 rows instead of 11,000.
- Correctly reasoned that Silver should stay on `overwrite`, not follow Bronze to `append` — Silver's source is a table read (a snapshot of current state), not a file glob, so there's no "already seen this file" ambiguity. Every Silver run is meant to be a full, correct recompute of the current Bronze state, not an incremental accumulation.
- Fixed Bronze: added a `batch_file` notebook widget, pointed the read at that specific file, switched the write to `append`.
- Built a poisoned batch generator (1,000 new transactions, 15% with null or negative amount) reusing the exact same account identity mapping as batch 1, then reseeding separately so the new batch doesn't just replay batch 1's row-level pattern. Verified zero identity mismatches across the 441 accounts shared between batches.
- Verified the full two-batch append + Silver recompute flow locally (11,000 total, original 10,000 still all valid, 850/150 split on the new batch, 15.0% rejection rate) before running it for real.
- Ran it for real: Bronze landed at exactly 11,000 (confirmed via SQL `COUNT`). Silver's cumulative print matched exactly: 10,850 valid / 150 dead letter.
- Hit two real widget-related issues getting the batch-specific summary logging correct (see Technical Challenges) before it reported the right numbers.

**🏗️ Architectural Decisions & Key Concepts**
- **Batch ingestion needs an explicit signal for "which file is this run," not a folder glob.** A wildcard read can't distinguish already-processed files from new ones and will silently reprocess/duplicate. Solved with a notebook widget (`batch_file`) — Databricks' run-time parameter mechanism. The production-scale answer to this same problem is Auto Loader (`cloudFiles`), which tracks already-processed files via checkpointing automatically; not adopted here since it's more machinery than this project's scope needs, but worth naming if asked how this scales.
- **Silver correctly stays on `overwrite`.** Its source is a table read, not a file glob — no incremental-ingestion ambiguity exists there. Every run is a full, correct recompute of "the current split of everything in Bronze, right now."
- **Batch-specific summary logging via a left-semi join** against the batch's own `transaction_id`s — Silver's output tables aren't partitioned by batch, so isolating "this run's contribution" from the cumulative totals needs an explicit filter, not just a fresh read.
- **Poisoned batch generator reuses the exact same account identity construction** (same seed, same order) as the original generator, then explicitly reseeds before generating batch-specific values — keeps masked identities consistent across batches while avoiding an accidental replay of batch 1's row-level pattern.

**⚠️ Technical Challenges & Troubleshooting**
1. `dbutils.widgets.text(name, default)` only sets a default the *first* time a widget is created — rerunning the same line with a new default value silently does nothing once the widget already exists. First attempt to fix the batch summary by editing the widget's default value in code didn't work, for exactly this reason. Root-caused and fixed by changing the value directly in the widget UI (the reliable fix), with `dbutils.widgets.remove()` + a later rerun as the code-only alternative.
2. Widgets are scoped **per notebook**, not shared globally — setting `batch_file` correctly in Bronze had zero effect on Silver's separate widget of the same name, even though Bronze's own run was already correct. Diagnosed by noticing the mismatched filename embedded directly in the log output, not by guessing.


## Session 6 — 2026-08-11 — SCD Type 2 via Delta MERGE (Roadmap Day 6)

**Goal**
Implement SCD Type 2 for account-level attributes (status, credit limit) via Delta `MERGE INTO`, enabling the point-in-time auditability that Type 1 and Type 3 can't provide.

**Outcome**
Go. `workspace.gold.account_history` built via a two-phase load (initial write, then `MERGE` for the update batch). 600 total rows after the update — 500 current, 100 historical — exactly matching the predicted split, verified in aggregate across all 100 changed accounts, not just one example. `VERSION AS OF 0` independently confirms the roadmap's time-travel check (returns 500, the pre-MERGE state, untouched).

**Actions**
- Worked through, before any code, why Type 1 fails for this table specifically: a concrete auditor scenario (a $9,000 transaction against a July credit limit later overwritten) showed Type 1 doesn't just lose history abstractly — it lets an already-happened transaction retroactively look compliant or non-compliant depending on whatever value happens to be current today, with no error or warning that anything's wrong.
- Also reasoned through why Type 3 fails here too: it only survives one change per attribute (current + previous columns), and this project already has multiple batches arriving over time — a second credit review isn't hypothetical. Type 3 would silently lose the same information Type 1 does, just one hop later.
- Built `generate_account_snapshots.py`: two flat files (initial state, updated state) for the same 500-account population, exactly 20% (100 accounts) genuinely changed — one attribute per account, credit limit drawn from fixed tiers rather than a continuous range, for realism and easy diffing during verification.
- Deliberately kept `valid_from`/`valid_to`/`is_current` out of the raw files — those are pipeline-managed SCD columns, not source data; mixing them in would blur that distinction.
- Loaded the initial batch as a plain write (no `MERGE` needed — table doesn't exist yet on the first load).
- Implemented the update batch via the double-staged-row `MERGE` pattern: every changed account duplicated into a real-`merge_key` row (closes the old current row via `WHEN MATCHED`) and a null-`merge_key` row (forces an `INSERT` of the new current row via `WHEN NOT MATCHED`, since `NULL` never matches anything). Verified the staged dataframe construction and the expected final state locally — this environment can't execute real Delta `MERGE`, so this is where actual bug risk lived and where verification focused — before running the real thing.
- Ran it for real: total 600, current 500, historical 100 — exact match. `ACC00002` traced correctly: closed row (`active`/2500, `valid_to = 2026-08-11`) and new current row (`frozen`/2500, `valid_from = 2026-08-11`), closed-open boundary, no gap or overlap.

**🏗️ Architectural Decisions & Key Concepts**
- **Type 2, not Type 1 or Type 3.** Type 1 lets an already-happened transaction retroactively look compliant based on today's value, silently. Type 3 only survives one change per attribute, and this project already proves multiple changes happen over time.
- **Raw snapshot files carry only `account_id`/`account_status`/`credit_limit`/`effective_date`** — `valid_from`/`valid_to`/`is_current` are pipeline-assigned metadata, not source data.
- **Credit limits drawn from fixed tiers, not a continuous range** — matches how real credit policies actually assign limits, and makes "did this actually change" trivially checkable.
- **`valid_to` uses a sentinel (`9999-12-31`), not `NULL`, for current rows** — avoids `NULL`-comparison footguns in range queries; "what was true on date X" stays a uniform query whether the row is current or historical.
- **Closed-open interval convention** (`valid_from` inclusive, `valid_to` exclusive) — the closed row's `valid_to` and the new row's `valid_from` share the same date with no gap or overlap, confirmed directly on `ACC00002`.
- **Double-staged-row `MERGE` pattern** — a single `MERGE` clause can only take one branch (`UPDATE` or `INSERT`) per matched pair; duplicating each changed row lets one statement correctly close an old row and insert a new one for the same logical account in a single pass.
- **`whenMatchedUpdate` scoped to `is_current = true`** — without it, a later `MERGE` run could match against an already-historical row for the same account and update the wrong one.

**⚠️ Technical Challenges & Troubleshooting**
Same limitation as Session 3: couldn't execute a real Delta `MERGE` in the verification environment (Maven Central unreachable). Worked around it by verifying everything that feeds into the `MERGE` — the staged dataframe construction and change-detection logic — plus computing the exact expected final row counts by hand, to check the real Databricks run against rather than trusting it blind. Minor: left a broken draft line in a local test script (a DataFrame passed where a Column was expected); caught immediately by the resulting stack trace, never made it into the handed-over notebook code.


## Session 7 — 2026-08-12 — Chaos 2: Late Arrival + SCD2 Unification (Roadmap Day 7)

**Goal**
Handle a late-arriving account snapshot record — one whose `effective_date` falls before the current row's `valid_from` — without corrupting the account's current state or breaking chronological ordering, and unify that fix with Session 6's normal-update `MERGE` into a single statement that correctly routes any incoming batch, whether it's late, normal, or a mix of both. Adapted from the roadmap's original framing (late *transaction*, `event_timestamp` vs `ingestion_timestamp`) to where late-arrival logic actually applies in this build: `account_history`'s SCD2 `MERGE`, since `transactions` is append-only with no ordering-sensitive logic at all.

**Outcome**
Go, fully verified from a clean, reproducible state. `workspace.gold.account_history` rebuilt from scratch and taken through three checkpoints: initial load (500 rows), the unified `MERGE` run alone against Session 6's original batch — reproducing that session's exact original result (600 total / 500 current, a genuine regression check) — and then run against a genuinely mixed batch of 15 late-arriving and 15 normal-forward changes (630 total / 500 current, zero chronological gaps or overlaps across every affected account).

**Actions**
- Flagged and documented the adaptation before building anything: the roadmap's Day 7 framing assumes a pipeline where transaction arrival order affects `MERGE` logic, but `transactions` in this build is append-only with nothing ordering-sensitive about it. The actual late-arrival risk lives in `account_history`'s SCD2 `MERGE` from Session 6.
- Reasoned through, before any code, exactly what Session 6's unchanged `MERGE` would do if run against a late-arriving record: traced the actual field values and found it would silently overwrite the current row's `valid_to` with a date earlier than its own `valid_from` (a chronologically impossible negative-duration window) and incorrectly flag the older, late-arriving fact as `is_current = true` while marking the real current state `false`.
- Built and verified the late-arrival fix in isolation first: detection (`effective_date < current row's valid_from`), a search across an account's *full* history (not just its current row) for which existing window actually contains the late date, and a shrink-plus-insert pair that splits that window correctly.
- Recognized, while reviewing whether Session 6's original cell was now obsolete, that it wasn't — but that neither cell alone could handle a batch containing *both* late and normal-forward changes correctly. Session 6's logic run against a late record reproduces the exact corruption just diagnosed; the late-arrival logic run against a normal record silently drops it, no error.
- Recognized that Session 6's original `merge_key`/`NULL` trick and the late-arrival fix's natural `(account_id, valid_from)` matching weren't fundamentally different techniques — the current-row close action can be matched the same way, since its `valid_from` is already known from the join used to detect changes. Unified both under one consistent matching scheme instead of running two mechanisms side by side.
- Built a genuinely mixed test batch (15 late, 15 normal, shuffled together in one file) and verified the unified routing logic locally before handing it over — correct 15/15 split, a late no-op case correctly filtered out, zero gaps or overlaps in the resulting simulation. Caught and corrected an arithmetic error in that verification (predicted 660 rows, correct value 630 — shrink actions modify existing rows in place, they don't add new ones) before it reached the handoff.
- Discovered, once run for real, that the live table's actual state (650) didn't match the clean-rebuild simulation (630) — traced to the standalone late-arrival cell having already executed against the real table before being replaced by the unified version. Deleting code from a notebook doesn't undo what it already committed to Delta; the table's true state was path-dependent on session history, not reproducible from committed code alone.
- Fixed with a full reset (`DROP TABLE`) and a from-scratch rebuild, verified across the three checkpoints above rather than trusted on a single number.

**🏗️ Architectural Decisions & Key Concepts**
- **Adapted the late-arrival exercise from transactions to `account_history`** — documented explicitly as a deviation, since the roadmap's framing assumed a table this project doesn't actually have ordering-sensitive logic on.
- **Late detection compares the incoming date against the current row's `valid_from` specifically**, not "today" or the batch's own date — what matters is whether this fact predates what's already on file as the newest known state.
- **Window search spans an account's full history, not just its current row** — a genuinely late fact can't post-date the current row's `valid_from` by definition, so it almost always belongs inside an already-closed historical window.
- **Unified matching scheme**: both "close the current row" and "shrink a historical window" match on the same `(account_id, valid_from)` composite key — replacing an original `merge_key`/`NULL` trick and separate natural matching with one consistent mechanism, once it became clear they were solving the same underlying shape.
- **Shrink actions collapse to one constant outcome** (`is_current = false`), regardless of whether the row being shrunk was the live current row or already historical — setting an already-false value to false again is harmless, and removes a per-row conditional that wasn't actually needed.
- **Late no-op filtering**: a late record whose values match what's already recorded for the window it falls into is correctly ignored, not just correctly split — prevents spurious historical rows that don't represent a real change.
- **One unified staged dataframe, one `MERGE` statement** — same atomicity reasoning as Session 6: a single commit, not multiple operations with a crash window between them.
- **A Delta table's actual state can silently diverge from what current notebook code would reproduce**, once cells have been run and later removed or replaced — deleting code doesn't undo prior writes. Verifying against a freshly reset table, not an accumulated one, is the only way to confirm a refactor is genuinely correct.

**⚠️ Technical Challenges & Troubleshooting**
1. `NameError: name 'col' is not defined` on the first real run of the late-arrival fix — Session 6's imports and `raw_schema` weren't in scope for this cell (either a fresh notebook that never ran Session 6's cells, or Free Edition's serverless compute detaching from inactivity and resetting previously-defined variables). Fixed by making the cell fully self-contained.
2. My own test prediction was arithmetically wrong during the unification verification (predicted 660, correct value 630) — caught and corrected before handoff, not after.
3. The live table's real state (650) diverged from the clean-rebuild simulation (630) because the original late-arrival cell had already run for real before being replaced — resolved with a full table reset rather than trying to reason about exactly what state the table was already in.


## Session 8 — 2026-08-12 — Chaos 3: Duplicates (Roadmap Day 8)

**Goal**
Handle duplicate `transaction_id`s at Silver via a `row_number()` window function, keeping the most recently ingested copy per ID, resolved before validation runs — not after.

**Outcome**
Go. Dedup correctly removed exactly 50 duplicate rows (12,050 → 12,000), keeping the corrected/later-ingested version in each case, verified directly on a concrete example before handoff. Silver's cumulative split (11,850 valid / 150 dead letter across all three batches ingested so far) and this batch's isolated summary (1,000/1,000 valid, 0 dead letter) both matched exactly on the real run.

**Actions**
- Designed duplicates with a genuinely different `amount` on the resent copy, not an identical row — makes "keep the latest by `ingestion_timestamp`" resolve a real business decision (a correction), not arbitrary tie-breaking between two identical copies.
- Caught and fixed a real design flaw in the generator before testing further: initially used placeholder `customer_name`/`ssn_last4` values, which would have broken the masked-identity consistency established since Session 1 (the same `account_id` must always mask to the same hash, everywhere). Fixed by reusing the exact identity-map construction every other generator in this project uses.
- Verified locally before handoff: dedup correctly resolves a concrete duplicate pair (kept the later-ingested, corrected amount), and unique `transaction_id` count post-dedup exactly equals row count — nothing over-collapsed.
- Placed the dedup step before validation in the Silver cell — a row's validity is only a meaningful question once there's exactly one row per `transaction_id` to evaluate.
- Made a real prediction error handing this off: forgot Session 5's poisoned batch (1,000 rows) when reconstructing Bronze's cumulative state locally, predicting 11,050 instead of the correct 12,050. Caught when the actual Bronze count came back higher — owned the error immediately rather than assuming the run was wrong, recomputed correctly, and the corrected prediction matched every subsequent real number exactly.
- Ran it for real: dedup before/after matched exactly, cumulative Silver split matched exactly, batch-specific summary matched exactly — duplicates were this batch's only issue, nothing else violated a validation rule.

**🏗️ Architectural Decisions & Key Concepts**
- **Duplicates carry a genuinely different value on the resent copy**, not an identical row — makes the ordering choice a real decision with business meaning, not arbitrary tie-breaking.
- **Dedup runs before validation, not after or in parallel** — a row's validity is only meaningful once there's exactly one row per `transaction_id`.
- **`row_number()` over `Window.partitionBy("transaction_id").orderBy(col("ingestion_timestamp").desc())`, keep `rn == 1`** — standard, defensible "most recent wins" pattern.
- **Reasoning about cumulative multi-batch pipeline state requires enumerating every batch actually ingested so far, not recalling from memory** — a real error caught in this session, worth remembering as the pipeline keeps accumulating more batches over the remaining days.

**⚠️ Technical Challenges & Troubleshooting**
Real one, on my end this time: predicted Bronze's cumulative count incorrectly (11,050 instead of 12,050) by reconstructing state from only two of the three batches already actually ingested. Caught immediately when the real result didn't match the prediction, recomputed correctly, and the corrected number matched the subsequent real Silver run exactly — a reminder that "trust the system's actual output over a memorized or reconstructed prediction" applies to me as much as to the pipeline itself.


## Session 9 — 2026-08-13 — Great Expectations Suite for `silver.transactions` (Roadmap Day 9)

**Goal**
Build a Great Expectations suite for `silver.transactions` (not-null on `transaction_id`/`account_id`/`amount`, amount range, currency regex, `transaction_id` uniqueness), confirm it passes on clean data, fails correctly on broken data, and can be configured to raise on failure.

**Outcome**
Go. Suite verified against the real Databricks table: clean run returns `success: True`; a deliberately broken in-memory copy (negative amount, lowercase currency — never written back to the real table) correctly returns `success: False` with exactly the two expected expectation types failing, and the configured raise fires with a clear message.

**Actions**
- Verified current GX 1.x Fluent API directly by installing and testing locally before writing any Databricks code (the roadmap itself flags this as a gotcha area) — confirmed exact expectation class names and the full suite → validation definition → checkpoint → Data Docs flow against real project data before handing anything over.
- Explained the three-way distinction between this, Day 2's contract, and Day 4's Silver validation before building anything: contract = boundary pass/fail on the raw file; Silver = in-pipeline routing that preserves both valid and invalid records; GX = documentation/observability, an accumulating, browsable history of what was checked, for an audience that never reads the pipeline code.
- Hit and resolved a real syntax/environment issue: `%pip install`, not plain `pip install`, is required in a notebook cell — and the cell has to actually be in Python mode. Databricks cells inherit the language of the cell immediately above them when newly inserted, which silently defaulted several new cells to SQL (inherited from Cell 1) until `%python` was added explicitly.
- Hit and diagnosed a genuine platform incompatibility: GX persists (caches) the Spark DataFrame by default as a performance optimization across its several expectation evaluations, but Databricks Serverless disallows all persist/cache operations entirely, by design. Found the confirmed fix directly from a Great Expectations engineer on GX's own support forum, responding to an identical reported error: pass `persist=False` when creating the Spark datasource.
- Hit and fixed a second real issue: GX's `FileDataContext` persists its configuration to disk, so rerunning a cell that uses plain `.add(...)` for a datasource, suite, validation definition, or checkpoint fails on the second run with "already exists" — inevitable given how many times this cell got rerun while debugging the two issues above. Rewrote the whole cell to use `add_or_update` throughout (confirmed to exist on every relevant GX collection by inspecting the installed library directly), plus a small get-or-add helper for the two objects that don't have their own `add_or_update`. Verified idempotent across three consecutive runs locally before handing it back.
- Ran the corrected cell for real: clean data → `success: True`. Deliberately corrupted copy (negative amount, lowercase currency) → `success: False`, exactly the two expected expectation types failed, configured raise fired correctly.

**🏗️ Architectural Decisions & Key Concepts**
- **Great Expectations is documentation/observability, not a third redundant validation gate** — distinct purpose from Day 2's contract (boundary check) and Day 4's Silver logic (in-pipeline routing).
- **`persist=False` on the Spark datasource** — required specifically because Databricks Serverless disallows all DataFrame caching/persist operations by design; GX's default caching optimization collides with that restriction.
- **`add_or_update` used throughout instead of `add`**, for every GX resource — a persistent `FileDataContext` remembers everything across notebook reruns, unlike an Ephemeral context; idempotent creation is the only way to make a GX setup cell safely rerunnable.
- **"Configure a failed check to raise" is implemented in the calling code, not inside GX itself** — `Checkpoint.run()` only reports a `.success` boolean; deciding whether that should actually halt something (a notebook, later a CI job) is the caller's responsibility, which is exactly what Day 10's CI integration will need.
- **GX file context stored in a Unity Catalog Volume** (`workspace.landing.gx_store`), not legacy DBFS — consistent with the Free Edition pattern established since Day 3.

**⚠️ Technical Challenges & Troubleshooting**
1. `%pip install` (not `pip install`) required in a notebook cell, and new cells inherit the language of the cell immediately above them in Databricks — several cells silently defaulted to SQL until `%python` was added explicitly.
2. `PERSIST TABLE is not supported on serverless compute` — a genuine, documented GX+Databricks-Serverless incompatibility. Fixed with `persist=False`, confirmed via GX's own official support forum rather than guessed at.
3. `DataContextError: ... already exists` — a persistent `FileDataContext` doesn't allow plain `.add()` twice. Fixed by rewriting to `add_or_update` throughout, verified idempotent across repeated runs before handoff.
4. `OSError: [Errno 5] Input/output error` when zipping Data Docs directly onto the Volume — a documented Databricks limitation, not something specific to this setup: Unity Catalog Volumes don't support direct-append or non-sequential (random) writes, and zip creation needs to seek backward to update its central directory after each entry. Fixed per Databricks' own official guidance: write the archive to local disk (`/tmp`) first, then copy the finished file onto the Volume as a single sequential write.


## Session 10 — 2026-08-13 — CI/CD (Roadmap Day 10)

**Goal**
Automate validation via GitHub Actions: on every push to `main`, regenerate sample data, run the data contract test and the Great Expectations suite, failing the build if either fails — and prove it with a genuine broken-schema-to-red, fixed-to-green cycle, not just a working pipeline that's never actually been shown to fail correctly.

**Outcome**
Go. Workflow live, badge green. A deliberately broken currency pattern correctly produced exit code 1 specifically on the `Run data contract test` step (10,000/10,000 rows correctly flagged), and reverting it produced a clean green run across all three steps (generation, contract, GX suite).

**Actions**
- Adapted the CI validation approach for the fact that GitHub Actions runners have no Databricks/Spark access — CI regenerates a fresh, deterministic sample via `generate_transactions.py` and validates that, rather than the live Silver table. A legitimate, common pattern (validating against fixture data because production isn't reachable from ephemeral runners), not a shortcut.
- Built a CI-specific Great Expectations script reusing the exact expectations from Session 9, simplified: ephemeral GX context and plain `pandas`, since none of Session 9's Databricks-specific complexity (Spark, `persist=False`, `FileDataContext` persistence, `add_or_update`) applies to a runner that starts fresh every single run.
- Verified the full CI sequence locally before it touched GitHub: generate → contract test → GX script, on both clean and deliberately broken data, confirming correct exit codes at each step — caught and corrected my own mistake in the process (piping `datacontract test`'s output through `tail` was swallowing its real exit code; re-verified without the pipe).
- Real bug surfaced independent of anything built this session: generator scripts got reorganized into a `scripts/` folder mid-session, breaking the workflow's hardcoded path. Diagnosed and fixed, with an explanation of why the `data/` output path itself didn't also need to move — relative paths resolve against the process's working directory, not the script file's own location.
- First attempt at the red/green demonstration produced a red run for the *wrong* reason: the path bug and the deliberately-broken contract landed across an overlapping sequence of commits, so the run that failed, failed at the generation step (missing file), never reaching the contract test at all. Recognized this didn't actually satisfy the point of the exercise — a red run only proves something if it's red for the reason being tested — and redid the cycle cleanly once the path issue was genuinely resolved.
- Confirmed the redone cycle correctly: broken run failed specifically on the contract test step; reverted run passed all three steps cleanly.

**🏗️ Architectural Decisions & Key Concepts**
- **CI validates a freshly generated deterministic sample, not the live Databricks table** — GitHub Actions runners have no Spark/Databricks access; a legitimate fixture-data pattern, not a compromise.
- **CI's GX script is deliberately simpler than Session 9's Databricks version** — the complexity in Session 9 was specifically about Databricks Serverless and persistent notebook state, neither of which exists on a runner that starts fresh every run.
- **A red CI run only counts as a valid demonstration if it's red for the reason being tested** — an incidental failure that happens to coincide with an intentional one doesn't prove the check works.
- **Git/CI history left as-is, not cleaned up after resolving the path bug** — an honest record of real engineering work reads as legitimate, not messy; rewriting published history is reserved for actual leaked secrets, not tidiness.

**⚠️ Technical Challenges & Troubleshooting**
1. My own exit-code verification was wrong once: piping `datacontract test`'s output through `tail` meant `$?` captured `tail`'s exit code, not `datacontract`'s — made a real failure look like exit code 0. Caught by re-running without the pipe.
2. Workflow broke on a hardcoded script path after generators got moved into a `scripts/` folder mid-session — diagnosed and fixed in one line, with the underlying reasoning (working-directory-relative paths) explained so the same class of issue is self-diagnosable next time.
3. The first red/green demonstration attempt was invalid — red for an incidental reason, not the intended one — because two separate issues got resolved in an order that let them overlap. Caught before accepting it as sufficient; redone cleanly.


## Session 11 — 2026-08-14 — dbt Setup + Staging (Roadmap Day 11)

**Goal**
Confirm dbt-databricks connectivity works on Free Edition (the roadmap's explicit "auth spike" — stop and adapt if it fails), then build thin staging models (`stg_transactions`, `stg_accounts`) on top of the already-governed Silver/Gold tables, with no re-validation or business logic.

**Outcome**
Go, and the auth spike wasn't a formality — Free Edition genuinely cannot create a new SQL Warehouse (confirmed via research before attempting), but does provide a pre-provisioned Serverless Starter Warehouse that works fine. `dbt debug` returned a clean connection test; `dbt run` built both staging models successfully (`PASS=2, ERROR=0`).

**Actions**
- Researched Free Edition's SQL Warehouse constraints before attempting connection, given the roadmap's explicit warning about this step — confirmed via a Databricks Community thread and a dedicated Free-Edition+dbt-Core walkthrough that new warehouse creation is disabled, but a pre-provisioned Serverless Starter Warehouse is available and PAT auth is supported.
- Built the full dbt project structure locally (`dbt_project.yml`, `sources.yml` declaring `silver.transactions` and `gold.account_history`, both staging models) and verified it with `dbt parse` against a dummy, non-connecting profile — confirmed correct Jinja/YAML/`source()` resolution before handing anything over, since a live connection test wasn't possible without real credentials.
- Made two real design decisions in the staging models, not just a passthrough: renamed `event_timestamp`/`ingestion_timestamp` to a `transacted_at`/`ingested_at` `_at`-suffix convention, and cast `valid_from`/`valid_to` from string to real `date` (a side effect of Session 6's string-literal sentinel dates in the MERGE logic) so every downstream consumer gets a proper type without re-casting independently.
- Emphasized, given this repo is public, that `profiles.yml` (holding the real PAT) must live entirely outside the git repo (`~/.dbt/profiles.yml`), never inside it.
- Diagnosed a real early failure precisely: `dbt debug` hung and failed trying to resolve a hostname that was literally the unfilled placeholder text, angle brackets and all — caught via the URL-encoded `%3c`/`%3e` characters visible in the actual error message, not guessed at.
- Diagnosed a recurring but non-blocking PowerShell issue: relative `cd` landed in unexpected nested folders twice, since the shell's working directory carried over from wherever a prior command left it. Confirmed this never actually broke dbt itself, since dbt walks upward through parent directories looking for `dbt_project.yml`, the same way git looks for `.git`.
- Confirmed for real: `dbt debug` → `Connection test: [OK connection ok]`, `All checks passed!`. `dbt run` → both `stg_transactions` and `stg_accounts` built as views, `PASS=2, ERROR=0`.

**🏗️ Architectural Decisions & Key Concepts**
- **Free Edition's SQL Warehouse is a pre-provisioned Serverless Starter Warehouse, not something you create** — a real, documented Free Edition constraint, not a workaround.
- **`profiles.yml` lives outside the repo entirely, never committed** — credential hygiene that matters specifically because this is a public repo.
- **Staging models read from Silver/Gold, not Bronze — "no re-ingestion"**: this data already passed the Day 2 contract and Day 9 GE gates; re-validating it in staging would be redundant, not extra-safe. Matches the realistic enterprise pattern of a downstream analytics team building on governed data they didn't produce.
- **Renames follow a consistent `_at` suffix convention for timestamps** — a real analytics-engineering naming pattern, not just relabeling.
- **`valid_from`/`valid_to` cast from string to date once, in staging** — every model built on top gets a correct type automatically instead of independently re-casting.
- **dbt walks upward looking for `dbt_project.yml`, same as git looks for `.git`** — explains why a wrong working directory doesn't actually break dbt commands, even when it looks alarming in the output.

**⚠️ Technical Challenges & Troubleshooting**
1. `dbt debug` initially hung, then failed — root cause was `profiles.yml` still containing literal template placeholders, not filled in with real values. Diagnosed precisely via the URL-encoded angle brackets visible in the DNS resolution error.
2. Relative `cd` commands landed in unexpected nested directories twice, since PowerShell's working directory persisted from wherever a prior command left it. Non-blocking (dbt found the right `dbt_project.yml` regardless), but worth using absolute paths going forward.
3. Python 3.14 + Pydantic v1 compatibility warning noted, non-blocking so far — flagged to watch if something unrelated breaks later.


## Session 12 — 2026-08-14 — Marts + SCD2 Second Way (dbt snapshot) + Tests (Roadmap Day 12)

**Goal**
Build the `fct_transactions_daily` mart and its tests, then implement a second, structurally different way to do SCD2 (dbt snapshot) specifically to make an explicit, defensible comparison against the custom Delta `MERGE` from Days 6–8 — required to articulate the tradeoff myself before any snapshot code was shown, per the original project instructions for this specific day.

**Outcome**
Go. Mart + tests: 4 models built (3 staging views + 1 marts table), 13/13 `dbt test` passing (12 generic + 1 custom). Snapshot: built using dbt's current YAML-based syntax, 500 rows confirmed on first run. Comparison paragraph written for the README, grounded in the actual verified mechanics of both approaches, not a generic explanation.

**Actions**
- Reasoned through, unprompted, what a dbt snapshot would have done with Session 7's late-arriving credit review record — initial answer was "I don't know," which is fine and expected; this wasn't a first-principles-derivable mechanism the way append-vs-overwrite was. Walked through the actual mechanism concretely: a snapshot only knows "did this differ from what I last recorded," stamping `dbt_valid_from` with the run's own timestamp, with zero awareness of a business-effective date — meaning the exact corruption diagnosed in Session 7 wouldn't get fixed by a snapshot, it would simply never get flagged as wrong, silently recording the correction as if it happened at run time.
- Built `fct_transactions_daily`: daily grain, transaction count/total/average, plus a high-value count and percentage at the $10,000 SAR threshold established back on Day 1.
- Built `stg_accounts_current` — current-state-only, one row per account — needed for two reasons: a clean `relationships`-test target (`stg_accounts` carries full SCD2 history, the wrong shape for a foreign-key check), and later reused as the snapshot's actual input.
- Added 13 dbt tests: `not_null`/`unique`/`accepted_values`/`relationships` across the staging models and the mart, plus one custom singular SQL test (no negative amounts reaching the mart, checked at `stg_transactions` specifically so a failure points at an exact `transaction_id`, not just a suspicious aggregate).
- Hit and fixed a real generic-test syntax deprecation: dbt 1.12 requires test arguments nested under an `arguments:` property, not as top-level keys — caught via a deprecation warning during `dbt parse`, fixed before handoff.
- Caught, before running anything for real, that marts had only been configured for materialization (table vs view), not schema — `fct_transactions_daily` would have silently landed in the same schema as the staging views, blurring a distinction that's supposed to be meaningful. Fixed with a `+schema: marts` config plus a `generate_schema_name` macro override (dbt's default behavior concatenates a custom schema with the base one rather than using it cleanly — a widely-documented, intentional override, not project-specific magic).
- Explained the actual mechanical reason dbt's folder structure matters, prompted by a direct question — not just organizational convention, but that dbt looks for specific folder names (`models/`, `tests/`, `snapshots/`, `macros/`) and specific macro names and behaves differently based on them; a model sitting in `tests/` wouldn't just look messy, it would get executed as a pass/fail check instead of built as a table.
- Verified current dbt snapshot syntax before writing anything, given the pattern of API drift already hit repeatedly this project — confirmed dbt 1.9+ replaced the legacy `{% snapshot %}` Jinja block with YAML-based config as current. Built `accounts_snapshot.yml` snapshotting `stg_accounts_current` (never `gold.account_history` directly, which is already versioned) with `strategy=check`, `check_cols=[account_status, credit_limit]`, and a `dbt_valid_to_current` sentinel deliberately matching the custom `MERGE`'s own `'9999-12-31'` convention.
- Ran `dbt run`, `dbt test`, and `dbt snapshot` for real: 4/4 models, 13/13 tests, 1/1 snapshot, all passing. Confirmed 500 rows in the snapshot table on its first run.
- Wrote the "when I'd use each" comparison paragraph for the README, grounded in the actual mechanics just verified.

**🏗️ Architectural Decisions & Key Concepts**
- **dbt snapshot's `check` strategy has no concept of a business-effective date** — it only knows "did this differ from what I last recorded," stamping changes with the run's own timestamp. Not a missing feature to work around; the actual tradeoff, correct exactly when "when we noticed it" is an acceptable definition of "when it happened," wrong precisely in the late-arrival case Session 7 exists to solve.
- **Snapshot target is `stg_accounts_current`, never `gold.account_history` directly** — snapshotting an already-versioned table means asking dbt to track changes in something whose entire point is to already contain every change.
- **Snapshot lives in its own `snapshots` schema, separate from `marts`** — a technical demonstration of the pattern, not a curated, business-facing deliverable.
- **`generate_schema_name` macro override** — dbt's default schema-naming behavior concatenates custom schemas with the base rather than using them directly; overriding this dbt-recognized macro name is standard, documented practice.
- **dbt's folder structure is functional, not just organizational** — each folder is looked for by name and drives genuinely different behavior.
- **`dbt_valid_to_current` sentinel deliberately matched to the custom `MERGE`'s own `'9999-12-31'` convention** — consistency across both SCD2 implementations in the same repo, not a coincidence.

**⚠️ Technical Challenges & Troubleshooting**
1. Generic test arguments deprecation (`accepted_values`, `relationships`) — caught via a parse-time warning, fixed before handoff.
2. `marts` materialized correctly but landed in the wrong schema on the first real run — caught by reading the actual `dbt run` output carefully, not assumed correct just because the run succeeded.
3. A malformed `sed` edit to `dbt_project.yml` produced broken YAML indentation — caught immediately via `dbt parse` before it was ever handed over.
4. Recurring PowerShell relative-path confusion (`git add` from inside a nested subfolder; `dbt run` from the repo root instead of the dbt project folder) — same underlying pattern as Session 11, worth logging since it happened twice more; reinforced the general fix (absolute paths) rather than re-explaining from scratch each time.
5. dbt's legacy `{% snapshot %}` Jinja-block syntax was deprecated in favor of YAML-based config as of dbt 1.9+ — verified current syntax via search before writing anything.


## Session 13 — 2026-08-18 — Lineage, Exposure, CI Extension (Roadmap Day 13)

**Goal**
Complete the pipeline's outward-facing layer: lineage documentation, a real downstream consumer (Power BI dashboard, tracked as a dbt exposure), and extending CI to validate the actual live warehouse via `dbt build`, not just local sample data.

**Outcome**
Go, fully verified end to end. `dbt docs` confirmed to include the exposure node in the lineage graph. Power BI dashboard built and connected live — cross-checked against a known real number: the dashboard's `11.85K` transaction count matched Session 8's independently-verified `11,850` valid-transaction total exactly, to the digit. CI extended with three GitHub repo secrets and a dedicated CI-only `profiles.yml`; confirmed via the actual run (#13, `Status: Success`, 1m 21s) that `dbt build` succeeded against the real Databricks warehouse on the first attempt after adding the secrets.

**Actions**
- Verified current Power BI + Databricks connector steps before giving instructions rather than relying on possibly-stale knowledge — confirmed the native connector flow, and recommended Import mode over DirectQuery specifically because this is static synthetic data with no need for live-query freshness, keeping the report screenshot-able without the warehouse needing to stay awake.
- Built the dashboard to a real, if intentionally minimal, spec: a daily `total_amount` trend line, two KPI cards (explicitly checked for `Sum` aggregation and currency formatting, not left at defaults), and a `high_value_count` bar chart chosen deliberately over a single aggregate card — a by-date breakdown surfaces *when* SAR-threshold activity clustered, the actually useful signal for that column's real purpose.
- Cross-verified the finished dashboard against already-known project history rather than just trusting it looked plausible — the transaction count matched Session 8's number exactly, and the two sharp spikes visible in both charts correspond to Sessions 5 and 8's real batch ingestion dates. The dashboard visually encodes this project's own build history.
- Defined the exposure, verified via `dbt list --resource-type exposure` that it registers as a real graph node, not just parsed YAML. Caught and required a fix on a literal placeholder email left in a first draft, given the file is committed to a public repo — resolved with a GitHub-provided noreply address instead of a real personal email.
- Made a real, avoidable mistake editing the README: an incautious edit accidentally deleted the entire "SCD Type 2: Two Approaches" section instead of just inserting new content before it. Caught immediately by re-viewing the file after the edit, not assumed correct just because the edit itself succeeded, and restored before it was ever handed over.
- Extended CI to touch real infrastructure for the first time: a dedicated `dbt/audittrail/ci/profiles.yml` containing only `env_var()` references, never actual values — safe to commit specifically because of that distinction, kept clearly separate from the rule that the real, value-holding `profiles.yml` never enters the repo at all.
- Verified the combined `requirements.txt` (now five packages) installs cleanly with zero dependency conflicts, tested in an isolated fresh virtual environment simulating a real CI runner, before trusting GitHub Actions to do the same thing unattended.
- Extended `data_quality.yml` with a `dbt build` step using explicit `--project-dir`/`--profiles-dir` flags rather than a working-directory change, specifically to avoid the same relative-path ambiguity that caused repeated confusion earlier this session.
- Ran `dbt build` locally first, against the real warehouse via the existing dev profile, before trusting the CI version — 18 PASS, 1 NO-OP (the exposure itself — pure dependency-graph metadata, no SQL to run), 0 errors.
- Confirmed the actual CI run directly rather than just trusting a report of success — an initial general repo-page fetch returned what looked like a stale cached snapshot (showing only the very first commit); recognized as almost certainly a caching artifact rather than reported as real, alarming news, and resolved by fetching the specific run URL instead, which returned accurate, current data.

**🏗️ Architectural Decisions & Key Concepts**
- **Import mode chosen over DirectQuery** — static synthetic data doesn't need live-query freshness; Import keeps the report usable without the warehouse needing to stay awake for every interaction.
- **A dbt exposure's `NO-OP` status during `dbt build` is correct, expected behavior, not a warning** — an exposure is pure dependency-graph metadata, never something materialized.
- **CI's `profiles.yml` is safe to commit specifically because it contains only `env_var()` references, never literal values** — categorically different from the local dev `profiles.yml`, which must never enter the repo.
- **GitHub Secrets are a fundamentally different security mechanism than a committed file** — encrypted at rest, never visible in the UI after creation, automatically redacted from logs if they appear there.
- **CI now validates two genuinely different things at two genuinely different levels**: local fixture data (fast, isolated, no live dependency) and the actual live warehouse (`dbt build`, real infrastructure, real credentials) — a meaningful escalation in scope from Day 10, not just "more steps."

**⚠️ Technical Challenges & Troubleshooting**
1. An incautious README edit deleted an entire existing section instead of just inserting before it — caught by re-viewing the file immediately after, not assumed correct because the tool call succeeded.
2. A literal placeholder email was initially left in a file meant for a public repo — caught and required an actual fix, not treated as cosmetic.
3. A general repository-page fetch used to verify the CI run returned a stale cached snapshot — recognized as a caching artifact rather than reported as real news; resolved by fetching the specific run URL directly, which returned accurate data.