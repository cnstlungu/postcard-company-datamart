# Postcard Company Datamart: review and improvement plan

Reviewed 2026-09-14 against `main` (`737d1e6`) and the five consumers:
`portable-data-stack-{mage,airflow,dagster,sqlmesh}` and `postcard-company-dataform`.
(`portable-data-stack-bruin` is a separate e-commerce demo and does not use this model.)

## 1. How the consumers depend on this repo (the compatibility contract)

| Consumer | Mechanism | What it depends on |
|---|---|---|
| mage, airflow, dagster, sqlmesh | `git clone --branch $DATAMART_REF` (default **`v1.0.0`**) at image build, sparse checkout of `generator/` and `postcard_company/`. Nothing vendored, no model code of their own. | Directory names `generator/` and `postcard_company/`; env vars `INPUT_FILES_PATH`, `DUCKDB_FILE_PATH`, `N_TRANSACTIONS`; dbt project name; `dbt deps/seed/run/test` (dagster: `dbt build`; sqlmesh: dbt adapter, so every dbt feature used must be one SQLMesh's adapter supports). |
| all four stacks | One identical `superset/assets/dashboard.zip` (exported 2024-02-21), six virtual SQL datasets hardcoding `postcard_company_core.*`. | `fact_sales(total_amount, qty, commissionpaid, geography_key, bought_date_key, channel_key, sales_agent_key)`, `dim_geography(geography_key, country_name, city_name)`, `dim_date(date_key, first_day_of_month, day_of_week)`, `dim_channel(channel_key, channel_name)`, `dim_sales_agent(sales_agent_key, reseller_name)` and the literal `'Direct Sales'`. |
| dataform | Vendored copy of `generate.py` (drifted: lacks `DATA_END_DATE`), full BigQuery re-implementation of every model, explicit external-table schemas for 5 of 7 parquet files, auto-detect for the two reseller feeds. Already has an SCD2 `dim_product`. | Exact parquet column names and types, including the spaced / camelCase / hyphenated names and the nested `customer` struct in `resellers_type2`. |

Consequences for planning:

- **Nothing consumes `main` today.** Everything is pinned to `v1.0.0`, so `main` can evolve freely as long as the tag stays put. Consumers opt in by bumping `DATAMART_REF`.
- **Additive changes are free**; renames and type changes of the columns above are breaking. New parquet columns are ignored by every consumer (dbt `raw_*` models enumerate columns; Dataform external tables ignore extras or auto-detect).
- **No consumer CI asserts data.** They only check that the UIs come up. Contract validation has to live here.
- `main` is already two commits past `v1.0.0` (seeded generator, global transaction ids, rolling date window) and is not tagged. Consumers cannot get those fixes without a tag.

## 2. Findings

### 2.1 Model and project defects

1. **Incrementality is fake.** Every `raw_*` model stamps `CURRENT_TIMESTAMP AS loaded_timestamp` and is rebuilt as a table each run, so the incremental filter `loaded_timestamp > max(this.loaded_timestamp)` matches every row every run. The three incremental staging models and `fact_sales` re-merge 100% of rows on each run. The models teach "incremental" while doing a full merge. Root cause: the source carries no extraction time and arrives as one monolithic file.
2. **Layer inversion.** `staging_customers` reads `dim_sales_agent`; `staging_transactions_main` and both reseller staging models read `dim_product`, `dim_channel`, `dim_customer`. The DAG goes raw → staging → core → staging → core, which contradicts the three-layer story in the README. The `LEFT JOIN dim_customer cu` in all three transaction staging models is dead (nothing is selected from it).
3. **The messy reseller format is never exercised.** The type 2 generator emits three date columns (`date` and `dateCreated` as `YYYYMMDD` strings, `Created Date` as ISO). The model reads only the clean ISO one; the other two are dead columns. Type 1 and type 2 staging models also output different column sets (type 2 leaks `product_name`, `created_date`).
4. **Accidental dirty data.** `'Donetsk '` and `'Mykolaiv '` carry trailing spaces in both `CITIES` and `seeds/geography.csv`; the product-to-geography join works only because both sides share the typo. Either intentionalise it (and trim in staging) or fix it.
5. **Money as DOUBLE.** Contracts declare `total_amount`, `commissionpaid`, `product_price` as `DOUBLE`; staging casts to `NUMERIC` and the union promotes back to `DOUBLE`. Should be `DECIMAL(12,2)` end to end. This is a type change, so it is a v2 item.
6. **`geography_key` on `fact_sales` is the postcard's depicted city, not where the sale happened.** The Superset "Sales per country" world map therefore shows which cities appear on postcards. Customer geography is absent, and customers are worldwide (see 2.2).
7. **Contradictory documentation.** Comments in both reseller staging models say `transaction_id` is unique only within a reseller; `schema.yml` says the same and also says it is globally unique; the generator now guarantees global uniqueness. README still references a removed `assets.py`, and its dimension list omits `dim_product`.
8. **Orphan / wrong configs.** Seed column types are written as `geography: id: int` instead of `+column_types:` so they are ignored; `partitioned_by` on the incremental models is a no-op for dbt-duckdb table materializations; `clean-targets` removes `dbt_packages`; `dim_date` is hardcoded to 2015 to 2030 while the generator window now floats with today's date.
9. **Tests.** `grain_is_correct` duplicates the `unique` test on `sales_key`. `total_amount_is_non_negative` actually asserts strictly positive. No relationship tests (removed for a Docker-on-macOS SIGBUS reason; `threads: 1` fixes that and is not set in `profiles.yml`). No dbt unit tests (dbt-core ≥ 1.8 is in use). No source freshness. CI hardcodes `220,000`.
10. **Repo hygiene.** Untracked strays: `.superset/config.json`, `dataform/workflow_settings.yaml`, `.idea/`, `.DS_Store`. `package-lock.yml` is gitignored but is normally committed for reproducible `dbt deps`.
11. **Generator performance and structure.** One 250-line script building Python lists of dicts through pandas; 1M rows takes minutes. `cleanup()` is dead code. `INPUT_FILES_PATH` unset raises a bare `KeyError`.

### 2.2 Realism gaps in the generated data

Verified on the built warehouse: sales per month vary by ±3%, day-of-week counts are flat, channel mix is a fixed ratio, every product sells equally, quantity is uniform.

- **No time structure.** Uniform dates: no seasonality (postcards should peak in summer and December), no weekday pattern, no growth trend, no promotions. Dashboards look like noise.
- **Uniform everything else.** Channel mix never shifts; product popularity is flat instead of long-tailed; prices are uniform 1.5 to 4.4 and unrelated to format; quantities are uniform.
- **Customers are not European.** Faker `en_US` produces worldwide countries (top: Congo, Korea, Burkina Faso) and US-style cities for a company that sells in Europe. Reseller customers are generated fresh per transaction, so almost none repeat (165k of 180k buy once); direct customers are uniformly random, no heavy buyers.
- **No history.** Prices never change, products are never launched or discontinued, resellers' commission never changes. `product_price` on the fact is called "price at time of sale" but there is only one price, so SCD2 has nothing to show. The Dataform consumer built an SCD2 `dim_product` anyway and it never produces a second version.
- **No business events beyond one line per transaction.** No orders with multiple lines, no returns or cancellations, no discounts, no currency (resellers span UK, Scandinavia, the Mediterranean), no VAT, no cost or margin, no wholesale price (resellers sell at exactly list price).
- **No dirt.** No nulls, duplicates, whitespace or case variance, no late-arriving rows, no corrections. Every test passes trivially and no quarantine or dedup logic is ever needed.
- **No delivery cadence.** One file per source, regenerated wholesale. The four orchestrators have nothing to schedule, partition, backfill, or sense.

## 3. Prioritised plan

Versioning rule: P0 and P1 are non-breaking and ship as `v1.1.0` and `v1.2.0`. P2 and P3 change the parquet layout or core types and ship as `v2.0.0`. Consumers bump `DATAMART_REF` when ready; `v1.0.0` is never moved.

### P0. Foundation and compatibility (ship first, low risk, ~1 to 2 days)

| # | Item | Why | Touches |
|---|---|---|---|
| 0.1 | **Tag `v1.1.0` from current `main`** and add `CHANGELOG.md`. | Consumers cannot get the id-collision fix or the seeded generator without a tag. | git, README |
| 0.2 | **Write `docs/consumer-contract.md`**: the seven parquet files with columns and types, the three schema names, the core table columns, the Superset column list, env vars, and the semver policy. Add **dbt exposures** for the six Superset datasets so the BI dependency is visible in `dbt docs` and lineage. | Makes breaking changes reviewable instead of discovered in a dashboard. | docs, `models/exposures.yml` |
| 0.3 | **Contract check in CI.** Script that opens the built DuckDB file, asserts the core column names and types from the contract, and runs the six Superset dataset SQL statements (copied verbatim into `tests/bi/`). | No consumer CI does this; a rename would ship green everywhere. | `.github/workflows`, `scripts/` |
| 0.4 | **Hygiene fixes** from 2.1 items 4, 7, 8, 9, 10: trim the two city names (generator and seed), set `threads: 1` and re-add relationship tests, rename `total_amount_is_non_negative` to `..._is_positive`, drop `grain_is_correct`, derive `dim_date` bounds from `var()`s with defaults covering the generator window, fix the README, gitignore or delete the strays, commit `package-lock.yml`. | Cheap and removes teaching noise. | many small |
| 0.5 | **Fix the layer inversion.** Staging models become pure clean-and-type layers on natural keys (`product_id` / `product_name`, `channel_name`, `reseller_id`, `email`). Introduce `intermediate/int_sales_unioned.sql` that unions the three feeds and resolves surrogate keys against the dims. `fact_sales` reads the intermediate. Core output columns unchanged. | Restores raw → staging → intermediate → core; removes the dead `dim_customer` join. | `models/staging/*`, new `models/intermediate/`, `fact_sales.sql` |
| 0.6 | **Make incrementality real, minimal version.** Generator adds an `extracted_at` timestamp column to every file (additive). `raw_*` models carry it instead of `CURRENT_TIMESTAMP`; incremental filters use it; add `freshness:` to `sources.yml` on it. | The incremental models then behave as documented even before batched drops (P3). | `generate.py`, `models/raw/*`, `sources.yml` |
| 0.7 | **dbt unit tests** for: type 2 date parsing (after 0.8), surrogate key composition, commission calculation, direct-sales sentinel. | Demonstrates dbt ≥ 1.8 unit testing; each is a two-row fixture. Verify SQLMesh's dbt adapter tolerates `unit_tests:` blocks (it may ignore them; must not fail). | `models/unit_tests.yml` |
| 0.8 | **Use the messy type 2 date.** Staging parses `dateCreated` (`YYYYMMDD`) with `strptime`; generator keeps `Created Date` for one more minor version, then drops it in v2. Align type 1 and type 2 staging output columns. | The point of two reseller formats is to show format wrangling. | `staging_reseller_type2_sales.sql`, `generate.py` |

### P1. Realistic data shape (generator only, schema unchanged, ~2 to 3 days)

Every item here changes values, not columns, so all consumers can adopt it with a `DATAMART_REF` bump and no code change. The dashboards immediately look like a real business.

| # | Item | Detail |
|---|---|---|
| 1.1 | **Time structure.** | Daily demand = base × yearly growth (e.g. +12%/yr) × seasonality (summer and December peaks, January trough) × weekday factor (weekend +30% in-store, weekday web) × occasional promo spikes. Sample dates from that curve instead of uniformly. |
| 1.2 | **Long-tailed catalogue and quantities.** | Product popularity Zipf-distributed with city-level popularity (Paris, London, Roma sell more); quantity geometric (mostly 1 to 2, rarely 10); prices by format with a small city premium. |
| 1.3 | **Channel drift.** | Web and mobile share grows over the window; in-store shrinks; resellers stay in-store heavy. |
| 1.4 | **European customers.** | Locale-weighted Faker (`de_DE`, `fr_FR`, `it_IT`, `es_ES`, `pl_PL`, `ro_RO`, `nl_NL`, `en_GB`, `sv_SE`, ...) with country consistent with locale and shared address, phone and postcode formats. Reseller customers drawn from a per-reseller pool with a Pareto repeat rate so `staging_customers` dedup finally does work; direct customer purchase frequency Pareto (a few heavy buyers). |
| 1.5 | **Controlled dirt, on by default with fixed rates.** | Duplicate reseller rows (feed retries, ~0.5%); mixed-case and padded emails; null postcodes and phones; a few product names with different whitespace; ~1% type 2 rows with an unparseable date; ~0.3% rows where `Total amount` differs from qty × price (rounding or discount). Each defect has a matching staging fix or quarantine model plus a test with `severity: warn`, so students see tests fire. Env var `DIRT_RATE=0` turns it off. |
| 1.6 | **Row counts.** | `N_TRANSACTIONS` should also scale the reseller feeds (currently fixed at 50,000 each), and the generator should write `_manifest.json` with per-file row counts so CI asserts against it instead of `220,000`. |
| 1.7 | **Generator refactor.** | Split into `generator/{config,catalogue,customers,demand,feeds,writers}.py` with an argparse CLI, vectorised sampling with numpy (1M rows in seconds), no bare `KeyError` on missing env. Keep `python generator/generate.py` as the entry point because all four Dockerfiles call it. |

### P2. Model extensions (additive columns and tables, v2.0.0, ~3 to 5 days)

| # | Item | Model change | Consumer impact |
|---|---|---|---|
| 2.1 | **Price history and SCD2 `dim_product`.** Generator emits `products.parquet` with `valid_from` per price version (two or three changes per year across the catalogue). dbt `snapshot` on products; `dim_product` gains `valid_from`, `valid_to`, `is_current`; fact joins on the price valid at `bought_date`, so `product_price` becomes a real "price at time of sale". | snapshot + dim columns | Additive. Aligns with the Dataform SCD2. **Verify SQLMesh dbt-adapter snapshot support** before committing to `snapshot`; fallback is an explicit SCD2 model. |
| 2.2 | **Customer geography.** `dim_geography` extended with customer countries and regions (seed); `fact_sales` gains `customer_geography_key`; keep `geography_key` as is and document it as the depicted city. Add a second Superset-ready column so the "Sales per country" map can be repointed in the stacks. | seed + fact column | Additive. |
| 2.3 | **Orders and lines.** Direct feed gains `order_id`; a basket has one to four lines. `fact_sales` gains `order_id`, `line_number`; new `fact_order` (header: lines, basket value, channel). | new columns + new fact | Additive. Enables basket analysis and a second fact for the semantic layer. |
| 2.4 | **Returns.** New `returns.parquet` (transaction_id, return_date, qty, reason) → `raw_returns`, `staging_returns`, `fact_returns`. `fact_sales` stays positive so its test holds. | new source + fact | Additive; needs a `sources.yml` entry (consumers get it from the clone). |
| 2.5 | **Money types and currency.** `DECIMAL(12,2)` for all amounts; reseller feeds carry `currency` (GBP, SEK, EUR); `fx_rates` seed; fact keeps `total_amount` in EUR and adds `total_amount_local`, `currency_code`. | type change + columns | **Breaking** on the Superset side only for `avg(total_amount::numeric)`, which still works; Dataform needs `FLOAT64` → `NUMERIC`. This is why it is v2. |
| 2.6 | **Cost and margin.** `unit_cost` on products; `fact_sales.gross_margin`. Reseller feeds carry a wholesale unit price so reseller `total_amount` ≠ list price × qty, making commission on net a meaningful calculation. | columns | Additive. |
| 2.7 | **Effective-dated commissions.** `resellers.parquet` with `valid_from`; SCD2 `dim_sales_agent`; `commissionpct` on the fact resolved by date. | snapshot | Additive. |

### P3. Orchestration-oriented delivery (what the four stacks need most, v2.0.0, ~2 to 3 days here plus one PR per stack)

| # | Item | Detail |
|---|---|---|
| 3.1 | **Batched file drops.** `OUTPUT_LAYOUT=single` (today's behaviour, remains default through v2.x) or `partitioned`: `main/date=YYYY-MM-DD/part-0.parquet`, reseller feeds as weekly files arriving with a two-day lag and occasional restated files (same transaction ids, corrected quantities). `sources.yml` reads `read_parquet('.../main/*/*.parquet', hive_partitioning=true)` so dbt sees one source either way. | Gives Airflow and Dagster date partitions and backfills, Mage a daily trigger, SQLMesh `incremental_by_time_range`, and makes the `on_schema_change` / late-arriving-data lessons real. |
| 3.2 | **Daily mode.** `python generator/generate.py --mode daily --run-date 2026-09-14` appends one day's files deterministically from `SEED` + date, so a scheduled run produces new data without regenerating history. | The stacks' schedules finally have something to do. |
| 3.3 | **Restatement recipe.** Document and CI-test the three replay scenarios: late file, corrected file, full-refresh, matching the existing "Upgrading an existing warehouse" section. | Turns the current upgrade caveat into a feature. |
| 3.4 | **Consumer follow-ups (separate PRs in each stack).** Bump `DATAMART_REF`; pass `SEED`, `DATA_END_DATE`, `DATA_WINDOW_MONTHS`, `OUTPUT_LAYOUT` through compose; re-export the Superset dashboard against v2 (customer geography map, margin, returns); dagster: stop bind-mounting the sparse checkout (stale host copies are currently older than the pin, and it still carries a leftover `config/profiles.yml` and a scaffold project); dataform: re-sync the generator copy and the new columns. | Out of scope for this repo but should be tracked alongside v2. |

## 4. Suggested order of work

1. P0.1 to P0.4 in one PR (tag, contract, CI check, hygiene). Half a day.
2. P0.5 to P0.8 in a second PR (layers, real incrementals, unit tests, messy date). One day. Tag `v1.1.0`.
3. P1 as one generator PR with before/after dashboard screenshots. Tag `v1.2.0`. Bump the four stacks to it.
4. P2.1, P2.2, P2.6 first (they make the existing dashboards meaningful), then P2.3, P2.4, P2.5, P2.7.
5. P3.1 to P3.3, tag `v2.0.0`, then the per-stack PRs.

## 5. Open questions to settle before P2

- Does the SQLMesh dbt adapter in the sqlmesh stack (`sqlmesh==0.231.1`) accept dbt snapshots and ignore `unit_tests:` blocks without failing the plan? If not, implement SCD2 as a plain model and keep unit tests in a directory the adapter does not load.
- Should dirt be on by default (better teaching, but the current tests become noisy) or opt-in? Recommended: on by default at low rates, with `severity: warn` on the affected tests.
- Keep `geography_key` named as is on the fact for dashboard compatibility, or rename to `product_geography_key` at v2 and re-export the dashboard? Recommended: keep, add `customer_geography_key`, rename only if the dashboard is re-exported anyway.

## 6. dbt engine versions and v2 readiness

Added 2026-09-18, after testing this project against dbt-core 1.10.15, 1.12.5 and dbt 2.0.4.

### Where the versions stand

| Version | Support status | Where it was pinned |
|---|---|---|
| 1.8 | Long past support | the mageai:0.9.79 image, which ships its own dbt |
| 1.10 | **Deprecated** as of the v2 release | airflow, dagster, sqlmesh images |
| 1.12 | Active support to 2027-07-15 | what CI floated to by accident |
| 2.0 | Released 2026-09-14, active to 2027-09-13 | nowhere yet |

`requirements-ci.txt` pinned only `dbt-duckdb`, so `dbt-core` and `duckdb` floated to the
newest release. CI was validating dbt-core 1.12.5 / duckdb 1.5.5 while every stack image
shipped dbt-core 1.10.15 / duckdb 1.4.4 - the one file where the "these move together"
rule was unenforceable because the pin was missing. All three are pinned now.

### What v2 needs, and what it already has

The project parses, builds and tests clean on dbt 2.0.4 (22 models, 50 tests) after the
changes that shipped with this section. Two of those changes are not cosmetic:

- **`+start: Jan 1 2000` is not a stray.** SQLMesh refuses to load a dbt project without a
  backfill start date, and dbt v2 rejects `+start` as an unrecognised key. The start date
  now lives in `portable-data-stack-sqlmesh`'s own `sqlmesh-dbt/config.py` as
  `model_defaults=ModelDefaultsConfig(start="2000-01-01")`, which is where it belongs.
  **That change has to land before this repo's tag moves**, or the sqlmesh stack breaks.
- **`external_location` is declared twice in `sources.yml`.** dbt v2's built-in DuckDB
  adapter reads `config.external_location`; SQLMesh reads it from the source's meta only.
  Both copies are required; a top-level `meta:` beside `config:` is rejected by v2.

Things the v2 upgrade guide warns about that do **not** apply here: no YAML anchors, no
`config.get()`/`meta` access, no behaviour-change flags, no duplicate docs blocks. Static
analysis over `read_parquet()` of local files - which the DuckDB v2 docs flag as a known
gap - resolves correctly for this project.

### The one thing still blocking v2

Every incremental model fails on its **second** run under v2:

```
[JinjaError (dbt1501)] The source and target schemas on this incremental model are out of sync!
New column types: [{'transaction_id','bigint'}, ... {'loaded_timestamp','datetime'}]
```

v2 compares its own canonical type names (`datetime`, `decimal(18, 3)`) against DuckDB's
(`TIMESTAMP`, `DECIMAL(18,3)`) and reports every column as changed. It is a false positive
and a Fusion parity bug; dbt 1.x is unaffected and keeps `on_schema_change: 'fail'`
working. `append_new_columns` works around it on all four models, and a contracted
incremental (`fact_sales`) cannot use `'ignore'` at all - v2 requires `append_new_columns`
or `fail` when a contract is enforced. Do not weaken `on_schema_change` for this: the
guard is worth more than early v2 support. Revisit when the bug is fixed upstream.

### Consumer readiness for 1.12

- **airflow**: no dbt-core ceiling of its own.
- **sqlmesh**: declares `dbt-core<2`; verified running on 1.12.5 + dbt-duckdb 1.11.0.
- **dagster**: `dagster-dbt==0.27.13` pins `dbt-core<1.11`, so 1.12 needs a
  dagster/dagster-dbt bump (1.13.23 / 0.29.23 resolves cleanly).
- **mage**: the `mageai:0.9.79` image ships dbt-core 1.8.7, which cannot parse
  `arguments:` at all - it fails with *macro 'dbt_macro__test_accepted_values' takes no
  keyword argument 'arguments'*. That stack has to install a newer dbt over the image
  before it can take a tag containing this change. Mage runs dbt as a subprocess, so
  upgrading the CLI is enough.

There is no form of `schema.yml` that satisfies both dbt 1.8 and v2: top-level test
arguments are an error under v2, and `arguments:` is an error before 1.10. Every consumer
has to reach at least 1.10 before this repo's tag moves.
