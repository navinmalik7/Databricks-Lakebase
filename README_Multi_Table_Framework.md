# Lakebase multi-table sync framework — FreshCart

Config-driven version of Part 2. You describe **which Gold tables go to Postgres** and **which Postgres columns flow back into Gold** in three Delta tables. Generic notebooks read that config and do the rest, so adding a table means adding config rows, not writing code.

```
                       config.sync_tables
 Gold tables ───────────────────────────────────────────► Postgres  gold.*_synced   (read-only, apps read)
   ▲                 synced table + 1 job per table
   │
   │  MERGE (all configured columns together)            Postgres  app.*           (apps write)
   │                                                          │
 Bronze lb_<table>_history ◄──────────────────────────────────┘
        config.writeback_tables / writeback_columns     Lakebase CDF (Option A) or notebook capture (Option B)
```

---

## 1 · Folder contents

| Notebook | What it does | When |
|---|---|---|
| `00_config` | Settings + helpers + config loaders + change capture (Option B) | `%run` by every notebook |
| `10_framework_setup` | Creates the 3 config tables, seeds 3 Gold tables, builds the 2 extra demo tables | once |
| `11_validate_config` | ~15 checks per table; stops on any ERROR | after every config change |
| `12_sync_gold_to_postgres` | Change Data Feed on → synced table per row → wait online → **one job per table** (table-update trigger) | after config change |
| `13_writeback_setup` | Creates/evolves `app.<table>` in Postgres from config, `REPLICA IDENTITY FULL`, starts change capture | after config change |
| `14_merge_writebacks` | Streams each history table → latest change per key → one MERGE per table; schedules itself | scheduled |
| `15_durable_gold_build` | Gold rebuild = override first, computed second, for every table; deletes revert | your Gold step |

Import these into **their own workspace folder**. This `00_config` replaces the single-table one and doesn't have the old names (`SOURCE_TABLE`, `HISTORY`, …), so don't mix it with notebooks 01–06.

**Run order:** original `01_setup_freshcart` (for `customer_360`) → `10` → `11` → `12` → `13` → `14` → `15`.

---

## 2 · The config tables

### `retail.config.sync_tables`: one row per Gold table to push to Postgres
| column | example | meaning |
|---|---|---|
| `sync_id` | `store_inventory` | short unique name (used in job names) |
| `source_table` | `retail.gold.store_inventory` | Gold table |
| `primary_key` | `["store_id","sku"]` | composite keys are fine |
| `scheduling_policy` | `TRIGGERED` | `SNAPSHOT` / `TRIGGERED` / `CONTINUOUS` |
| `synced_table` | NULL | default `<source_table>_synced` |
| `timeseries_key` | NULL | only if the source can have duplicate keys |
| `computed_source` | `retail.silver.inventory_computed` | for the durable rebuild (notebook 15) |
| `enabled` | true | switch a table off without deleting its config |

### `retail.config.writeback_tables`: one row per Postgres table the apps write to
| column | example | meaning |
|---|---|---|
| `writeback_id` | `inventory_overrides` | short unique name (checkpoint name) |
| `pg_table` | `inventory_overrides` | table in Postgres schema `app` (created for you) |
| `target_table` | `retail.gold.store_inventory` | Gold table it updates |
| `key_columns` | `["store_id","sku"]` | same names in Postgres and Gold |
| `audit_by_column` / `audit_at_column` | `override_by` / `override_at` | optional Gold columns for who/when |
| `apply_order` | 30 | merge order |

### `retail.config.writeback_columns`: one row per editable column
| column | example | meaning |
|---|---|---|
| `writeback_id` | `inventory_overrides` | which write-back |
| `pg_column` | `reorder_level` | column in `app.inventory_overrides` |
| `gold_column` | `reorder_level` | column it updates in Gold |
| `merge_rule` | `COALESCE` | `COALESCE`: NULL means "no change" (default). `OVERWRITE`: the app owns the value, NULL clears it |

All columns configured for one write-back table are applied **together in one MERGE per key**. When store ops changes `reorder_level` and `safety_stock` in one save, both land in Gold in the same commit.

---

## 3 · Adding a new table (example: `promotions`)

```sql
-- 1. push it to Postgres
INSERT INTO retail.config.sync_tables VALUES
 ('promotions', 'retail.gold.promotions', array('promo_id'), 'TRIGGERED', NULL, NULL,
  'retail.silver.promotions_computed', true, 'data-team', 'Marketing app');

-- 2. let the marketing app edit two columns
INSERT INTO retail.config.writeback_tables VALUES
 ('promo_overrides', 'promo_overrides', 'retail.gold.promotions', array('promo_id'),
  'override_by', 'override_at', 40, true, 'marketing-app', NULL);
INSERT INTO retail.config.writeback_columns VALUES
 ('promo_overrides', 'discount_pct', 'discount_pct', 'COALESCE', true, NULL),
 ('promo_overrides', 'end_date',     'end_date',     'COALESCE', true, NULL);
```
Then run **11 → 12 → 13**. Notebook 14's job picks the new table up on its next run.
Adding a column later is one `INSERT` into `writeback_columns` + run 11 → 13. It's added to Postgres with `ADD COLUMN IF NOT EXISTS`.

The Gold table needs a primary key, no NULL/duplicate keys, the edited columns, and (optionally) `override_by STRING, override_at TIMESTAMP`. Notebook 11 checks all of this.

---

## 4 · Rules the framework enforces (notebook 11)

- **One owner per Gold column.** Two write-backs can't both update `customer_360.credit_limit`, which prevents last-writer-wins fights between apps.
- **Keys can't be edited**, and names can't clash with the audit columns (`reason`, `updated_by`, `updated_at`).
- **No loop:** the app schema (`app`) must never be a synced-table schema. Synced tables land in `gold`, the feed watches only `app`.
- **Only simple identifiers** (`lower_snake_case`) are accepted from config, because they're used to build SQL.
- Supported types for edited columns: BIGINT, INT, SMALLINT, STRING, BOOLEAN, DOUBLE, FLOAT, DECIMAL(p,s), DATE, TIMESTAMP. The Postgres types are copied from Gold automatically.

---

## 5 · Is a Delta config table the best way? My recommendations

**Delta config tables, as built here, are a good fit**: queryable, governed by Unity Catalog, versioned by Delta history, and editable with plain SQL. Three improvements I'd make for production:

1. **Keep the config in Git and load it into the Delta tables (config as code).** Put the same rows in a YAML file in your repo, review changes in pull requests, and have a deployment job (e.g. a Databricks Asset Bundle) upsert them into `retail.config.*` and then run 11 → 12 → 13. A Delta table edited by hand has history, but no review step. With Git, a wrong `OVERWRITE` rule gets caught before it reaches Gold.
2. **One write-back table per Gold table, not one generic key/column/value table.** A single generic "overrides" table (entity, key, column, value-as-string) looks simpler but loses types, NOT NULL and primary-key constraints in Postgres, and makes the app's SQL and the change feed harder to read. Typed tables generated from config (this design) give you both.
3. **At larger scale, consider Lakeflow Declarative Pipelines' AUTO CDC** (`create_auto_cdc_flow`) instead of the hand-written MERGE in notebook 14. You'd loop over the config to declare one flow per write-back; it handles ordering by `_sort_by`, deletes and SCD type 1 for you. The MERGE approach here is easier to follow on video and runs on any compute, so I kept it.

---

## 6 · Things to know before production

- **Each synced table uses its own pipeline and up to 16 Postgres connections.** With many tables, size the Lakebase compute (CU range) for it, and prefer Triggered over Continuous.
- **Schema changes on Gold:** Triggered/Continuous syncs only accept additive changes. Anything else means recreating that synced table.
- **Lakebase CDF (Option A)** watches the whole `app` schema with one feed. A table appears only after it has at least one row. If a newly added table doesn't show in `SELECT * FROM wal2delta.tables`, stop and start the feed.
- **Notebook 14 doesn't revert on delete.** If an app deletes an override, Gold keeps the last value until the durable build (notebook 15) runs. Make 15 your real Gold step to get reverts.
- **Notebook 15's MERGE deletes Gold rows that are missing from `computed_source`** (`WHEN NOT MATCHED BY SOURCE THEN DELETE`). That's right when the Silver table is the full set; remove that line if it isn't.
- `foreachBatch` uses the batch's own Spark session (`batch_df.sparkSession`), which serverless compute requires.

---

## 7 · What was tested

Tested locally on Apache Spark 4.0 with a fake Postgres (Delta couldn't be loaded offline, so tables were Parquet):
- change capture across **insert → no-change → update + delete** on a **composite key**, including a row where only one of two columns was set,
- the "latest change per key wins" logic used by the MERGE,
- the generated Gold build: overrides win on both columns, a deleted override falls back to the computed value, untouched rows stay computed.

**Not tested here:** the Databricks / Lakebase API calls (synced tables, jobs, CDF), the Delta MERGE statements, and real Postgres DDL. Do a dry run in your workspace before recording.
