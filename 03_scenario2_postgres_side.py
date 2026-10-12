# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Scenario 2 (part A) — The trap, the overrides table, and the change feed
# MAGIC **Video: Part 2, Slides 7 – 9.**
# MAGIC
# MAGIC On camera, the SQL in this notebook can also be run in the **Lakebase SQL Editor** (Lakebase app → project →
# MAGIC SQL Editor, database `freshcart_app`). It's the same Postgres either way — pick whichever looks better on screen.

# COMMAND ----------

# MAGIC %pip install -q -U "databricks-sdk>=0.89.0" "psycopg[binary,pool]>=3.1"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 · The trap — try to UPDATE the synced table  *(Slide 7)*
# MAGIC Synced tables are owned by the sync pipeline. Your user gets SELECT / DELETE / TRUNCATE — **no INSERT or UPDATE**.
# MAGIC Expect `permission denied`. (If you're `databricks_superuser` it may *succeed* — and the next sync silently overwrites it.
# MAGIC Either outcome makes the point.)

# COMMAND ----------

try:
    pg(f"UPDATE gold.customer_360_synced SET credit_limit = 75000 WHERE customer_id = {DEMO_CUSTOMER}", fetch=False)
    print("⚠️ The UPDATE went through — but the next sync will overwrite it. Don't do this.")
except Exception as e:
    print("❌", type(e).__name__, "—", str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 · A table the app owns: `app.customer_overrides`  *(Slide 9)*
# MAGIC Who changed it and when — auditors will love you.
# MAGIC
# MAGIC ⚠️ Keep this in its **own schema** (`app`). Lakebase CDF watches a *whole schema*; never start it on `gold`,
# MAGIC where the synced tables live, or you'd feed the synced copy back into the lakehouse.

# COMMAND ----------

pg(f"CREATE SCHEMA IF NOT EXISTS {PG_APP_SCHEMA}", fetch=False)
pg(f"""
CREATE TABLE IF NOT EXISTS {PG_APP_SCHEMA}.customer_overrides (
  customer_id  BIGINT PRIMARY KEY,
  credit_limit NUMERIC(12,2),
  segment      TEXT,
  reason       TEXT,
  updated_by   TEXT        NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
)""", fetch=False)
print("app.customer_overrides ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 · The line everyone forgets  *(Slide 9)*
# MAGIC By default Postgres logs only the key for updates/deletes. CDF needs the **full before and after row**.

# COMMAND ----------

pg(f"ALTER TABLE {PG_APP_SCHEMA}.customer_overrides REPLICA IDENTITY FULL", fetch=False)
print(pg(f"""
SELECT c.relname, CASE c.relreplident WHEN 'f' THEN 'full' WHEN 'd' THEN 'default'
                                      WHEN 'n' THEN 'nothing' WHEN 'i' THEN 'index' END
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = '{PG_APP_SCHEMA}' AND c.relkind = 'r'"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 · Priya clicks **Save** — the Support Portal writes the override
# MAGIC This is the exact statement the app runs. (It also gives the table its first row — CDF only picks up
# MAGIC tables that contain at least one row.)

# COMMAND ----------

pg(f"""
INSERT INTO {PG_APP_SCHEMA}.customer_overrides (customer_id, credit_limit, reason, updated_by)
VALUES (%s, 75000, 'Loyal, 3 yrs on-time payments', 'priya@freshcart.in')
ON CONFLICT (customer_id) DO UPDATE
  SET credit_limit = EXCLUDED.credit_limit, reason = EXCLUDED.reason,
      updated_by = EXCLUDED.updated_by, updated_at = now()""", (DEMO_CUSTOMER,), fetch=False)
print(pg(f"SELECT * FROM {PG_APP_SCHEMA}.customer_overrides"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 · Start Lakebase Change Data Feed  *(Slide 9 — Public Preview)*
# MAGIC **On camera (recommended):** Lakebase app → `freshcart` → click the branch name → **Lakebase CDF** tab → **Start** →
# MAGIC Database `freshcart_app` · Schema `app` → To catalog `retail` · Schema `bronze` → **Start**.
# MAGIC
# MAGIC **Or** run the cell below (same thing via the SDK). Run only one of the two — a schema can have only one feed.
# MAGIC
# MAGIC Prerequisite: a workspace admin enabled the **Lakebase Change Data Feed** preview, and you have **CAN MANAGE** on the project.

# COMMAND ----------

from databricks.sdk.service.postgres import CdfConfig

DB_RES = database_resource()
existing = [c for c in w.postgres.list_cdf_configs(parent=DB_RES) if c.postgres_schema == PG_APP_SCHEMA]
if existing:
    CDF_CONFIG = existing[0].name
    print("Feed already running:", CDF_CONFIG)
else:
    op = w.postgres.create_cdf_config(
        parent=DB_RES,
        cdf_config=CdfConfig(catalog=CATALOG, schema=BRONZE, postgres_schema=PG_APP_SCHEMA),
    )
    CDF_CONFIG = op.wait().name
    print("Feed started:", CDF_CONFIG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 · Watch it stream
# MAGIC `SNAPSHOTTING` → `STREAMING`. Changes are flushed to Delta roughly every 15 seconds.

# COMMAND ----------

for _ in range(20):
    rows = pg("SELECT * FROM wal2delta.tables")
    print(rows)
    if rows and all("STREAMING" in str(r) for r in rows): break
    time.sleep(15)

# COMMAND ----------

display(spark.sql(f"""
  SELECT customer_id, credit_limit, updated_by, _pg_change_type, _pg_lsn, _timestamp
  FROM {HISTORY} ORDER BY _sort_by DESC"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### (Optional, great on camera) change it again and watch two new rows appear
# MAGIC An UPDATE produces `update_preimage` (old row) + `update_postimage` (new row).

# COMMAND ----------

pg(f"UPDATE {PG_APP_SCHEMA}.customer_overrides SET reason = 'Approved by Priya after call', updated_at = now() WHERE customer_id = %s",
   (DEMO_CUSTOMER,), fetch=False)
time.sleep(30)
display(spark.sql(f"SELECT customer_id, credit_limit, reason, _pg_change_type, _timestamp FROM {HISTORY} ORDER BY _sort_by DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Postgres changes now land in `retail.bronze.lb_customer_overrides_history`. Next: notebook **04** merges them into Gold.
