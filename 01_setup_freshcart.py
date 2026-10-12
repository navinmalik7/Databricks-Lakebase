# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Setup — FreshCart data + Lakebase project
# MAGIC **Run once, before recording.** Not shown in the video (or show it as a 10-second B-roll).
# MAGIC
# MAGIC Creates:
# MAGIC - catalog `retail` with schemas `bronze`, `silver`, `gold` and a volume for streaming checkpoints
# MAGIC - `retail.silver.customer_computed` — 1,000 synthetic customers (the "computed" values)
# MAGIC - `retail.gold.customer_360` — the Gold table: **primary key** + **Change Data Feed on**
# MAGIC - the Lakebase project `freshcart` (if it doesn't exist yet)

# COMMAND ----------

# MAGIC %pip install -q -U "databricks-sdk>=0.89.0" "psycopg[binary,pool]>=3.1"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Catalog, schemas, checkpoint volume
# MAGIC If you can't create catalogs, create `retail` once in Catalog Explorer (with a **managed storage location
# MAGIC on an external location** — Lakebase CDF does not support catalogs on *default storage*), or change `CATALOG` in `00_config`.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for s in (BRONZE, SILVER, GOLD):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE}.chk")
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Silver: computed customer values (1,000 customers, #1042 is our hero row)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {COMPUTED} AS
SELECT
  CAST(id AS BIGINT)                                             AS customer_id,
  concat('Customer ', id)                                        AS customer_name,
  CASE WHEN id = {DEMO_CUSTOMER} THEN 'Gold'
       ELSE element_at(array('Bronze','Silver','Gold'), CAST(id % 3 AS INT) + 1) END AS segment,
  CASE WHEN id = {DEMO_CUSTOMER} THEN CAST(118900 AS DECIMAL(12,2))
       ELSE CAST(5000 + (id * 37) % 150000 AS DECIMAL(12,2)) END AS lifetime_value,
  CASE WHEN id = {DEMO_CUSTOMER} THEN CAST(0.04 AS DECIMAL(4,2))
       ELSE CAST(((id * 13) % 50) / 100.0 AS DECIMAL(4,2)) END   AS churn_risk,
  CASE WHEN id = {DEMO_CUSTOMER} THEN CAST(50000 AS DECIMAL(12,2))
       ELSE CAST(10000 * (1 + id % 6) AS DECIMAL(12,2)) END      AS credit_limit
FROM range(1001, 2001)
""")
display(spark.table(COMPUTED).where(f"customer_id = {DEMO_CUSTOMER}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Gold: `customer_360`
# MAGIC Three things the synced table needs, all done at create time:
# MAGIC 1. **Primary key** on `customer_id` (NOT NULL) — no key, no sync.
# MAGIC 2. **Change Data Feed** on — needed for Triggered / Continuous sync.
# MAGIC 3. **`override_by` / `override_at`** columns exist up front, so Scenario 2's MERGE never needs a schema change.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {SOURCE_TABLE} (
  customer_id     BIGINT NOT NULL,
  customer_name   STRING,
  segment         STRING,
  lifetime_value  DECIMAL(12,2),
  churn_risk      DECIMAL(4,2),
  credit_limit    DECIMAL(12,2),
  override_by     STRING,
  override_at     TIMESTAMP,
  CONSTRAINT customer_360_pk PRIMARY KEY (customer_id)
)
TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT 'FreshCart Gold customer 360 — read by Power BI, synced to Lakebase'
""")
spark.sql(f"""
INSERT INTO {SOURCE_TABLE}
SELECT customer_id, customer_name, segment, lifetime_value, churn_risk, credit_limit,
       CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP)
FROM {COMPUTED}
""")
display(spark.sql(f"SELECT * FROM {SOURCE_TABLE} WHERE customer_id = {DEMO_CUSTOMER}"))

# COMMAND ----------

# Prerequisite check: CDF + primary key
props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {SOURCE_TABLE}").collect()}
print("Change Data Feed:", props.get("delta.enableChangeDataFeed"))
display(spark.sql(f"""
  SELECT constraint_name, constraint_type FROM {CATALOG}.information_schema.table_constraints
  WHERE table_schema = '{GOLD}' AND table_name = 'customer_360'"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Lakebase project `freshcart`
# MAGIC In the video you show this in the **Lakebase app** (app switcher → Lakebase Postgres → *New project*).
# MAGIC This cell creates it only if it's missing, so the notebook also works end-to-end.
# MAGIC
# MAGIC After it exists: Lakebase app → `freshcart` → `production` branch → compute → **Edit** → autoscale **2 – 8 CU**, scale to zero **on**.

# COMMAND ----------

from databricks.sdk.service.postgres import Project, ProjectSpec

try:
    proj = w.postgres.get_project(name=project_name())
    print("Project already exists:", proj.name)
except Exception:
    print("Creating project", PROJECT_ID, "… (1–2 min)")
    proj = w.postgres.create_project(
        project=Project(spec=ProjectSpec(pg_version=17, display_name="FreshCart")),
        project_id=PROJECT_ID,
    ).wait()
print("Default branch:", branch_name())
ep = rw_endpoint()
print("Read-write endpoint:", ep.name)
print("Autoscale CU:", ep.status.autoscaling_limit_min_cu, "–", ep.status.autoscaling_limit_max_cu)

# COMMAND ----------

# Smoke test: can I log in to Postgres with an OAuth token? (connects to the default database)
print(pg("SELECT current_user, version()", database="databricks_postgres"))
