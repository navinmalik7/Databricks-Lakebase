# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Cleanup — stop paying after recording
# MAGIC Lakebase compute, snapshot storage and sync pipelines are billed. Run the parts you want, top to bottom.
# MAGIC Nothing here runs unless you set `CONFIRM = True`.

# COMMAND ----------

# MAGIC %pip install -q -U "databricks-sdk>=0.89.0" "psycopg[binary,pool]>=3.1"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

CONFIRM = False          # ← set True to actually delete
DELETE_PROJECT = False   # also delete the whole Lakebase project (all branches, data)
DROP_DELTA = False       # also drop the retail.* demo tables

# COMMAND ----------

if CONFIRM:
    # 1. jobs created by notebooks 02 and 04
    for j in w.jobs.list(name=None):
        if j.settings and j.settings.name and j.settings.name.startswith("freshcart ·"):
            w.jobs.delete(job_id=j.job_id); print("deleted job", j.settings.name)

    # 2. CDF feed on schema app
    try:
        for c in w.postgres.list_cdf_configs(parent=database_resource()):
            w.postgres.delete_cdf_config(name=c.name).wait(); print("deleted CDF", c.name)
    except Exception as e: print("CDF:", e)

    # 3. synced table (also drops the Postgres copy)
    try:
        w.postgres.delete_synced_table(name=f"synced_tables/{SYNCED_TABLE}").wait(); print("deleted synced table")
    except Exception as e: print("synced table:", e)

    # 4. whole project
    if DELETE_PROJECT:
        w.postgres.delete_project(name=project_name()).wait(); print("deleted project", PROJECT_ID)

    # 5. Delta demo tables
    if DROP_DELTA:
        for t in (HISTORY, SOURCE_TABLE, COMPUTED):
            spark.sql(f"DROP TABLE IF EXISTS {t}"); print("dropped", t)
        spark.sql(f"DROP VIEW IF EXISTS {OVERRIDES_CURRENT}")
        dbutils.fs.rm(CHECKPOINT, True)
else:
    print("Dry run — set CONFIRM = True to delete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset for a retake (keeps everything, just puts #1042 back to the "before" state)
# MAGIC Deletes Priya's override, sets Gold #1042 back to ₹50,000 and re-syncs Postgres. Then re-run notebooks 03 → 04.
# MAGIC (The CDF history keeps the old events; the 04 checkpoint makes sure they aren't re-applied.)

# COMMAND ----------

RESET_FOR_RETAKE = False

if RESET_FOR_RETAKE:
    pg(f"DELETE FROM {PG_APP_SCHEMA}.customer_overrides WHERE customer_id = %s", (DEMO_CUSTOMER,), fetch=False)
    spark.sql(f"""UPDATE {SOURCE_TABLE} SET credit_limit = 50000, segment = 'Gold', override_by = NULL, override_at = NULL
                  WHERE customer_id = {DEMO_CUSTOMER}""")
    pid = w.postgres.get_synced_table(name=f"synced_tables/{SYNCED_TABLE}").status.pipeline_id
    try: w.pipelines.start_update(pipeline_id=pid)
    except Exception as e: print("sync already running:", str(e).splitlines()[0])
    time.sleep(60)
    print("Postgres now:", pg("SELECT credit_limit FROM gold.customer_360_synced WHERE customer_id = %s", (DEMO_CUSTOMER,)))
