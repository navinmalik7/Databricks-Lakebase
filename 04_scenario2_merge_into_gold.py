# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Scenario 2 (part B) — MERGE the changes into Gold and close the loop
# MAGIC **Video: Part 2, Slides 8 & 10.**
# MAGIC
# MAGIC `bronze.lb_customer_overrides_history` → (latest change per customer) → **MERGE** into `gold.customer_360`
# MAGIC → Triggered synced table pushes it back to Postgres → Power BI (reads Gold) shows ₹75,000.

# COMMAND ----------

# MAGIC %pip install -q -U "databricks-sdk>=0.89.0" "psycopg[binary,pool]>=3.1"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Before: Gold still says ₹50,000

# COMMAND ----------

display(spark.sql(f"SELECT customer_id, credit_limit, override_by, override_at FROM {SOURCE_TABLE} WHERE customer_id = {DEMO_CUSTOMER}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## The MERGE job  *(Slide 10)*
# MAGIC For each micro-batch: **drop the "before" images**, keep the **latest** change per customer (`_sort_by`),
# MAGIC then MERGE into Gold. `availableNow` = runs like a normal scheduled job and stops when caught up.
# MAGIC The checkpoint makes sure nothing is applied twice.

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

def apply_overrides(batch_df, batch_id):
    latest = (batch_df
        .filter(F.col("_pg_change_type") != "update_preimage")            # drop "before" rows
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("customer_id").orderBy(F.col("_sort_by").desc())))
        .filter("rn = 1"))                                                 # latest change wins
    (DeltaTable.forName(spark, SOURCE_TABLE).alias("g")
        .merge(latest.alias("o"), "g.customer_id = o.customer_id")
        .whenMatchedUpdate(
            condition="o._pg_change_type <> 'delete'",
            set={"credit_limit": "coalesce(o.credit_limit, g.credit_limit)",
                 "segment":      "coalesce(o.segment, g.segment)",
                 "override_by":  "o.updated_by",
                 "override_at":  "o.updated_at"})
        .execute())

q = (spark.readStream.table(HISTORY)
        .writeStream.foreachBatch(apply_overrides)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)
        .start())
q.awaitTermination()
print("MERGE done.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## After: Gold says ₹75,000 — and who changed it

# COMMAND ----------

display(spark.sql(f"SELECT customer_id, credit_limit, override_by, override_at FROM {SOURCE_TABLE} WHERE customer_id = {DEMO_CUSTOMER}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Close the loop: Postgres follows Gold  *(Slide 8, step 4)*
# MAGIC The table-update job from notebook 02 fires on this Gold change. To not wait on camera, this cell also kicks the
# MAGIC sync pipeline directly (same as **Sync now** on the synced table's Overview tab).

# COMMAND ----------

pid = w.postgres.get_synced_table(name=f"synced_tables/{SYNCED_TABLE}").status.pipeline_id
try:
    w.pipelines.start_update(pipeline_id=pid)
    print("Sync started …")
except Exception as e:
    print("Sync already running (the job beat us to it):", str(e).splitlines()[0])

for _ in range(30):
    row = pg("SELECT customer_id, credit_limit, override_by FROM gold.customer_360_synced WHERE customer_id = %s", (DEMO_CUSTOMER,))
    print(row)
    if row and float(row[0][1]) == 75000: break
    time.sleep(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎬 Side-by-side moment
# MAGIC - **Postgres** (app reads): `gold.customer_360_synced` → 75,000
# MAGIC - **Gold** (Power BI reads): `retail.gold.customer_360` → 75,000
# MAGIC - Refresh the Power BI card (DirectQuery shows it immediately). **One truth.**

# COMMAND ----------

display(spark.sql(f"SELECT 'Gold (Power BI)' AS reader, credit_limit FROM {SOURCE_TABLE} WHERE customer_id = {DEMO_CUSTOMER}"))
print("Postgres (app):", pg("SELECT credit_limit FROM gold.customer_360_synced WHERE customer_id = %s", (DEMO_CUSTOMER,)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schedule this notebook (run once, off camera)
# MAGIC Runs whenever the history table changes — no polling. Change the path if you imported the notebooks elsewhere.

# COMMAND ----------

from databricks.sdk.service import jobs

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
job = w.jobs.create(
    name="freshcart · merge overrides into Gold",
    tasks=[jobs.Task(task_key="merge_overrides", notebook_task=jobs.NotebookTask(notebook_path=nb_path))],
    trigger=jobs.TriggerSettings(
        table_update=jobs.TableUpdateTriggerConfiguration(table_names=[HISTORY], min_time_between_triggers_seconds=60),
        pause_status=jobs.PauseStatus.UNPAUSED),
)
print(f"Job: {w.config.host}/jobs/{job.job_id}  (runs on serverless jobs compute if your workspace has it)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Fallback — no preview features allowed?  *(Slide 12 last line)*
# MAGIC Lakebase CDF is Public Preview. Without it: register the Lakebase database in Unity Catalog, then pull changed rows
# MAGIC with a watermark on `updated_at`. Simpler, but no deletes and no full history. Uncomment to use.

# COMMAND ----------

# from databricks.sdk.service.postgres import Catalog, CatalogCatalogSpec
# LB_CATALOG = "freshcart_lakebase"
# w.postgres.create_catalog(
#     catalog=Catalog(spec=CatalogCatalogSpec(postgres_database=PG_DATABASE, branch=branch_name())),
#     catalog_id=LB_CATALOG).wait()
#
# last = spark.sql(f"SELECT coalesce(max(override_at), timestamp'1970-01-01') m FROM {SOURCE_TABLE}").first().m
# changed = spark.sql(f"SELECT * FROM {LB_CATALOG}.{PG_APP_SCHEMA}.customer_overrides WHERE updated_at > timestamp'{last}'")
# (DeltaTable.forName(spark, SOURCE_TABLE).alias("g")
#    .merge(changed.alias("o"), "g.customer_id = o.customer_id")
#    .whenMatchedUpdate(set={"credit_limit": "coalesce(o.credit_limit, g.credit_limit)",
#                            "override_by": "o.updated_by", "override_at": "o.updated_at"})
#    .execute())
