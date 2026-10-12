# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Scenario 1 — Gold → Postgres with a synced table
# MAGIC **Video: Part 2, Slides 3 – 6.**
# MAGIC
# MAGIC `retail.gold.customer_360` → **synced table** (Triggered) → Postgres `freshcart_app.gold.customer_360_synced`
# MAGIC → Priya's Support Portal reads customer 1042 in milliseconds.

# COMMAND ----------

# MAGIC %pip install -q -U "databricks-sdk>=0.89.0" "psycopg[binary,pool]>=3.1"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 · Change Data Feed on the source  *(Slide 5: "First line…")*
# MAGIC Triggered and Continuous sync move only the changes, so the source must expose a change feed.
# MAGIC (Setup already did this — running it again is harmless and is the line you show on camera.)

# COMMAND ----------

spark.sql(f"ALTER TABLE {SOURCE_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
display(spark.sql(f"SHOW TBLPROPERTIES {SOURCE_TABLE} ('delta.enableChangeDataFeed')"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 · Create the synced table  *(Slide 5: "one call — create_synced_table")*
# MAGIC Point at: **source** · **branch** · **primary key** · **Triggered** · **Postgres database**.
# MAGIC
# MAGIC The UC schema name (`gold`) becomes the Postgres schema name → `gold.customer_360_synced`.
# MAGIC `create_database_objects_if_missing=True` also creates the `freshcart_app` database the first time.

# COMMAND ----------

from databricks.sdk.service.postgres import (
    SyncedTable, SyncedTableSyncedTableSpec,
    SyncedTableSyncedTableSpecSyncedTableSchedulingPolicy as Policy,
)

try:
    st = w.postgres.get_synced_table(name=f"synced_tables/{SYNCED_TABLE}")
    print("Synced table already exists:", st.name)
except Exception:
    st = w.postgres.create_synced_table(
        synced_table=SyncedTable(spec=SyncedTableSyncedTableSpec(
            source_table_full_name=SOURCE_TABLE,          # Gold table
            branch=branch_name(),                         # projects/freshcart/branches/production
            primary_key_columns=["customer_id"],          # no key, no sync
            scheduling_policy=Policy.TRIGGERED,           # incremental, fired by a job
            postgres_database=PG_DATABASE,                # freshcart_app
            create_database_objects_if_missing=True,
        )),
        synced_table_id=SYNCED_TABLE,
    ).wait()
    print("Synced table created:", st.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wait for the initial snapshot
# MAGIC `INITIALIZING` / `PROVISIONING` for a while is normal — the first load copies the whole table.

# COMMAND ----------

def wait_synced(timeout_s=900):
    t0 = time.time()
    while True:
        s = w.postgres.get_synced_table(name=f"synced_tables/{SYNCED_TABLE}").status
        state = s.detailed_state.name if s.detailed_state else "UNKNOWN"
        print(f"{int(time.time()-t0):>4}s  {state}  {s.message or ''}")
        if state.startswith("SYNCED_TABLE_ONLINE") and "UPDATING" not in state and "TRIGGERED_UPDATE" not in state:
            return s
        if "FAILED" in state or time.time() - t0 > timeout_s:
            raise RuntimeError(f"Sync not online: {state} — {s.message}")
        time.sleep(20)

status = wait_synced()
PIPELINE_ID = status.pipeline_id
print("Last sync:", status.last_sync_time, "| pipeline:", PIPELINE_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 · Query it like any Postgres app  *(Slide 6)*
# MAGIC No password anywhere: the SDK mints an OAuth token (≤ 1 hour) and we pass it to plain psycopg.
# MAGIC
# MAGIC ⚠️ Run this cell **once before recording** — the first query after scale-to-zero includes the compute waking up.

# COMMAND ----------

ep = rw_endpoint()
cred = w.postgres.generate_database_credential(endpoint=ep.name)   # OAuth token — never print it

with psycopg.connect(host=ep.status.hosts.host, dbname=PG_DATABASE, user=ME,
                     password=cred.token, sslmode="require") as conn:
    timings = []
    for _ in range(5):
        t0 = time.perf_counter()
        row = conn.execute(
            "SELECT customer_id, segment, credit_limit "
            "FROM gold.customer_360_synced WHERE customer_id = %s", (DEMO_CUSTOMER,)
        ).fetchone()
        timings.append((time.perf_counter() - t0) * 1000)

print(row)
print("query latency (ms):", [round(x, 1) for x in timings])
print(f"best: {min(timings):.1f} ms  (includes the network hop from this notebook)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 · Keep it fresh: a job that syncs whenever Gold changes  *(Slide 5: "Then go to Jobs…")*
# MAGIC On camera you can do this in the UI: **Jobs & Pipelines → Create job → + Add task → Database Table Sync pipeline →
# MAGIC pick the synced table's pipeline → Schedules & Triggers → Table update → `retail.gold.customer_360`**.
# MAGIC The cell below creates the same job with the SDK.

# COMMAND ----------

from databricks.sdk.service import jobs

job = w.jobs.create(
    name="freshcart · sync customer_360 to Lakebase",
    tasks=[jobs.Task(task_key="sync_customer_360",
                     pipeline_task=jobs.PipelineTask(pipeline_id=PIPELINE_ID))],
    trigger=jobs.TriggerSettings(
        table_update=jobs.TableUpdateTriggerConfiguration(table_names=[SOURCE_TABLE]),
        pause_status=jobs.PauseStatus.UNPAUSED,
    ),
)
print(f"Job created: {w.config.host}/jobs/{job.job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 · Prove the trigger works (optional on camera)
# MAGIC Change one Gold value → the table-update trigger fires the sync job → Postgres follows.
# MAGIC Uses customer 1043 so 1042 stays at ₹50,000 for Scenario 2.

# COMMAND ----------

spark.sql(f"UPDATE {SOURCE_TABLE} SET segment = 'Silver' WHERE customer_id = 1043")
print("Gold updated. Waiting ~90 s for the trigger + sync …")
time.sleep(90)
print(pg("SELECT customer_id, segment FROM gold.customer_360_synced WHERE customer_id = 1043"))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Scenario 1 done.** Gold is in Postgres, refreshed by a job, readable in milliseconds.
