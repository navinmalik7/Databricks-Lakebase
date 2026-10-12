# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Architect's upgrade — make overrides survive Gold rebuilds
# MAGIC **Video: Part 2, Slide 11.**
# MAGIC
# MAGIC 1. **Fragile:** rebuild Gold from Silver the "normal" way → Priya's ₹75,000 is wiped.
# MAGIC 2. **Durable:** overrides become a real source in Silver; the Gold build does `COALESCE(override, computed)`.
# MAGIC 3. Bonus: delete the override in Postgres → Gold reverts to the computed value automatically.

# COMMAND ----------

# MAGIC %pip install -q -U "databricks-sdk>=0.89.0" "psycopg[binary,pool]>=3.1"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

def show_1042(label):
    r = spark.sql(f"SELECT credit_limit, override_by FROM {SOURCE_TABLE} WHERE customer_id = {DEMO_CUSTOMER}").first()
    print(f"{label:<28} Gold #1042 credit_limit = {r.credit_limit}   override_by = {r.override_by}")

show_1042("Now:")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Fragile — the nightly rebuild from Silver
# MAGIC A typical Gold job that recomputes everything from Silver. It knows nothing about overrides.

# COMMAND ----------

spark.sql(f"""
MERGE INTO {SOURCE_TABLE} g
USING {COMPUTED} c ON g.customer_id = c.customer_id
WHEN MATCHED THEN UPDATE SET
  g.segment = c.segment, g.lifetime_value = c.lifetime_value, g.churn_risk = c.churn_risk,
  g.credit_limit = c.credit_limit, g.override_by = NULL, g.override_at = NULL
""")
show_1042("After naive rebuild:")     # → 50,000 again. Priya's change vanished.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Durable — overrides as a Silver source
# MAGIC Current state of every override, built from the CDF history (latest change wins, deletes drop out).
# MAGIC A view keeps the demo simple; in production make it a **materialized view** or a Lakeflow pipeline table.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {OVERRIDES_CURRENT} AS
SELECT * EXCEPT (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _sort_by DESC) AS rn
  FROM {HISTORY}
  WHERE _pg_change_type <> 'update_preimage')
WHERE rn = 1 AND _pg_change_type <> 'delete'
""")
display(spark.table(OVERRIDES_CURRENT).select("customer_id", "credit_limit", "segment", "updated_by", "updated_at"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The new Gold build: override first, computed second
# MAGIC A MERGE (not a CREATE OR REPLACE) so only changed rows produce change-feed events — cheap for the synced table.

# COMMAND ----------

GOLD_BUILD = f"""
MERGE INTO {SOURCE_TABLE} g
USING (
  SELECT c.customer_id, c.customer_name,
         COALESCE(o.segment, c.segment)           AS segment,
         c.lifetime_value, c.churn_risk,
         COALESCE(o.credit_limit, c.credit_limit) AS credit_limit,
         o.updated_by AS override_by, CAST(o.updated_at AS TIMESTAMP) AS override_at
  FROM {COMPUTED} c
  LEFT JOIN {OVERRIDES_CURRENT} o USING (customer_id)
) s
ON g.customer_id = s.customer_id
WHEN MATCHED AND (g.credit_limit <> s.credit_limit OR g.segment <> s.segment
                  OR g.override_by IS DISTINCT FROM s.override_by) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE
"""
spark.sql(GOLD_BUILD)
show_1042("After durable rebuild:")   # → 75,000, override_by = priya@…

# COMMAND ----------

# Run the rebuild again — it survives every time
spark.sql(GOLD_BUILD)
show_1042("After 2nd rebuild:")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Bonus — deleting the override reverts the value
# MAGIC The delete flows through CDF, drops out of `customer_overrides_current`, and the next build falls back to computed.

# COMMAND ----------

pg(f"DELETE FROM {PG_APP_SCHEMA}.customer_overrides WHERE customer_id = %s", (DEMO_CUSTOMER,), fetch=False)
print("Override deleted in Postgres. Waiting ~40 s for CDF …")
time.sleep(40)
spark.sql(GOLD_BUILD)
show_1042("After override delete:")    # → 50,000 (computed)

# COMMAND ----------

# Put Priya's override back so the rest of your recording / Power BI shows 75,000
pg(f"""INSERT INTO {PG_APP_SCHEMA}.customer_overrides (customer_id, credit_limit, reason, updated_by)
       VALUES (%s, 75000, 'Loyal, 3 yrs on-time payments', 'priya@freshcart.in')
       ON CONFLICT (customer_id) DO UPDATE SET credit_limit = EXCLUDED.credit_limit, updated_at = now()""",
   (DEMO_CUSTOMER,), fetch=False)
time.sleep(40)
spark.sql(GOLD_BUILD)
show_1042("Restored:")

# COMMAND ----------

# MAGIC %md
# MAGIC **No infinite loop:** Gold → Postgres lands in schema `gold` (synced, read-only). The feed back watches only schema `app`.
# MAGIC The overrides table is never written by the sync, so nothing bounces.
