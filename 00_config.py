# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Config & helpers — Lakebase Part 2 (FreshCart)
# MAGIC Shared names and helper functions. Every other notebook starts with `%run ./00_config`.
# MAGIC
# MAGIC **Edit only the cell below** if your catalog / project names differ.
# MAGIC (The calling notebook installs `databricks-sdk` and `psycopg` first.)

# COMMAND ----------

# ---- names used across the whole demo (match the video & slides) ----
CATALOG      = "retail"                    # UC catalog (must NOT use default storage if you want Lakebase CDF)
BRONZE, SILVER, GOLD = "bronze", "silver", "gold"

SOURCE_TABLE = f"{CATALOG}.{GOLD}.customer_360"          # Gold table Power BI reads
SYNCED_TABLE = f"{CATALOG}.{GOLD}.customer_360_synced"   # UC synced table → Postgres gold.customer_360_synced
COMPUTED     = f"{CATALOG}.{SILVER}.customer_computed"   # Silver source the Gold build reads
HISTORY      = f"{CATALOG}.{BRONZE}.lb_customer_overrides_history"   # created by Lakebase CDF
OVERRIDES_CURRENT = f"{CATALOG}.{SILVER}.customer_overrides_current"
CHECKPOINT   = f"/Volumes/{CATALOG}/{BRONZE}/chk/overrides_to_gold"

PROJECT_ID   = "freshcart"        # Lakebase project id (lowercase, digits, hyphens)
PG_DATABASE  = "freshcart_app"    # Postgres database (created by the synced table if missing)
PG_APP_SCHEMA = "app"             # schema the Support Portal owns (CDF watches ONLY this schema)

DEMO_CUSTOMER = 1042

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient
import psycopg

w = WorkspaceClient()
ME = w.current_user.me().user_name          # your Databricks identity = your Postgres login

def project_name():   return f"projects/{PROJECT_ID}"

def branch_name():
    """Resource name of the project's default branch (normally .../branches/production)."""
    proj = w.postgres.get_project(name=project_name())
    b = proj.status.default_branch
    return b if b.startswith("projects/") else f"{project_name()}/branches/{b}"

def rw_endpoint():
    """The read-write compute endpoint on the default branch."""
    eps = list(w.postgres.list_endpoints(parent=branch_name()))
    for ep in eps:
        if ep.status and ep.status.endpoint_type and ep.status.endpoint_type.name.endswith("READ_WRITE"):
            return ep
    return eps[0]

def pg_connect(database=PG_DATABASE):
    """Plain psycopg connection. Password = a fresh OAuth token (valid ≤ 1 hour). Never print it."""
    ep = rw_endpoint()
    cred = w.postgres.generate_database_credential(endpoint=ep.name)
    return psycopg.connect(host=ep.status.hosts.host, dbname=database, user=ME,
                           password=cred.token, sslmode="require", autocommit=True)

def pg(sql, params=None, database=PG_DATABASE, fetch=True):
    """Run one SQL statement on Lakebase and (optionally) return rows."""
    with pg_connect(database) as conn:
        cur = conn.execute(sql, params)
        return cur.fetchall() if (fetch and cur.description) else None

def database_resource(pg_db=PG_DATABASE):
    """Resource name of a Postgres database (needed by the CDF API)."""
    for d in w.postgres.list_databases(parent=branch_name()):
        if d.status and d.status.postgres_database == pg_db:
            return d.name
    raise ValueError(f"Database {pg_db} not found on {branch_name()} — run notebook 01 first.")

print("Config loaded ✔  user:", ME)
