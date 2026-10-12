# Lakebase Part 2 — Notebooks & Step-by-Step Recording Guide
*Data Architect Diaries · matches `Lakebase_Part2_Practical_Teleprompter.md` (13 slides, ~8 min)*

---

## 1 · What's in the folder

| Notebook | Purpose | On camera? |
|---|---|---|
| `00_config` | All names (catalog, project, tables) + helpers: `pg()` runs Postgres SQL with an OAuth token, `branch_name()`, `rw_endpoint()` | No |
| `01_setup_freshcart` | Creates `retail.bronze/silver/gold`, 1,000 synthetic customers, `gold.customer_360` (**primary key + CDF on**), checkpoint volume, Lakebase project `freshcart` | No (optional 10-sec B-roll) |
| `02_scenario1_gold_to_postgres` | CDF on → `create_synced_table` (Triggered) → wait for snapshot → query #1042 with timings → job with **table-update trigger** | **Yes** — Slides 5–6 |
| `03_scenario2_postgres_side` | The trap (UPDATE on synced table fails) → `app.customer_overrides` → `REPLICA IDENTITY FULL` → Priya's upsert → start Lakebase CDF → watch history | **Yes** — Slides 7, 9 |
| `04_scenario2_merge_into_gold` | Streaming `foreachBatch` MERGE (latest change wins) → Gold = 75,000 → sync → Postgres = 75,000 → schedule as job · fallback without preview | **Yes** — Slides 8, 10 |
| `05_architect_upgrade` | Naive rebuild wipes the override → Silver `customer_overrides_current` + `COALESCE` Gold build survives → delete override reverts | **Yes** — Slide 11 |
| `06_cleanup` | Delete jobs / CDF feed / synced table / project (dry-run by default) + **reset for a retake** | No |

**Import:** Workspace → your folder → ⋮ → **Import** → *File* → select the `.py` files (or the `.zip`). Keep them **in one folder** (they use `%run ./00_config`).
**Compute:** serverless notebook compute (or any recent DBR cluster). Each notebook installs `databricks-sdk ≥ 0.89` and `psycopg` in its first cell.

---

## 2 · Things missing from your side (fix these before recording)

These aren't in the script or deck but the demo breaks — or looks wrong on camera — without them.

1. **Power BI isn't set up yet.** The whole story ends on "Power BI shows ₹75,000". You need Power BI Desktop connected to a **SQL warehouse** (Catalog → `customer_360` → *Use with BI tools*, or Partner Connect) with a single **Card visual**: `credit_limit` filtered to `customer_id = 1042`. Use **DirectQuery** — Import mode won't change until you refresh the dataset.
2. **The CDF destination catalog can't be on *default storage*.** Lakebase CDF refuses catalogs on default storage (this includes the catalog Free Edition creates for you), and catalogs whose storage is reachable only through a private endpoint. Create `retail` with a managed location on an external location.
3. **Preview + permission for Scenario 2.** A workspace admin must enable **Lakebase Change Data Feed** on the workspace *Previews* page, and your user needs **CAN MANAGE** on the `freshcart` project. Check this the day before, not on recording day.
4. **Order matters.** The Postgres database `freshcart_app` is created by the synced table (`create_database_objects_if_missing=True`). Run notebook **02 before 03**, or the overrides table has no database to live in.
5. **Start CDF on schema `app` only — never on `gold`.** CDF captures a *whole schema*. If you point it at `gold` (where synced tables land) you'd feed the synced copy back into the lakehouse — a loop. The script says "source schema `app`"; make sure you click that on camera.
6. **Gold needs `override_by` / `override_at` columns up front.** The MERGE writes them. `01_setup` creates them before the synced table exists, so no schema change hits the sync later.
7. **Warm up the compute before you hit record.** With scale-to-zero, the first query wakes the compute and won't be "milliseconds". Run notebook 02's query cell once, then record. Say "milliseconds" rather than a hard number — the notebook timing also includes the network hop.
8. **Pause the merge job while recording.** Notebook 04 schedules a job that fires when the history table changes. If it's live, Priya's change gets merged *before* you show the "before" state. Pause it in Jobs (or create it after recording).
9. **Use "Sync now" instead of waiting on camera.** The table-update trigger can take a minute or two. Notebook 04 kicks the sync pipeline directly; in the UI it's the **Sync now** button on the synced table's Overview tab.
10. **Hide sensitive things on screen.** Workspace URL, Lakebase host name, your email (it's your Postgres user). The notebooks never print the token — keep it that way. Blur in Filmora if needed.
11. **Costs.** Lakebase compute, snapshot storage (billable since June 2026) and sync pipelines cost money. Keep Triggered (not Continuous), leave scale-to-zero on, and run `06_cleanup` after the shoot.
12. **Retakes.** `06_cleanup` → set `RESET_FOR_RETAKE = True` → puts #1042 back to ₹50,000 everywhere. Then re-run 03 → 04.
13. **Deck vs notebook — one small difference.** Deck slide 16 shows a *materialized view*; notebook 05 uses a regular **view** (works on any compute). Say "in production, make it a materialized view" — that's already in the notebook's markdown.
14. **Link the code.** The script says "the full notebook is in the description" — push the folder to a GitHub repo (or attach the `.zip` to a community post) before you publish.

---

## 3 · Pre-production checklist (the day before)

- [ ] Admin: Lakebase enabled, **Lakebase Change Data Feed** preview enabled
- [ ] You: `CAN MANAGE` on project, `USE CATALOG / USE SCHEMA / CREATE TABLE` on `retail.*`
- [ ] Run `01_setup_freshcart` → confirm `Change Data Feed: true` and the `PRIMARY KEY` row
- [ ] Lakebase app → `freshcart` → `production` compute → autoscale **2–8 CU**, scale-to-zero on (matches Part 1 Slide 4)
- [ ] Do a full **dry run** of 02 → 03 → 04 → 05, then `RESET_FOR_RETAKE` (catches permission/preview issues early)
- [ ] Power BI Desktop: DirectQuery to SQL warehouse, Card = #1042 `credit_limit`, shows **50,000**
- [ ] Browser zoom 125–150 %, notebook in **Focus mode**, dark theme, results tables collapsed to the columns you need
- [ ] Close notifications; hide bookmarks bar; one browser tab per screen you'll show

**Recording-day state:** synced table exists (from the dry run), #1042 = 50,000 everywhere, override deleted, merge job **paused**, compute warmed.

---

## 4 · Shot list — slide by slide

Format: **Slide (time)** → what's on screen → what you click/run → what you say (first words from the script).

### Slide 1 · Cold open (0:00–0:45)
- **Screen:** face cam full, or face cam + Part 1 Slide 1 animation (`Slide1_Hook_Lakebase.mp4`, 0:00–0:20) as B-roll.
- **Do:** nothing technical.
- **Say:** "Customer 1042. Priya's support app says seventy-five thousand…"

### Slide 2 · Meet the example (0:45–1:05)
- **Screen:** deck slide 7, then cut to Catalog Explorer → `retail.gold.customer_360` → *Sample data*, filter `customer_id = 1042`.
- **Say:** "Quick recap of our cast…" Point at `customer_id` (PK icon) and `credit_limit = 50,000`.

### Slide 3 · Scenario 1 plan (1:05–1:30)
- **Screen:** deck slide 8.
- **Optional B-roll:** Lakebase app (app switcher → Lakebase Postgres) → `freshcart` project → `production` branch.

### Slide 4 · Pick a sync mode (1:30–2:05)
- **Screen:** deck slide 9. Optional B-roll: Catalog → `customer_360` → **Create → Synced table** dialog, open *Advanced settings* to show Snapshot / Triggered, then **Cancel** (we create it from code).

### Slide 5 · Code: create the synced table (2:05–2:50) — `02_scenario1_gold_to_postgres`
1. Run **Step 1** (ALTER TABLE … CDF) → show the `true` result. *"First line: turn on Change Data Feed…"*
2. Scroll to **Step 2** → highlight the five arguments while you name them (source, branch, PK, Triggered, database). Run it (says *already exists* if you did a dry run — fine, or delete it first in `06` for a fresh create).
3. **Cut** the wait. Show the "Wait for the initial snapshot" output ending in `SYNCED_TABLE_ONLINE…`.
4. *"Then go to Jobs…"* — either run **Step 4** and click the printed job link, or do it in the UI: Jobs & Pipelines → Create job → **Database Table Sync pipeline** task → pick the pipeline → Schedules & Triggers → **Table update** → `retail.gold.customer_360`.

### Slide 6 · Code: query it (2:50–3:20)
- Run **Step 3** → show `(1042, 'Gold', Decimal('50000.00'))` and the latency list.
- **Say:** "Notice — no password anywhere…" Point at `generate_database_credential`. End: "**Scenario one: done.**"

### Slide 7 · The trap (3:20–3:55) — `03_scenario2_postgres_side`
- Run **Step 1** → show the `permission denied` error on screen. Hold 2 seconds.
- **Say:** "Priya wants to change the credit limit… Don't." Then cut to deck slide 12 for the rule.

### Slide 8 · Write-back loop (3:55–4:30)
- **Screen:** deck slide 13 — trace the four steps with your pointer.

### Slide 9 · Postgres side (4:30–5:00)
1. Run **Step 2** (create table) and **Step 3** (REPLICA IDENTITY FULL) → output shows `full`. *"The line everyone forgets…"*
2. Run **Step 4** (Priya's upsert) → row shows 75000, `priya@freshcart.in`.
3. **Switch to the Lakebase app:** project → click branch name → **Lakebase CDF** tab → **Start** → Database `freshcart_app`, Schema **`app`** → Catalog `retail`, Schema `bronze` → **Start**. (Don't also run Step 5's SDK cell — one feed per schema.)
4. Show the **Tables** sub-tab going `Snapshotting → Streaming`, then run **Step 6** → `lb_customer_overrides_history` with `_pg_change_type = insert`.
5. *(High-retention moment)* run the optional UPDATE cell → two new rows: `update_preimage` / `update_postimage`.

### Slide 10 · MERGE into Gold (5:00–5:40) — `04_scenario2_merge_into_gold`
1. Run **Before** → Gold #1042 = 50,000.
2. Scroll the MERGE cell slowly; point at "drop before rows", "latest change wins", `availableNow`, `checkpointLocation`. Run it.
3. Run **After** → 75,000 and `override_by = priya@freshcart.in`.

### Back to Slide 8's payoff — the split screen
- Run **Close the loop** → Postgres prints `75000.00`.
- **Split screen in Filmora:** left = notebook output (Postgres 75,000), right = Power BI card refreshing from **50,000 → 75,000**.
- **Say:** "And Power BI? It reads Gold… **One truth.**"

### Slide 11 · Architect's upgrade (5:40–6:15) — `05_architect_upgrade`
1. Run **Fragile** → prints `50000` → *"…that MERGE gets wiped out."*
2. Run the view cell + **durable rebuild** → `75000, override_by = priya@…`. Run it again → still 75,000.
3. *(Optional, 10 sec)* Bonus delete → reverts to 50,000, then **Restored** → 75,000.

### Slide 12 · Gotchas (6:15–6:55)
- **Screen:** deck slide 17 (no notebook). Optional B-roll: `SELECT * FROM wal2delta.tables` output.

### Slide 13 · Recap & CTA (6:55–7:50)
- **Screen:** face cam; end screen links Part 1 + subscribe. Show the GitHub/notebook link on screen when you say "in the description".

---

## 5 · Filmora assembly

1. **Tracks:** V1 = screen recording, V2 = face cam (picture-in-picture, bottom-right, 22 % width), V3 = deck slides / Part 1 animations, A1 = voice.
2. **Cut every wait** (snapshot, sync, CDF streaming) — replace with a 0.5 s "⏩ 2 min later" text card.
3. **Zoom-ins:** keyframe 140–160 % on each highlighted code line (Slides 5, 9, 10). Hold 1.5 s on errors and on 50,000 → 75,000.
4. **Callouts:** red box on `permission denied`; green box on `75000.00`; arrow on `REPLICA IDENTITY FULL`.
5. **Blur** workspace URL, Lakebase host, email (Filmora → Effects → *Mosaic*, track the region).
6. **Captions:** Filmora *Auto Caption* from the voice track, then fix `Lakebase`, `psycopg`, `MERGE`, `CDF`.
7. Export 1920×1080, 30 fps, H.264, matching the Part 1 animation clips.

---

## 6 · If something goes wrong

| Symptom | Likely cause | Fix |
|---|---|---|
| `create_synced_table` → permission error | Missing `USE SCHEMA` / `CREATE TABLE` on `retail.gold` | Grant, or use a schema you own |
| Synced table `…OFFLINE_FAILED`, duplicate key | Duplicate `customer_id` in Gold | Dedupe, or set a timeseries key |
| `password authentication failed` | Token expired (> 1 h) or your user has no Postgres role | Re-run the cell (new token); add your identity under the project's **Roles** |
| CDF table never appears | Not `REPLICA IDENTITY FULL`, table empty, or partitioned | Re-run Step 3; insert a row; avoid partitioned tables |
| CDF "Start" fails on catalog | Destination catalog on default storage / private endpoint | Use a catalog on an external location |
| Gold updated but Postgres not | Triggered mode waits for a trigger | Run the sync cell in 04, or **Sync now** |
| Postgres shows 75,000 before you ran 04 | Merge job is live | Pause the job; `RESET_FOR_RETAKE` |
| Power BI still 50,000 | Import mode | Switch the card's table to DirectQuery, or refresh |

---

### Sources (checked Oct 2026)
- Synced tables: https://docs.databricks.com/aws/en/oltp/projects/sync-tables
- Lakebase Change Data Feed: https://docs.databricks.com/aws/en/oltp/projects/lakebase-cdf
- Connect with OAuth (SDK): https://docs.databricks.com/aws/en/oltp/projects/external-apps-connect
- Python SDK `w.postgres` reference: https://databricks-sdk-py.readthedocs.io/en/latest/workspace/postgres/postgres.html
