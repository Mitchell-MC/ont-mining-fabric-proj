# Ontario Mining Claims Lakehouse on Microsoft Fabric — Project Spec

| | |
|---|---|
| Repo | https://github.com/Mitchell-MC/ont-mining-fabric-proj |
| Status | Draft v0.1, 2026-11-09 |
| Effort | About 2 weekends of build, then passive daily loads for the rest of the 60-day Fabric trial |
| Platform | Microsoft Fabric (free trial capacity), Power BI, GitHub, Azure DevOps Pipelines, fabric-cicd |

## 1. Summary

An end-to-end lakehouse on Microsoft Fabric over Ontario's public mining-claims register:

- a metadata-driven Data Factory pipeline that loads the province's daily MLAS GIS extracts,
- PySpark bronze and silver layers with data-quality checks and a quarantine path,
- a gold star schema that keeps claim status and ownership history with SCD Type 2,
- a Direct Lake on OneLake semantic model secured with dynamic RLS and OLS,
- an analytical report and a parameterized paginated report,
- Git-based Dev → Test deployment with fabric-cicd and an approval gate.

Every performance or operational claim the project makes is backed by a measured number (section 13).

**Why this dataset.** It has about 411k active claims, refreshed daily, with real-world mess: packed multi-holder strings, truncated dBASE fields, test records, and a swapped code in a reference layer. It is also topical. Ontario's Bill 5 (*Protect Ontario by Unleashing our Economy Act, 2025*, royal assent 5 June 2025) amends the Mining Act to speed up mining permitting, so the claims register is a live view of exploration activity across the province.

## 2. Goals and non-goals

**Goals**

| # | Goal |
|---|---|
| G1 | Metadata-driven ingestion: one control table drives every source and layer; adding a layer means adding a row |
| G2 | Medallion layers with explicit, unit-tested data-quality rules and a quarantine path |
| G3 | Star schema with SCD2 history of claim status, due date and ownership |
| G4 | Direct Lake on OneLake semantic model that provably stays in Direct Lake, tuned with V-Order and OPTIMIZE |
| G5 | Governance: dynamic RLS by mining division, OLS on holder identity |
| G6 | One interactive analytical report and one parameterized paginated report |
| G7 | CI/CD: Fabric Git integration plus fabric-cicd, Dev → Test behind an approval gate, no manual steps after approval |
| G8 | Measured results for every metric in section 13 |

**Non-goals**

- Production use or SLAs. A trial capacity is for evaluation only.
- Legal or tenure interpretation. The source itself says to verify before relying on it.
- The Mining Claims Information database. It isn't open at present; Ontario is reviewing whether it can be.
- Streaming, machine learning, or a Prod workspace (Prod is a stretch goal).
- Redistributing source data (see 4.4).

## 3. What the project demonstrates

| Capability | Where it lives | Evidence captured |
|---|---|---|
| Data Factory in Fabric: Lookup → ForEach → Copy driven by metadata, incremental loads, retries, failure alerts | `pl_ingest_daily`, `lh_meta.ctl_*` | Run history; full vs incremental runtime; number of tables driven by one control table |
| Medallion architecture in PySpark | `nb_bronze_*`, `nb_silver_*`, `nb_dq` | `dq_results`, quarantine counts |
| Dimensional modelling with SCD2 | `nb_gold_*`, `lh_gold` | SCD2 changes per load |
| Direct Lake on OneLake, V-Order, OPTIMIZE | `sm_claims` | `TABLETRAITS()` output, Performance Analyzer, Parquet file counts |
| Dynamic RLS and OLS | `sm_claims` roles (TMDL in Git) | "View as" and Viewer-account tests per role |
| Analytical and paginated reporting | `rpt_claims_overview`, `prpt_claims_due` | Screenshots (subject to 4.4) |
| CI/CD | `deploy/`, Azure Pipelines, fabric-cicd | Items deployed, deploy duration, approval record |

## 4. Data sources (verified 2026-11-09)

Everything comes from [OGSEarth](https://www.geologyontario.mndm.gov.on.ca/ogsearth.html), in the *Mining Claims* and *Administrative Boundaries* sections. On 2026-11-09 both zip downloads below returned HTTP 200, and the operational zip downloaded in about 15 seconds.

### 4.1 Downloads

| `source_id` | URL | Size | Observed refresh | Use |
|---|---|---|---|---|
| `mlas_operational` | `https://www.geologyontario.mines.gov.on.ca/mmd/mines/documents/claimaps/mlas_operational_gis_data.zip` | 207 MB | Daily (Last-Modified 2026-11-09 14:45 UTC; layer files stamped about 07:50–08:10 local) | Claims, cancelled history, tenure, grid IDs |
| `mndm_admin` | `https://www.geologyontario.mines.gov.on.ca/mmd/mines/documents/claimaps/endm_administrative_gis_data.zip` | 640 MB | Infrequent (Last-Modified 2026-09-04) | Mining divisions, regions, townships and areas, cell grid |
| `amis` (stretch) | `https://www.geologyontario.mndm.gov.on.ca/mines/data/google/amis/AMIS.kml` | — | — | Abandoned-mines context layer |

The OGSEarth "Mining Claims (KML)" link is an 8.7 KB Google Earth network link, not a bulk extract. Don't use it.

### 4.2 Layers used

From `mlas_operational`. These are shapefiles in NAD83(CSRS) geographic coordinates (EPSG:4617). dBASE field names are truncated to 10 characters, so silver renames them through a mapping table.

| Layer | Rows on 2026-11-09 | Key fields | Notes |
|---|---|---|---|
| `Operational_Cell_Claims` | 411,363 | `TENURE_NUM`, `TITLE_TYPE`/`TITLE_TY_1`, `TENURE_STA`/`TENURE_S_1`, `ISSUE_DATE`, `ANNIVERSAR`, `EXTENSION_`, `CLAIM_DUE_`, `HOLDER` | One row per active claim. **No district field.** |
| `Operational_Cell_Claims_GRID_ID_<yyyymmdd>.csv` | 410,179 | Same fields plus `AREA_HA`, `GRID_ID` (e.g. `43E07A368`) | Date in the file name, so it must be matched by glob. Lags the shapefile (dated 2026-11-04). |
| `Cancelled_Claim_Polygons` | 440,443 | Adds `TENURE_HIS`, `REVISION_N`, `ENTRY_TIME`, `TERMINATIO`, `STATUS` | Termination history. Contains test records. |
| `Mining_Land_Tenure` (stretch) | 22,954 | Patents and leases, `EXPIRY_DAT`, `HOLDER` | `HOLDER` is truncated at 254 characters |
| `Legacy_Boundary_*_<yyyymmdd>` | — | — | Out of scope for v1. Date-suffixed file names. |

From `mndm_admin`:

| Layer | Rows | Use |
|---|---|---|
| `MENDM_Mineral_Exploration_Regions` | 3 (Northwest, Northeast, South) | Top of the district hierarchy |
| `MENDM_Mining_Divisions` | 9 (Kenora, Larder Lake, Patricia, Porcupine, Red Lake, Sault Ste. Marie, Southern Ontario, Sudbury, Thunder Bay) | RLS grain |
| `MENDM_Administrative_Boundaries` | 5,274 (2,530 geographic townships, 2,744 unsurveyed areas) | Lowest district grain |

### 4.3 Profile of the 2026-11-09 snapshot

These are baseline facts about the source, not project results.

- **Title types:** 382,379 single-cell, 21,423 boundary-cell and 7,561 multi-cell claims.
- **Statuses:** 9 distinct values.
  - Active: 401,266
  - Hold – Special Circumstances Apply: 6,684
  - Active Pending Proceedings: 2,715
  - Active Pending: 540
  - Hold Pending extension of time: 145
  - four other statuses, 13 claims in total
- **Due dates:** 31,971 claims fall due within 90 days (by 2027-02-07). 30,815 carry an extension date. About 3,900 have a due date before 2026, most likely holds; that is a data-quality warning, not an error.
- **`HOLDER` is a packed string,** for example `(49) COMPANY A CORP., (51) Company B Ltd.`. The ownership percentage sits in leading parentheses.
  - Some claims have 5 or more holders.
  - About 6.5% of claims have more than one holder.
  - Parsed percentages sum to 100 on every claim.
  - Holder names can themselves contain parentheses (`… (Canada) LTD.`). The parser must split on the leading `(pct)` tokens, not on brackets.
- **Holder identity:** there are about 1,300 distinct holder names, and holders include individual prospectors as well as companies.
  - The operational layer carries no client ID.
  - Cancelled history uses a different format, `(client_id) Name (pct%)`.
  - The holder natural key is therefore a normalized name, which risks name variants.
- **Reference-data defect:** in `MENDM_Mineral_Exploration_Regions`, Northwest is coded `NE` and Northeast is coded `NW`. The names match the polygon sizes (Northwest is the largest), so the codes are what's wrong.
- **Test rows:** cancelled history contains junk records, with holder names containing "Test" or "Do Not Use".

### 4.4 Terms of use: read before publishing anything

Each zip includes the MNDM *Electronic Information Products Terms of Use*. In summary (this is not legal advice):

- Non-commercial use of *unsubstantial excerpts* is allowed, provided credit is given and Crown copyright is acknowledged.
- *Substantial reproduction* needs prior written permission. The terms say this explicitly includes graphs, charts and maps.
- So does any commercial use, which the terms define to include creating value-added products.
- The contact for permission is Pubsales.ndm@ontario.ca.

Project rules:

1. **No source data in this repo, ever.** `.gitignore` blocks archives, shapefile parts, KML and `data/`.
2. Reports stay inside the Fabric workspace. Don't use Publish to web.
3. Public artefacts show engineering evidence (runtimes, file counts, DAX and `TABLETRAITS` output, architecture), not charts or maps of the data, unless written permission has been obtained. Ask on Day 0; it costs one email.
4. Attribution in the README and in report footers: *"Source: Government of Ontario, OGSEarth / MLAS GIS data. © King's Printer for Ontario."*
5. Holder names include private individuals. Treat them as personal information: protect them with OLS and take no holder-level screenshots.

## 5. Architecture

```
                 OGSEarth (public HTTPS, daily zips)
                                │
┌───────────────────────────────▼──── Fabric workspace omc-dev (trial capacity) ────────┐
│ pl_ingest_daily                                                                       │
│   nb_probe_sources → Lookup ctl_source → Filter → ForEach → Copy (HTTP → landing)     │
│        │                                           └→ nb_bronze_load (per object)     │
│        └─ on failure, at each stage: Teams / Outlook alert                            │
│   → nb_silver → nb_dq → nb_gold → refresh sm_claims → (Sun) nb_maintenance            │
│                                                                                       │
│ lh_meta    ctl_source, ctl_source_object, run_log, dq_results, sec_user_division      │
│ lh_bronze  Files/landing/…  +  raw layers, append-only by _snapshot_date              │
│ lh_silver  typed, conformed, spatially joined tables  +  quarantine_*                 │
│ lh_gold    fact_claim_snapshot, dim_claim (SCD2), dim_district, dim_holder,           │
│            bridge_claim_holder, dim_date             (V-Order on, OPTIMIZE weekly)    │
│                                                                                       │
│ sm_claims (Direct Lake on OneLake, RLS + OLS) → rpt_claims_overview, prpt_claims_due  │
│ sm_claims_import (Import-mode copy, benchmark only)                                   │
└───────────────────────────────▲───────────────────────────────────────────────────────┘
                                │ Fabric Git integration (main:/fabric)
   GitHub repo ──► Azure Pipelines: validate → [approval] → fabric-cicd ──► omc-test
```

- **Naming:** prefix `omc` (Ontario mining claims). Item prefixes are `lh_`, `nb_`, `pl_`, `sm_`, `rpt_`, `prpt_`, `env_` and `vl_`.
- **Compute:** one Environment item, `env_geo`, with pinned geopandas, pyogrio and shapely plus the project's helper wheel. If publishing custom libraries through Git or fabric-cicd isn't supported when you build, keep the wheel in `lh_meta/Files/lib/` and `%pip install` it from there.
- **Spark resource profile:** new workspaces default to `writeHeavy`, which has **V-Order off**. Bronze and silver keep that default. Gold notebooks set `spark.fabric.resourceProfile = readHeavyForPBI`, which turns V-Order on. This is an explicit step, not something you get by default.

## 6. Ingestion: `pl_ingest_daily`

### 6.1 Control tables (`lh_meta`)

`ctl_source`: one row per download.

| Column | Example / meaning |
|---|---|
| `source_id` | `mlas_operational` |
| `url` | Full HTTPS URL (4.1) |
| `format` | `zip` |
| `expected_cadence` | `daily`, `quarterly` |
| `enabled` | `true` |
| `last_modified_seen` | Set by the probe from the HTTP `Last-Modified` header |
| `content_length_seen` | Bytes; used to detect a changed or truncated file |
| `last_loaded_modified` | Watermark |
| `needs_load` | Set by the probe: `last_modified_seen > last_loaded_modified` |
| `landing_root` | `Files/landing/mlas_operational/` |

`ctl_source_object`: one row per layer, each producing one bronze table.

| Column | Example / meaning |
|---|---|
| `object_id` | `cell_claims` |
| `source_id` | `mlas_operational` |
| `member_glob` | `MLAS_Operational_GIS_Data/Operational_Cell_Claims.shp`, `*Cell_Claims_GRID_ID_*.csv` |
| `reader` | `shapefile`, `csv`, `kml` |
| `bronze_table` | `mlas_cell_claims` |
| `natural_key` | `TENURE_NUM` |
| `enabled` | `true` |

v1 enables 6 objects: cell claims, grid-ID CSV, cancelled claims, mining divisions, exploration regions and admin boundaries. Land tenure and AMIS are stretch rows. The metric "tables driven by one control table" is the count of enabled rows.

### 6.2 Flow

1. **`nb_probe_sources`** sends an HTTP HEAD request to each enabled URL and writes `last_modified_seen`, `content_length_seen` and `needs_load`. This step is a notebook because the pipeline Web activity only supports GET, POST, PUT, DELETE and PATCH, and caps responses at 4 MB.
2. **Lookup** reads `ctl_source` (Lakehouse table source). A **Filter** activity keeps `enabled && (needs_load || force_full)`.
3. **ForEach** (parallel, batch count 2) runs a **Copy**: HTTP (anonymous) binary to `lh_bronze/Files/landing/{source_id}/{yyyy-MM-dd}/`, with ZipDeflate decompression in the copy. If decompression fails on the 640 MB archive, unzip in the bronze notebook instead.
   - Retry: 3 attempts, 120 s apart.
   - Timeout: 30 minutes.
4. On Copy success, the watermark is updated and a `run_log` row is written. Skipped sources are logged as `skipped`.
5. **`nb_bronze_load`** loops over the `ctl_source_object` rows for sources that landed in this run. It reads each layer and appends to its bronze table with `_snapshot_date`, `_source_file`, `_source_last_modified` and `_ingested_at`. Geometry is kept as WKB, plus centroid longitude and latitude.
6. **`nb_silver` → `nb_dq` → `nb_gold`** run in order. Each one writes row counts to `run_log`.
7. A **Semantic model refresh** activity reframes `sm_claims`.
8. **Sunday branch:** `nb_maintenance` records `DESCRIBE DETAIL` (file count and size), runs `OPTIMIZE … VORDER` and `VACUUM` (7-day retention), then records the file counts again. It is a notebook, not the Lakehouse maintenance activity, so the before and after numbers for M4 are captured in the same step.
9. **Alerts.**
   - A Teams (or Outlook) activity sits on the failure path of each stage. Use one alert per monitored activity: an alert wired to several activities fires only when *all* of them fail.
   - A daily success message carries row counts and data-quality failures.
   - Outlook and Teams activities that use user-auth connections go **inactive in the target workspace after deployment** until the connection is re-created there. This is on the Test setup checklist.

**Schedule:** daily at 11:00 America/Toronto. The source refreshes between about 08:00 and 10:00 ET.

### 6.3 What "incremental" means here

The source publishes full snapshots, so incrementality works at three levels:

- **File:** unchanged sources are skipped by watermark. The 640 MB admin zip changes every few months, so most daily runs skip it.
- **Silver:** `MERGE` on the natural key with a row hash, so unchanged rows aren't rewritten. That keeps the Delta log small and Direct Lake framing incremental.
- **Gold:** SCD2 touches only the claims that changed.

`force_full = true` re-downloads everything and rebuilds silver and gold from bronze. That is the "full" side of the M1 benchmark.

## 7. Bronze and silver

**Bronze** holds raw layers as landed, append-only. The claims tables are partitioned by `_snapshot_date`, about 60 partitions over the trial. Reference layers are overwritten in place, with history kept by Delta time travel.

**Silver**

| Table | Grain | Notes |
|---|---|---|
| `claim` | `tenure_num` (current state) plus `row_hash` | Renamed and typed columns; status code and description; `area_ha` and `grid_id` from the grid CSV; centroid; `admin_area_id`, `mining_division` and `region` from the spatial join |
| `claim_holder` | `tenure_num` × holder | Parsed from `HOLDER`, with `ownership_pct` |
| `holder` | Normalized holder name | `holder_type` heuristic: a company-suffix regex (LTD, INC, CORP, LIMITED, …) means company, otherwise individual. Includes an alias table for known variants. |
| `claim_termination` | `tenure_num` × revision | From the cancelled layer, minus test rows |
| `admin_area`, `mining_division`, `region` | Reference | Region codes corrected through a mapping table; each correction is logged |
| `quarantine_<entity>` | Failed rows plus `rule_id` and `run_id` | Kept for 30 days |

**Spatial join.** Claim centroids are computed in a projected CRS (EPSG:3161, NAD83 / Ontario MNR Lambert) and then converted back to longitude and latitude. They are joined with geopandas `sjoin` on the driver to admin areas (5,274) and mining divisions (9). 411k points is a single-node job. Each admin area gets its division once per admin refresh.

**Holder parser.** This is pure Python in `src/`, unit-tested in CI.

- Split on the regex `(?:^|,\s*)\((\d+(?:\.\d+)?)\)\s*`.
- Normalize each name: upper-case, collapse whitespace, strip punctuation at the ends.
- Validate that the percentages sum to 100 ± 0.5.

### 7.1 Data-quality rules

| Rule | Check | Severity | Action |
|---|---|---|---|
| DQ01 | `TENURE_NUM` null or duplicated within a snapshot | error | quarantine |
| DQ02 | `HOLDER` doesn't parse, or percentages don't sum to 100 ± 0.5 | error | quarantine |
| DQ03 | Holder looks like a test record (`test`, `do not use`) | error | quarantine |
| DQ04 | Invalid or empty geometry, centroid outside Ontario, or no mining-division match | error | quarantine |
| DQ05 | Unknown status or title-type code | error | quarantine |
| DQ06 | `HOLDER` length ≥ 254 (truncated) | warn | flag |
| DQ07 | Active claim with a past due date and no extension | warn | flag |
| DQ08 | Claim in the shapefile but not in the grid CSV, or the reverse | warn | flag; derive `area_ha` from geometry |
| DQ09 | Reference code doesn't match its name (region codes) | warn | correct through mapping; log |
| DQ10 | Row count drifts more than 5% from the previous snapshot | warn | alert; don't block |

Every run writes `dq_results(run_id, rule_id, entity, rows_checked, rows_failed, action)`.

Expect low quarantine counts on the active register. Today there are 0 duplicate tenure numbers and 0 claims whose percentages don't sum to 100. That is the honest result. The test rows in cancelled history and the region-code defect give the rules something real to catch.

## 8. Gold star schema

```
                         dim_date
                            │
dim_district ──< fact_claim_snapshot >── dim_claim (SCD2) ──< bridge_claim_holder >── dim_holder
```

| Table | Grain / keys | Columns |
|---|---|---|
| `fact_claim_snapshot` | One row per active claim per weekly snapshot (the first successful load of each ISO week) | `snapshot_date_key`, `claim_key` (the version valid at the snapshot), `district_key`, `due_date_key`, `area_ha`, `days_to_due`, `is_due_90d`, `claim_count` = 1 |
| `dim_claim` | SCD2: one row per `tenure_num` per version | `claim_key` (surrogate), `tenure_num`, `title_type`, `status_code`, `status_desc`, `issue_date`, `anniversary_date`, `extension_date`, `due_date`, `holder_set_hash`, `effective_from`, `effective_to`, `is_current`, `change_reason` |
| `dim_district` | One row per township or area, plus an unknown member | `district_key`, `admin_area_id`, `admin_area_name`, `admin_area_type`, `mining_division`, `region` |
| `dim_holder` | One row per normalized holder | `holder_key`, `holder_name`, `holder_type` |
| `bridge_claim_holder` | `claim_key` × `holder_key` | `ownership_pct` |
| `dim_date` | Day | Date, ISO week start, month, quarter, Ontario fiscal year (April–March) |
| `sec_user_division` (in `lh_meta`) | UPN × mining division | RLS mapping; hidden in the model |

**SCD2 rules**

- **Tracked attributes** (a change creates a new version):
  - status
  - due date
  - anniversary date
  - extension date
  - title type
  - holder set, as a hash of the sorted normalized holder names and percentages
- **Type 1 attributes** (overwritten in place): issue-date corrections and `area_ha`.
- **Claims that drop off the register.** When a claim is missing from today's register, its current version is closed and a terminal version is inserted. `status_desc` comes from `claim_termination` where available (for example Cancelled or Forfeited), otherwise `Removed from register`.
- **Detection cadence.** Change detection runs **daily**, so short-lived states such as *Active Pending Transfer* are caught. The fact table snapshots **weekly** to keep its size bounded: about 411k rows × about 8 weeks ≈ 3.3M rows over the trial.
- **Logging.** Each load writes new claims and closed versions to `run_log`, broken down by `change_reason`.
- **Backfill (stretch).** Closed historical versions can be backfilled from the 440k-row cancelled layer, so history doesn't start on Day 1.

**Physical layout**

- No partitioning on gold. Every table is under 10M rows, so partitioning would only add small files.
- V-Order is on.
- Writes use `MERGE`, never a full overwrite, so Direct Lake framing stays incremental.

## 9. Semantic model: `sm_claims`

- **Storage mode:** Direct Lake on OneLake over `lh_gold`. It is Microsoft's recommended Direct Lake option for new models. It runs only in Direct Lake (no DirectQuery fallback).
- **Connection:** a fixed-identity cloud connection, which Microsoft recommends for RLS on Direct Lake. Report viewers get no access to the lakehouse itself.
- **Relationships:** a single-direction star. The holder bridge is resolved inside measures (`CROSSFILTER`), not with bi-directional relationships.
- **Starter measures:**
  - Active Claims
  - Claim Area (ha)
  - Claims Due in N Days (what-if parameter, default 90)
  - New Claims
  - Status Changes
  - Claims Removed
  - Ownership-Weighted Claims
  - WoW Δ Active Claims
- **Source of truth:** TMDL in Git (PBIP format). Roles and OLS are defined in TMDL. Power BI Desktop has no OLS UI, so use TMDL view or Tabular Editor.

### 9.1 Proving the model stays in Direct Lake

- Run `EVALUATE TABLETRAITS()` in DAX query view. `[DirectLakeFallbackInfo]` must be `None` for every table. Save the output to `docs/evidence/`.
- In Performance Analyzer, no visual may show a "Direct query" line.
- Use `INFO.STORAGETABLECOLUMNSEGMENTS()` to check column residency before and after framing. This backs the cold vs warm numbers (M3) and shows that daily MERGEs keep framing incremental.

### 9.2 Design decision: security lives in the semantic model, not the SQL endpoint

RLS or OLS on the SQL analytics endpoint does not help a Direct Lake model:

- **Direct Lake on SQL:** a table with endpoint RLS falls back to DirectQuery, or the query fails if fallback is disabled. Endpoint OLS or CLS returns errors.
- **Direct Lake on OneLake:** the endpoint isn't in the query path, so SQL RLS is simply not applied.

Either way, endpoint rules are the wrong tool for this model, so RLS and OLS are defined in `sm_claims`. OneLake security is the other supported option and is not used here.

**Experiment E1** (about an hour) demonstrates this:

1. Create `sm_claims_dlsql` (Direct Lake on SQL) over the same tables.
2. Add a T-SQL security policy on `fact_claim_snapshot` using `sql/endpoint_rls_demo.sql`.
3. Capture `TABLETRAITS()` reporting the fallback reason, and Performance Analyzer showing DirectQuery.
4. Show that `sm_claims` ignores the policy.
5. Drop the policy and the demo model.

## 10. Security and governance

RLS and OLS for the same user must be defined in the same role. Combining them across different roles fails at query time.

| Role | RLS | OLS |
|---|---|---|
| `Division Analyst` | `dim_district[mining_division]` limited to the user's divisions in `sec_user_division`, matched on `USERPRINCIPALNAME()` | `dim_holder[holder_name]`, `dim_holder[holder_type]` and `bridge_claim_holder[ownership_pct]` hidden (`None`) |
| `Division Analyst (Holders)` | Same as above | None |
| `Province Analyst` | None | None |

- **RLS and OLS apply only to workspace Viewers.** Admin, Member and Contributor roles bypass them. Test with "View as role" and with Viewer-role test accounts.
- **Test users:** three Entra users in the project tenant: a Red Lake analyst, a Sudbury + Porcupine analyst and a province analyst.
- **Holder visuals:** these go on a page that only holder-enabled roles use. For other roles the secured fields don't exist, and any visual that references them shows an error, so keep them off shared pages. Measures that reference secured columns are secured automatically.
- **Other controls:**
  - workspace roles: Dev has you; Test has the service principal and you
  - endorsement: `sm_claims` marked Promoted in Test
  - item descriptions on every item
  - a lineage-view screenshot

## 11. Reports

### `rpt_claims_overview`

A Power BI thin report on `sm_claims`, with four pages:

1. **Overview:** active claims and area by region and division, with a weekly trend.
2. **Expiry outlook:** due dates by month over the next 12 months, by division and title type.
3. **Change log:** derived from SCD2: new claims, status changes and removals per week, with drill-through to the claim.
4. **Holders** (holder-enabled roles only): ownership concentration and the share of claims with multiple holders.

An optional map plots claim centroids on the Azure Maps visual, which needs its tenant setting enabled. Maps count as substantial reproduction under 4.4, so the map is internal only.

### `prpt_claims_due` (paginated)

Built in Power BI Report Builder on `sm_claims`. Title: *Claims due in the next N days by mining division*.

- **Parameters:**
  - `DaysAhead`: 30, 60, 90 or 180; default 90
  - `MiningDivision`: multi-select; the list is limited by RLS
  - `TitleType`: optional
- **Layout:**
  - grouped by division, then by township or area
  - columns: tenure number, title type, status, due date, days remaining and area
  - group subtotals and a page break per division
  - a header showing the snapshot date and the parameter values
- **Exports:** PDF and Excel. RLS applies, and the Division Analyst version has no holder column.
- **Volume:** today's snapshot gives 31,971 rows at 90 days across the province. Test pagination and render time at that size.

## 12. CI/CD

- **Fabric Git integration:** `omc-dev` syncs with GitHub `main`, folder `/fabric`. It uses a fine-grained GitHub PAT with read/write access to contents on this repo only.
- **CD with Azure Pipelines** (`deploy/azure-pipelines.yml`), connected to GitHub through a service connection. It triggers on changes under `fabric/**` on `main`. Stages:
  1. **Validate:** `pytest` (holder parser, DQ rules, SCD2 change detection on fixture data) and a lint of the YAML and parameter files.
  2. **DeployTest:** a deployment job bound to the Azure DevOps Environment `omc-test`, with an **Approvals** check. It runs `deploy/deploy.py`, which calls fabric-cicd `publish_all_items()` and then `unpublish_all_orphan_items()` with an explicit `item_type_in_scope`.
  3. **SmokeTest:** starts `pl_ingest_daily` in Test through the Fabric job API and polls until it finishes. Then it runs a DAX row-count query through the `executeQueries` REST API. The stage fails if counts are zero or data-quality errors exceed a threshold.
- **Auth:**
  - Entra app registration `sp-omc-deploy`, with its secret in an Azure DevOps secret variable group.
  - A tenant setting that allows service principals to use Fabric APIs.
  - The service principal is Admin on `omc-test` (narrow to Contributor later).
- **Environment differences:**
  - `deploy/parameter.yml` holds `find_replace` rules using tokens such as `$items.Lakehouse.lh_gold.$id` and `$workspace.$id`.
  - The `vl_settings` Variable Library holds the alert recipient, the schedule and the `force_full` default.
  - No GUIDs are hard-coded in notebooks.
- **Item types in scope:** Lakehouse, Environment, VariableLibrary, Notebook, DataPipeline, SemanticModel, Report and PaginatedReport. fabric-cicd supports all of them. Lakehouse *data* is not deployed; Test loads its own data from the source on its first run.
- **One-time Test setup:** documented, and not counted as deployment steps:
  - create the Teams or Outlook connection
  - bind the fixed-identity cloud connection to `sm_claims`
  - assign role members
  - enable the schedule
- **Agent fallback:** a new Azure DevOps organization may have to request the free Microsoft-hosted parallel job, which can take a few business days. Request it on Day 0. Until it arrives, use a self-hosted agent on your laptop, or GitHub Actions with an environment protection rule (free on public repos).

## 13. Metrics to record

Write results to `docs/metrics.md` and `lh_meta.run_log`. Report the median of at least 3 runs, and record the date, the capacity (trial) and the data volume. Each write-up covers what was measured, how it was measured, the number, and any caveat.

| # | Metric | Method | Result |
|---|---|---|---|
| M1 | Full vs incremental pipeline runtime, and the number of tables one control table drives | `pl_ingest_daily` with `force_full=true` vs the default daily run, end to end, from Monitoring hub; count of enabled `ctl_source_object` rows | TBD |
| M2 | SCD2 changes captured per load | `run_log` from `nb_gold`: new claims and closed versions by `change_reason`, summed per week | TBD |
| M3 | Visual render time, cold vs warm | Performance Analyzer on 3 fixed visuals. *Cold* is the first render after reframing that follows a full gold rebuild; confirm segments aren't resident with `INFO.STORAGETABLECOLUMNSEGMENTS()`. *Warm* is the third consecutive render. | TBD |
| M4 | Parquet file count before and after OPTIMIZE | `DESCRIBE DETAIL` file count and size for silver and gold tables after at least 14 daily loads, then `OPTIMIZE … VORDER`; repeat the M3 cold run | TBD |
| M5 | Import refresh time vs Direct Lake framing time | `sm_claims_import` vs `sm_claims` on identical gold tables; refresh through the REST API; duration from refresh history | TBD |
| M6 | Items deployed and deploy duration, with no manual steps | Item count from the fabric-cicd log; DeployTest stage duration, excluding time waiting for approval | TBD |
| M7 | Rows quarantined by data-quality checks | `dq_results` per run, by rule | TBD |
| E1 | SQL endpoint RLS forces fallback (Direct Lake on SQL) | `TABLETRAITS()` output and Performance Analyzer capture | TBD |

## 14. Plan

**Day 0** (a weeknight, about 2 hours)

1. Email Pubsales.ndm@ontario.ca asking for permission to publish charts (4.4).
2. Set up the tenant. If you don't have a work account, create an Azure free account with a personal email, create a user in Microsoft Entra ID, sign in to Fabric as that user, and start the 60-day trial. Pick a Canadian region if one is offered.
3. Create the `omc-dev` and `omc-test` workspaces on the trial capacity.
4. Enable the service-principal tenant setting and create the app registration.
5. Create the Azure DevOps organization and project, and request hosted parallelism, which has a lead time.
6. Connect `omc-dev` to this repo.

**Weekend 1: data**

- *Saturday:*
  - control tables and seed rows
  - `env_geo`
  - `pl_ingest_daily`: probe, Lookup, Filter, ForEach, Copy, alerts
  - `nb_bronze_load`
  - one forced full run
- *Sunday:*
  - silver: renames, types, the holder parser with its tests, the spatial join
  - the data-quality rules
  - gold dimensions, SCD2, the fact table and `dim_date`
  - **Turn on the daily schedule before you stop.** Every day it isn't running is SCD2 history you won't have.

**Weekdays in between** (passive): check each daily run, fix any data-quality surprises, and note the `run_log` numbers.

**Weekend 2: serve and ship**

- *Saturday:*
  - `sm_claims`, its measures and the RLS/OLS roles in TMDL
  - `TABLETRAITS()` and Performance Analyzer evidence
  - `sm_claims_import` and M5
  - experiment E1
  - both reports
- *Sunday:*
  - `deploy/` scripts and `parameter.yml`
  - the Azure pipeline with its approval check
  - the first Dev → Test deploy (M6)
  - OPTIMIZE (M4)
  - README results

**Until about Day 50:**

- Daily loads keep running. Refresh M2, M4 and M7 at Day 30 and Day 45.
- By Day 55, export the evidence (screenshots, plus `run_log` and `dq_results` as CSV) to a private location. Code is already in Git.
- When the trial expires, workspaces fall back to Pro and non-Power BI items become inactive. OneLake content is kept for only 7 days after that.

Example calendar, if Day 0 is Friday 2026-11-13:

| Milestone | Date |
|---|---|
| Weekend 1 | 14–15 November |
| Weekend 2 | 21–22 November |
| Day 45 | about 28 December |
| Trial ends | about 12 January 2027 |

## 15. Acceptance criteria

- [ ] Adding a layer needs only a row in `ctl_source_object`, demonstrated by enabling one stretch layer.
- [ ] The daily schedule has run for at least 14 consecutive days, and alerts have been tested with a forced failure.
- [ ] Incremental runs skip unchanged sources (`run_log` shows `skipped`).
- [ ] `dq_results` is written on every run, and quarantine tables fill where rules fire.
- [ ] `dim_claim` has at least one closed version for each `change_reason` observed.
- [ ] `TABLETRAITS()` shows `None` for every table in `sm_claims`.
- [ ] RLS and OLS are verified for each role with Viewer test accounts.
- [ ] The paginated report renders 90 days across all divisions to PDF.
- [ ] Dev → Test runs from commit to a working Test workspace with only the approval click.
- [ ] All seven metrics are in `docs/metrics.md` with method and caveat.
- [ ] The public repo contains no source data, and no data charts or maps unless permission has been obtained.

## 16. Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Source URL, format or file names change | Medium | Globs in `ctl_source_object`; the probe alerts on 404s and on size changes; the last good landing is kept |
| Terms of use restrict public charts and maps | High (it's in the terms) | Rules in 4.4; ask for permission on Day 0 |
| Fabric trial not available with a personal email | Low to medium | Documented Azure + Entra-user route; a Microsoft 365 trial tenant as backup |
| Trial expires mid-project | Certain at Day 60 | Front-load the build; export evidence by Day 55 |
| Delay in the Azure DevOps hosted-agent grant | Medium | Request on Day 0; fall back to a self-hosted agent or GitHub Actions |
| Outlook or Teams activity inactive after deployment | High | Test setup checklist; consider a webhook-based Teams alert |
| Holder name variants split one holder into several | Medium | Normalization plus an alias table; compare distinct counts before and after |
| Few SCD2 changes in a short window | Medium | Daily change detection; optional backfill from cancelled history |
| Changes to preview or fast-moving features (Direct Lake on OneLake, Variable Libraries, paginated reports in Git) | Medium | Check the docs when you build; record versions in `docs/decisions/` |

## 17. Repo layout (target)

```
ont-mining-fabric-proj/
├── README.md
├── SPEC.md
├── fabric/                          # Fabric Git integration root (omc-dev ↔ main)
│   ├── lh_meta.Lakehouse/  lh_bronze.Lakehouse/  lh_silver.Lakehouse/  lh_gold.Lakehouse/
│   ├── env_geo.Environment/
│   ├── vl_settings.VariableLibrary/
│   ├── nb_probe_sources.Notebook/  nb_bronze_load.Notebook/  nb_silver.Notebook/
│   ├── nb_dq.Notebook/  nb_gold.Notebook/  nb_maintenance.Notebook/
│   ├── pl_ingest_daily.DataPipeline/
│   ├── sm_claims.SemanticModel/  sm_claims_import.SemanticModel/
│   ├── rpt_claims_overview.Report/
│   └── prpt_claims_due.PaginatedReport/
├── src/omc/                         # pure-Python helpers: holder parser, DQ rules, SCD2 diff
├── tests/                           # pytest + small synthetic fixtures (no source data)
├── deploy/
│   ├── deploy.py
│   ├── parameter.yml
│   └── azure-pipelines.yml
├── sql/endpoint_rls_demo.sql        # experiment E1
└── docs/
    ├── metrics.md
    ├── data-dictionary.md
    ├── decisions/                   # short ADRs
    └── evidence/                    # TABLETRAITS output, timings, deploy logs
```

## 18. Decisions

| # | Decision | Alternative considered |
|---|---|---|
| D1 | GitHub for source control (the repo already exists) and Azure Pipelines for CD (Azure DevOps Environments provide the approval gate) | Azure Repos as Fabric's Git provider; GitHub Actions for CD |
| D2 | Direct Lake on OneLake | Direct Lake on SQL, used only in experiment E1 |
| D3 | Daily change detection with a weekly fact snapshot | Daily fact (about 25M rows over the trial) |
| D4 | District = township or area, with a mining-division → region hierarchy; RLS at mining division | RLS at region (too coarse) |
| D5 | Holder natural key = normalized name, because the source has no client ID | Parse client IDs from cancelled history (inconsistent formats) |

## Appendix: references

- OGSEarth downloads: https://www.geologyontario.mndm.gov.on.ca/ogsearth.html
- Direct Lake overview: https://learn.microsoft.com/fabric/fundamentals/direct-lake-overview
- How Direct Lake works (fallback, `TABLETRAITS`): https://learn.microsoft.com/fabric/fundamentals/direct-lake-how-it-works
- Direct Lake security integration: https://learn.microsoft.com/fabric/fundamentals/direct-lake-security-integration
- Analyze Direct Lake query processing: https://learn.microsoft.com/fabric/fundamentals/direct-lake-analyze-query-processing
- Direct Lake query performance and storage: https://learn.microsoft.com/fabric/fundamentals/direct-lake-understand-storage
- V-Order: https://learn.microsoft.com/fabric/data-engineering/delta-optimization-and-v-order
- Resource profiles: https://learn.microsoft.com/fabric/data-engineering/resource-profiles-overview
- Object-level security: https://learn.microsoft.com/fabric/security/service-admin-object-level-security
- Pipeline alerts: https://learn.microsoft.com/fabric/data-factory/create-alerts-for-pipeline-runs
- Web activity: https://learn.microsoft.com/fabric/data-factory/web-activity
- fabric-cicd: https://microsoft.github.io/fabric-cicd/latest/
- fabric-cicd with Azure DevOps: https://learn.microsoft.com/fabric/cicd/tutorial-fabric-cicd-azure-devops
- Fabric trial: https://learn.microsoft.com/fabric/fundamentals/fabric-trial
- Fabric trial with a personal email: https://learn.microsoft.com/fabric/fundamentals/free-trial-account-personal-email
- Bill 5 overview: https://en.wikipedia.org/wiki/Protect_Ontario_by_Unleashing_our_Economy_Act
- Bill 5 in force: https://www.mccarthy.ca/en/insights/blogs/canadian-era-perspectives/bill-5-protect-ontario-by-unleashing-our-economy-act-2025-now-in-force
