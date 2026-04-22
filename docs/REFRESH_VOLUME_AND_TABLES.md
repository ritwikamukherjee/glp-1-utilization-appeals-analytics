# Refreshing volume data and tables

## 1. Refresh the volume (programmatic)

Regenerate synthetic data in the Unity Catalog volume so it includes **drug_name** (and no fake NDC). Use either option below.

### Option A: Run the notebook (recommended)

1. In Databricks, open:  
   **`/Workspace/Users/raven.mukherjee@databricks.com/appeals-review/scripts/refresh_volume_notebook`**
2. Attach a **cluster** (no cluster is needed for SQL warehouses; this job needs a cluster).
3. Run all cells.  
   - The notebook installs `faker` and `holidays` if needed, then runs the generator.  
   - Output is written to:  
     **`/Volumes/hls_amer_catalog/appeals-review/raw_data/`**  
     (members, eligibility, providers, prior_authorizations, claims, appeals).

### Option B: Run the Python script on a cluster

From a notebook or job, run the generator script on the same cluster:

```python
# In a notebook cell (with a cluster attached):
%run /Workspace/Users/raven.mukherjee@databricks.com/appeals-review/scripts/generate_healthcare_data
```

Or schedule a **job** that runs `generate_healthcare_data.py` from that path on a cluster.

### Config (optional)

- **Config path:** `/Workspace/Users/raven.mukherjee@databricks.com/appeals-review/config.py`  
- The script uses this if it’s on the Python path (e.g. when run via the notebook).  
- If config is missing, the script uses built-in defaults (same catalog/schema/volume, 5K members, 15K PAs, 30K claims, 2K appeals).

---

## 2. Refresh the tables (you run this)

After the volume has been refreshed, recreate or refresh the Delta tables from the volume so they pick up the new **drug_name** column and the updated **ndc_code** (null for drugs).

- **If you use the SQL pipeline** (`create_tables.sql`): run it again (e.g. in a SQL warehouse or as a job). It does `CREATE OR REPLACE TABLE ... AS SELECT * FROM read_files('/Volumes/...')`, so the new schema is applied.
- **If you use DLT or another pipeline**: run a full refresh so the tables are rebuilt from the volume.

Example (if you run SQL manually):

```sql
-- Example: refresh one table from the volume (repeat for each table)
CREATE OR REPLACE TABLE hls_amer_catalog.`appeals-review`.prior_authorizations AS
SELECT * FROM read_files(
  '/Volumes/hls_amer_catalog/appeals-review/raw_data/prior_authorizations',
  format => 'parquet'
);

CREATE OR REPLACE TABLE hls_amer_catalog.`appeals-review`.claims AS
SELECT * FROM read_files(
  '/Volumes/hls_amer_catalog/appeals-review/raw_data/claims',
  format => 'parquet'
);
-- ... and similarly for members, eligibility, providers, appeals
```

---

## Summary

| Step | Who | What |
|------|-----|------|
| 1. Refresh volume | You (programmatic) | Run the notebook or script on a cluster; overwrites parquet in the volume with new data (drug_name, no fake NDC). |
| 2. Refresh tables | You | Run your table pipeline / SQL so Delta tables are recreated or refreshed from the volume. |
