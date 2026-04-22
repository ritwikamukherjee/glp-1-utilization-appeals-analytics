# Metric Views for Claim Denials & Appeals

## How and where metric views help the Genie Space

### What metric views are

**Unity Catalog metric views** define reusable business metrics (measures) and the dimensions you can slice them by. They live as views in your catalog; you query them with `MEASURE(...)` or by selecting their columns. They give you:

- **Single definition of KPIs** – e.g. “overturn rate” is defined once (e.g. `AVG(CASE WHEN is_overturned THEN 1 ELSE 0 END)`), so everyone – including Genie – uses the same formula.
- **Flexible grouping** – You can ask “overturn rate by appeal type” or “overturn rate by denial reason” without writing different SQL each time; the metric view exposes dimensions and measures separately.
- **Governance** – Metrics are first-class catalog objects (permissions, lineage, discovery).

### Where they help in this Genie Space

| Without metric views | With metric views |
|----------------------|-------------------|
| Genie writes ad hoc SQL (e.g. different `SUM(is_overturned)/COUNT(*)` vs `AVG(CAST(is_overturned AS INT))`) and can drift from your standard definition. | Genie can query **appeals_metrics** and use the pre-defined **Overturn Rate** and **Appeal Count** measures, so answers stay consistent. |
| Questions like “denied claim count by reason” require Genie to remember `status = 'Denied'` and the right aggregation. | **claim_denials_metrics** exposes **Denied Count**, **Claim Count**, **Total Billed**, **Total Paid** and dimensions (Claim Status, Denial Reason, Service Type), so Genie has clear, named concepts to use. |
| Exploratory questions (“overturn rate by type”, “by status”, “by denial reason”) each need correct SQL. | One metric view supports many questions: same measures, different dimensions. |

So metric views help the Genie Space by:

1. **Consistency** – Same KPI definition for “overturn rate”, “denied count”, etc., in Genie and in any dashboard or report.
2. **Easier NL → SQL** – Genie can map “what’s our overturn rate by appeal type?” to the **appeals_metrics** view and the **Overturn Rate** measure grouped by **Appeal Type**.
3. **Discovery** – In Catalog Explorer, analysts (and Genie’s context) see which metrics exist and what they mean (comments on the view and measures).

You still keep the base tables (**appeals**, **claims**, **prior_authorizations**, etc.) in the space for drill-down and ad hoc analysis; metric views sit alongside them for KPI-style questions.

---

## Metric views created

Two metric views are defined in `hls_amer_catalog`.`appeals-review` and can be added to your Genie Space.

### 1. `appeals_metrics`

**Source table:** `appeals`

**Purpose:** Appeals KPIs for review and reporting – counts and overturn rate, sliceable by type, status, denial reason, and source.

| Dimension | Expression | Use |
|-----------|------------|-----|
| Appeal Type | `appeal_type` | Prior Auth Denial, Claim Denial, Partial Denial |
| Appeal Status | `appeal_status` | Pending, Under Review, Approved, Denied, Withdrawn |
| Original Denial Reason | `original_denial_reason` | E.g. Not medically necessary, Member not active |
| Appeal Source | `appeal_source` | Provider vs Member |

| Measure | Expression | Use |
|---------|------------|-----|
| Appeal Count | `COUNT(*)` | Total appeals (or by dimension) |
| Overturned Count | `SUM(CASE WHEN is_overturned = true THEN 1 ELSE 0 END)` | Number of overturned appeals |
| Overturn Rate | `AVG(CASE WHEN is_overturned = true THEN 1.0 ELSE 0.0 END)` | 0–1 rate (e.g. 0.37 = 37%) |

**Example Genie-style questions:**

- “What is the appeal overturn rate by appeal type?”
- “How many appeals are there by status?”
- “What’s the overturn rate by original denial reason?”
- “Appeal count and overturn rate by appeal source (provider vs member).”

---

### 2. `claim_denials_metrics`

**Source table:** `claims`

**Purpose:** Claim-level KPIs – counts, denied count, and dollar amounts, by status, denial reason, and service type.

| Dimension | Expression | Use |
|-----------|------------|-----|
| Claim Status | `status` | Paid, Denied, Pending, Partially Paid |
| Denial Reason | `denial_reason` | Reason when status = Denied |
| Service Type | `service_type` | E.g. Physical Therapy, Mental Health |

| Measure | Expression | Use |
|---------|------------|-----|
| Claim Count | `COUNT(*)` | Total claims (or by dimension) |
| Denied Count | `SUM(CASE WHEN status = 'Denied' THEN 1 ELSE 0 END)` | Number of denied claims |
| Total Billed | `SUM(CAST(billed_amount AS DOUBLE))` | Sum of billed amount |
| Total Paid | `SUM(CAST(paid_amount AS DOUBLE))` | Sum of paid amount |

**Example Genie-style questions:**

- “How many claims were denied and what are the top denial reasons?”
- “Total billed and total paid by service type.”
- “Denied count by denial reason.”
- “Claim count and denied count by status.”

---

## Adding the metric views to the Genie Space

1. In Databricks, open **Genie** and the space **Claim Denials & Appeals Review**.
2. Open the space **settings** (e.g. gear or “Manage space”).
3. Under **Tables** (or “Data”), add:
   - `hls_amer_catalog`.`appeals-review`.`appeals_metrics`
   - `hls_amer_catalog`.`appeals-review`.`claim_denials_metrics`
4. Save.

The space will then have 8 assets: the 6 base tables plus these 2 metric views. Genie can answer both detail-level questions (using base tables) and KPI questions (using the metric views).

---

## Querying metric views directly (SQL)

You can query metric views in the SQL editor or in pipelines:

```sql
-- Appeals: appeal count and overturn rate by appeal type
SELECT
  "Appeal Type",
  MEASURE("Appeal Count"),
  MEASURE("Overturn Rate")
FROM hls_amer_catalog.`appeals-review`.appeals_metrics
GROUP BY "Appeal Type";

-- Claim denials: denied count and total billed by denial reason (denied only)
SELECT
  "Denial Reason",
  MEASURE("Denied Count"),
  MEASURE("Total Billed")
FROM hls_amer_catalog.`appeals-review`.claim_denials_metrics
WHERE "Claim Status" = 'Denied'
GROUP BY "Denial Reason";
```

---

## Summary

- **Why:** Metric views give the Genie Space consistent, named KPIs (overturn rate, denied count, totals) and clear dimensions, so Genie produces consistent answers and fewer ad hoc formula mistakes.
- **Where they help:** KPI-style questions (“overturn rate by …”, “denied count by …”, “total billed/paid by …”); base tables remain for detail and joins.
- **What was created:** `appeals_metrics` (appeal count, overturned count, overturn rate; dimensions: type, status, denial reason, source) and `claim_denials_metrics` (claim count, denied count, total billed, total paid; dimensions: status, denial reason, service type).
- **Next step:** Add `appeals_metrics` and `claim_denials_metrics` to your Genie Space tables so Genie can use them in natural language.
