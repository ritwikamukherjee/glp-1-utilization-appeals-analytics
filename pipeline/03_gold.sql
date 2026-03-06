-- Gold Layer: Aggregated views for dashboard consumption

-- 1. Overall KPIs
CREATE OR REFRESH MATERIALIZED VIEW gold_appeal_summary
AS
SELECT
  COUNT(*)                                                    AS total_appeals,
  SUM(CAST(is_overturned AS INT))                            AS overturned_count,
  ROUND(
    SUM(CAST(is_overturned AS INT)) * 100.0 / COUNT(*), 2
  )                                                           AS overturn_rate,
  SUM(CASE WHEN appeal_status = 'Pending' THEN 1 ELSE 0 END) AS pending_count,
  SUM(CAST(has_documentation AS INT))                        AS with_documentation_count,
  COUNT(DISTINCT member_id)                                   AS unique_members,
  COUNT(DISTINCT provider_id)                                 AS unique_providers,
  MIN(appeal_date)                                            AS earliest_appeal_date,
  MAX(appeal_date)                                            AS latest_appeal_date
FROM silver_enriched_appeals;


-- 2. Denial & Overturn Breakdown
CREATE OR REFRESH MATERIALIZED VIEW gold_denial_overturn
AS
SELECT
  effective_denial_reason,
  effective_denial_category,
  appeal_type,
  COUNT(*)                                                    AS appeal_count,
  SUM(CAST(is_overturned AS INT))                            AS overturned_count,
  ROUND(
    SUM(CAST(is_overturned AS INT)) * 100.0 / COUNT(*), 2
  )                                                           AS overturn_rate,
  SUM(CAST(has_documentation AS INT))                        AS with_docs_count,
  SUM(CASE WHEN NOT has_documentation THEN 1 ELSE 0 END)     AS without_docs_count
FROM silver_enriched_appeals
GROUP BY
  effective_denial_reason,
  effective_denial_category,
  appeal_type;


-- 3. Member Segment Analysis
CREATE OR REFRESH MATERIALIZED VIEW gold_member_segments
AS
SELECT
  member_plan_type,
  age_band,
  member_state                                               AS state,
  appeal_source,
  COUNT(*)                                                   AS appeal_count,
  SUM(CAST(is_overturned AS INT))                           AS overturned_count,
  ROUND(
    SUM(CAST(is_overturned AS INT)) * 100.0 / COUNT(*), 2
  )                                                          AS overturn_rate,
  COUNT(DISTINCT member_id)                                  AS unique_members
FROM silver_enriched_appeals
GROUP BY
  member_plan_type,
  age_band,
  member_state,
  appeal_source;


-- 4. Provider Insights
CREATE OR REFRESH MATERIALIZED VIEW gold_provider_insights
AS
SELECT
  provider_name,
  specialty,
  provider_type,
  provider_state,
  COUNT(*)                                                   AS appeal_count,
  SUM(CAST(is_overturned AS INT))                           AS overturned_count,
  ROUND(
    SUM(CAST(is_overturned AS INT)) * 100.0 / COUNT(*), 2
  )                                                          AS overturn_rate,
  COUNT(DISTINCT member_id)                                  AS unique_members,
  COUNT(DISTINCT effective_denial_reason)                    AS unique_denial_reasons
FROM silver_enriched_appeals
GROUP BY
  provider_name,
  specialty,
  provider_type,
  provider_state;
