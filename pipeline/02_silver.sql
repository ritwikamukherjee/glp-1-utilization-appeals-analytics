-- Silver Layer: Enriched appeals with all dimensional joins and derived fields

CREATE OR REFRESH MATERIALIZED VIEW silver_enriched_appeals
AS
SELECT
  -- Appeal core fields
  a.appeal_id,
  a.appeal_type,
  a.appeal_source,
  a.member_id,
  a.provider_id,
  a.prior_auth_id,
  a.claim_id,
  a.original_denial_reason,
  a.appeal_date,
  a.appeal_status,
  a.is_overturned,
  a.has_documentation,

  -- Member demographics
  m.date_of_birth,
  m.gender,
  m.state        AS member_state,
  m.plan_type    AS member_plan_type,

  -- Eligibility
  e.coverage_start,
  e.coverage_end,
  e.is_active,

  -- Provider attributes
  p.provider_name,
  p.provider_type,
  p.specialty,
  p.state        AS provider_state,

  -- Prior auth attributes (for Prior Auth Denial appeals)
  pa.service_type       AS pa_service_type,
  pa.denial_reason      AS pa_denial_reason,
  pa.denial_category    AS pa_denial_category,

  -- Claim attributes (for Claim Denial appeals)
  cl.service_type       AS cl_service_type,
  cl.denial_reason      AS cl_denial_reason,
  cl.denial_category    AS cl_denial_category,
  cl.billed_amount,
  cl.status             AS claim_status,

  -- Derived: age band bucketed from date_of_birth relative to appeal_date
  CASE
    WHEN DATEDIFF(a.appeal_date, m.date_of_birth) / 365.25 < 18  THEN '<18'
    WHEN DATEDIFF(a.appeal_date, m.date_of_birth) / 365.25 < 35  THEN '18-34'
    WHEN DATEDIFF(a.appeal_date, m.date_of_birth) / 365.25 < 50  THEN '35-49'
    WHEN DATEDIFF(a.appeal_date, m.date_of_birth) / 365.25 < 65  THEN '50-64'
    ELSE '65+'
  END AS age_band,

  -- Derived: effective denial reason — prefer PA/claim over appeal-level
  COALESCE(pa.denial_reason, cl.denial_reason, a.original_denial_reason) AS effective_denial_reason,

  -- Derived: effective denial category
  COALESCE(pa.denial_category, cl.denial_category)                       AS effective_denial_category,

  -- Derived: service type
  COALESCE(pa.service_type, cl.service_type)                             AS service_type

FROM bronze_appeals AS a

LEFT JOIN bronze_members AS m
  ON a.member_id = m.member_id

LEFT JOIN bronze_eligibility AS e
  ON  a.member_id = e.member_id
  AND a.appeal_date BETWEEN e.coverage_start AND COALESCE(e.coverage_end, DATE('9999-12-31'))

LEFT JOIN bronze_providers AS p
  ON a.provider_id = p.provider_id

LEFT JOIN bronze_prior_authorizations AS pa
  ON a.prior_auth_id = pa.prior_auth_id

LEFT JOIN bronze_claims AS cl
  ON a.claim_id = cl.claim_id;
