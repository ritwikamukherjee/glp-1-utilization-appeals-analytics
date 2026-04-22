# Appeals Analytics Pipeline Plan

I want to build a denials and appeals analytics pipeline. It should combine appeals, prior authorizations, claims, and member data to identify top denial reasons and overturn patterns and surface member and provider insights.

Pipeline:

- Bronze layer: Bronze tables from hls_amer_catalog.appeals-review members, eligibility, providers, prior_authorizations, claims, and appeals.
- Silver layer: Cleaned, enriched, and joined into an enriched_appeals table (appeals + member + eligibility snapshot + prior auth or claim context).
- Gold layer: Dashboard-ready business metrics: appeal metrics, denial/overturn metrics, and member segments.

Appeal metrics:  
Overturn rate, appeal volume by status, pending/backlog count, appeals by type (Prior Auth Denial, Claim Denial, Partial Denial).

Denial / overturn: 
Top denial reasons by volume, overturn rate by denial reason, appeals with vs without documentation.

Member segments:  
Plan type (Medicare Advantage, Medicaid, Commercial, etc.), appeal source (Provider vs Member), age band if available, state/region.

Visualization: 
A dashboard with:

- Bar chart: e.g. appeal volume by denial reason or overturn rate by plan type.
- KPI charts: Total appeals, overturn rate, pending count, avg time to resolution (if you have dates).
- Tables: Top denial reasons, appeals by status, franchise-style “top” view (e.g. top providers by appeal volume or top denial reasons by overturn rate).

Make it filterable (e.g. date range, appeal type, appeal status, plan type, denial reason).

The end.
