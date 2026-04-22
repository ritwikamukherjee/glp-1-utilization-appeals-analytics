# Genie Space: Claim Denials & Appeals Review

## Overview

A **Databricks Genie Space** is set up for natural language exploration of claim denials, prior authorizations, and appeals.

| Item | Value |
|------|--------|
| **Display name** | Claim Denials & Appeals Review |
| **Space ID** | `01f1083faf3b1f32b53f69073b52bb38` |
| **Catalog / schema** | `hls_amer_catalog`.`appeals-review` |
| **Warehouse** | Auto-selected (ID: `4b28691c780d9875`) |

## Tables in the space

**Base tables (6):**

- **members** – Member demographics, plan_type
- **eligibility** – Coverage periods, is_active, coverage_start/end
- **providers** – Provider info, npi, provider_type, specialty
- **prior_authorizations** – PA requests, is_approved, denial_reason, service_type
- **claims** – Claims, status (Paid/Denied/Pending), denial_reason, billed_amount, paid_amount
- **appeals** – Appeals, appeal_type, appeal_status, is_overturned, original_denial_reason

**Metric views (2) – for consistent KPIs:**

- **appeals_metrics** – Dimensions: Appeal Type, Appeal Status, Original Denial Reason, Appeal Source. Measures: Appeal Count, Overturned Count, Overturn Rate. Use for “overturn rate by …”, “appeal count by …”.
- **claim_denials_metrics** – Dimensions: Claim Status, Denial Reason, Service Type. Measures: Claim Count, Denied Count, Total Billed, Total Paid. Use for “denied count by …”, “total billed/paid by …”.

To add the metric views to this Genie Space: Genie → Claim Denials & Appeals Review → Space settings → add tables `appeals_metrics` and `claim_denials_metrics` from `hls_amer_catalog`.`appeals-review`. See [METRIC_VIEWS.md](METRIC_VIEWS.md) for how they help and example questions.

Use these table and column names when asking questions so Genie can generate accurate SQL.

## Sample questions to add in the UI

In the Genie Space in Databricks, you can add these as **sample questions** (or use them as-is):

1. How many claims were denied and what are the top denial reasons?
2. What is the appeal overturn rate by appeal type?
3. How many appeals are pending or under review?
4. Which providers have the most prior auth denials?
5. Show appeals that were overturned with their original denial reason.
6. How many members have active eligibility?
7. Break down claim denials by service type.
8. What are the most common original denial reasons for appeals?
9. List appeals filed by members vs providers (use appeal_source).
10. Show prior authorizations that were denied and whether they were appealed.

## Instructions for better answers

You can add **space instructions** in the Genie UI (Space settings → Instructions) so the model stays consistent:

- Join **appeals** to **claims** on `claim_id` for claim-denial appeals, and to **prior_authorizations** on `prior_auth_id` for prior-auth appeals.
- Join **members** and **providers** when you need member or provider attributes (e.g. plan_type, provider_name).
- Use **eligibility** to check active coverage: `is_active = true` and coverage dates.
- For denial analysis use: **claims**.`denial_reason` and **prior_authorizations**.`denial_reason`, and **appeals**.`original_denial_reason`.
- Appeal outcome: **appeals**.`appeal_status` (e.g. Approved, Denied) and **appeals**.`is_overturned`.

## Metric views (created)

Two metric views exist and can be added to this Genie Space for consistent KPIs:

- **appeals_metrics** – Appeal Count, Overturned Count, Overturn Rate; dimensions: Appeal Type, Appeal Status, Original Denial Reason, Appeal Source.
- **claim_denials_metrics** – Claim Count, Denied Count, Total Billed, Total Paid; dimensions: Claim Status, Denial Reason, Service Type.

See **[METRIC_VIEWS.md](METRIC_VIEWS.md)** for why they help Genie, how to add them to the space, and example SQL.

## Opening the space

1. In Databricks, go to **Genie** (in the left sidebar or search).
2. Open the space **Claim Denials & Appeals Review** (or search by the space ID above).
3. Ask questions in natural language; Genie will generate SQL and return results.

## Querying via API

To ask questions programmatically (e.g. from an agent or notebook):

```python
# Use ask_genie(space_id="01f1083faf3b1f32b53f69073b52bb38", question="Your question here")
# Use ask_genie_followup() for follow-up questions in the same conversation.
```
