# Healthcare Dashboard Story: "Reducing the Overturn Rate"

Audience: VP of Claims Operations / Chief Medical Officer at a payer.

Narrative arc: **Every overturned appeal is a denial we got wrong.**

---

## Page 1 — Executive Scorecard: "The Cost of Getting It Wrong"

- KPI tiles
  - Total appeals: 2,305
  - Overturn rate: 23%
  - YoY trend: up 5 pts
  - Estimated admin cost per appeal (~$1,200 industry avg) = **$2.7M/yr**
- Waterfall chart: Denials → Appeals filed → Overturned → $ paid after appeal
- Key message: "We overturn ~1 in 4 appeals. Each one costs us admin dollars AND member trust."

## Page 2 — Root Cause: "Which Denials Are We Getting Wrong?"

- Heatmap: denial reason × overturn rate; highlight red zones
  - Formulary exclusion: 55%
  - Service not covered: 44%
  - Experimental: 35%
- Bar chart: volume of overturned appeals by denial category
- Drill-down to specific denial codes
- Insight: "Formulary exclusions and 'service not covered' denials are overturned nearly half the time — our criteria need updating."

## Page 3 — The GLP-1 Crisis

- Timeline: GLP-1 utilization curve (2019–2026) overlaid with denial rate and overturn rate
- Drug-level breakdown: Ozempic vs Mounjaro vs Wegovy by plan type, denial rate, appeal outcome
- Medicare vs Commercial split showing the coverage-exclusion gap
- CMS drug spending benchmark overlay
- Insight: "GLP-1 denials are growing 3× faster than utilization. Step-therapy denials have 28% overturn — our step-therapy criteria may be too aggressive for T2DM-indicated GLP-1s."

## Page 4 — Documentation & Process Gaps

- Surprising finding: documentation doesn't significantly improve overturn rates (22.1% with docs vs 24.9% without) — the denials themselves are the problem, not the appeal evidence.
- Appeal source analysis: provider-filed (85% of appeals) vs member-filed
- Time-to-appeal: 8–30 days is the sweet spot (22.4% overturn)
- Insight: "The issue isn't that appellants bring better evidence — it's that our initial denial logic is wrong for certain categories."

## Page 5 — Provider Intelligence

- Scatter plot: provider appeal volume vs overturn rate (flag outliers)
- Top providers with 50%+ overturn rates — the plan is systematically wrong on their claims
- Cross-reference with fraud flags to separate "plan is wrong" providers from "provider is suspicious" providers
- Insight: "For 20 providers, we overturn more than half their appeals. Either auto-approve their common claim types or update utilization-review criteria."

## Page 6 — Fraud & Waste Intersection

- Sankey: fraudulent claims → Paid (60.7%) / Denied (23.1%) / Pending
- GLP-1 fraud typology: upcoding, compounding, telehealth mills
- Fraud that was appealed AND overturned (11 claims) — potential fraud slipping through appeals
- Insight: "We catch more legitimate claims than fraudulent ones. Redirect denial resources toward the $542K in fraud that's getting paid."

## Page 7 — Recommendations / Action Plan

1. Update formulary exclusion criteria — 55% overturn means the formulary is out of date.
2. Revise step-therapy requirements for GLP-1s — 28% overturn for step-therapy denials.
3. Create provider-specific auto-approval pathways for providers with 50%+ overturn rates.
4. Deploy ML-based pre-denial scoring — predict which denials will be overturned and route them for clinical review instead.
5. Separate fraud detection from claims denial — fraud claims have 60.7% paid rate; denial logic isn't catching fraud, it's catching legitimate claims.
6. Medicare GLP-1 triage — auto-route weight-loss GLP-1 claims for Medicare members to a specialized queue (they'll be denied per CMS, but proactive member communication reduces appeals).

---

## Platform story (what the demo showcases)

- **Unity Catalog** governing sensitive PHI/PII across claims, members, providers
- **AI/BI Metric Views** (the 4 metric view tables) for consistent KPI definitions
- **SQL analytics** for root-cause analysis
- **ML scoring** (MAS endpoint / MLflow judges) for appeal-outcome prediction
- **Compound AI agent** (the existing chat app) for natural-language exploration
- **Fraud detection** as a separate ML workload that feeds back into claims ops

---

## Build notes

- Underlying tables live in `hls_amer_catalog.`appeals-review``.
- Dataset snapshot used for the numbers above: 2026-02-27 post-regeneration (2,305 appeals, 33,500 claims, 2,095 GLP-1 Rx claims).
- KPI percentages should be implemented as Metric Views so the embedded dashboard and the chat agent share definitions.
- The app currently embeds a dashboard via iframe at `App.jsx` — replace that dashboard ID once the 7-page version is published.
