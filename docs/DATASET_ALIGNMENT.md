# Synthetic Data vs. Standard Dataset Types

This document confirms how the appeals-review synthetic data aligns with common medical claims and appeals dataset types and standard sources.

---

## Summary Table

| Dataset Type | Purpose | Our Data Alignment | Notes |
|--------------|---------|--------------------|--------|
| **Medical Claims / Claims Dataset** | Predict approval/denial of medical claims | ✅ **Strong** | `claims` table: status (Paid/Denied/Pending/Partially Paid), denial_reason, amounts, service_type, member, provider. |
| **Claims Adjudication Dataset** | Claim approval/denial decisions | ✅ **Strong** | Same as above; adjudication outcome = `status` + `denial_reason`. |
| **Appeal Decision / Claims Appeal Dataset** | Predict appeal outcome: upheld vs overturned | ✅ **Strong** | `appeals` table: `is_overturned`, `appeal_status` (Approved/Denied = overturned/upheld), `appeal_type`, `original_denial_reason`. |
| **Prior Authorization (PA) Dataset** | Predict approval/denial of PA requests | ✅ **Strong** | `prior_authorizations` table: `is_approved`, `denial_reason`, service_type, dates, codes. |
| **Coverage Determination Dataset** | Predict if treatment meets medical necessity | ⚠️ **Partial** | Medical necessity is a denial reason and appears in appeal context; no separate coverage determination entity. |

---

## 1. Medical Claims Dataset / Claims Dataset (most general)

**Typical use:** Predict approval/denial of medical claims.

**Our synthetic data:**

- **Table:** `claims`
- **Outcome:** `status` = Paid | Denied | Pending | Partially Paid (approval/denial at claim level).
- **Features:** `member_id`, `provider_id`, `service_type`, `service_date`, `billed_amount`, `prior_auth_id` (linked PA), `was_member_active`, `primary_diagnosis_code`, `cpt_code` / `hcpcs_code` / `drug_name` (for Prescription Drug; NDC from external source), `denial_reason` (when denied), `denial_code` (e.g. CO-16, CO-4).
- **Volume:** ~30,000 claims; mix of statuses (e.g. ~70% Paid, ~15% Denied, ~10% Pending, ~5% Partially Paid in the generator).

**Alignment:** ✅ Matches. The dataset supports building a model to predict claim-level approval/denial from member, provider, service, and eligibility-related features.

---

## 2. Claims Adjudication Dataset

**Typical use:** Focus on claim approval/denial decisions (adjudication outcome).

**Our synthetic data:**

- Same **claims** table; adjudication decision is represented by `status` and `denial_reason` (and optionally `denial_code` / `denial_category`).
- Dates: `submitted_date`, `processed_date`, `service_date` support timing and process analysis.

**Alignment:** ✅ Matches. This is a claims adjudication dataset with explicit decision and reason.

---

## 3. Appeal Decision Dataset / Claims Appeal Dataset (most specific)

**Typical use:** Predict outcome of claim (or prior auth) appeals:
- **Upheld** = original decision maintained (appeal denied).
- **Overturned** = original decision reversed (appeal approved).

**Our synthetic data:**

- **Table:** `appeals`
- **Outcome:**
  - `appeal_status`: Pending | Under Review | Approved | Denied | Withdrawn.
  - `is_overturned`: boolean — `True` = original decision reversed (aligned with “overturned”), `False` = original decision maintained when appeal is resolved (aligned with “upheld” when status = Denied).
- **Context:** `appeal_type` (Prior Auth Denial | Claim Denial | Partial Denial), `original_denial_reason`, `prior_auth_id` or `claim_id`, `appeal_source` (Provider | Member), `has_documentation`, `appeal_context` (e.g. “Medical necessity documentation added”, “Coding error corrected”).
- **Volume:** ~2,000 appeals; ~37% overturn rate (tunable in generator); 60% from PA denials, 35% claim denials, 5% partial denials; 85% provider-filed, 15% member-filed.

**Alignment:** ✅ Matches. We have explicit upheld/overturned semantics via `appeal_status` and `is_overturned`, and enough structure to predict appeal outcome from type, denial reason, documentation, and context.

---

## 4. Prior Authorization (PA) Dataset

**Typical use:** Predict approval/denial of prior authorization requests.

**Our synthetic data:**

- **Table:** `prior_authorizations`
- **Outcome:** `is_approved` (boolean), `denial_reason` when denied.
- **Features:** `member_id`, `provider_id`, `service_type`, `service_date`, `request_date`, `decision_date`, `was_member_active`, CPT/HCPCS procedure codes, `drug_name` for Prescription Drug (NDC from external source), ICD-10 diagnosis codes, `denial_code` (e.g. CO-50, CO-16), `denial_category` (e.g. Medical Necessity, Eligibility).
- **Volume:** ~15,000 PAs; ~75% approved, ~25% denied; denial reasons include “Not medically necessary”, “Member not active at time of service”, “Service not covered under plan”, etc.

**Alignment:** ✅ Matches. Full PA request/decision data suitable for predicting PA approval/denial.

---

## 5. Coverage Determination Dataset

**Typical use:** Predict whether treatment meets medical necessity (coverage determination).

**Our synthetic data:**

- **Not a separate table.** We do **not** have a dedicated “coverage determination” or “medical necessity determination” entity.
- **Signals that are present:**
  - PA and claim **denial reasons** include “Not medically necessary” (and CO-50 / Medical Necessity category).
  - **Appeals** include `appeal_context` such as “Medical necessity documentation added” and “Not medically necessary” as `original_denial_reason`.
- So “medical necessity” appears as a denial reason and in appeal context, but there is no explicit coverage-determination record or binary “meets medical necessity” field.

**Alignment:** ⚠️ Partial. Usable for analyzing medical necessity–related denials and appeals, but not a full coverage determination dataset. Extending the model with a dedicated coverage determination step (or derived flag) would improve alignment.

---

## 6. Standard Sources (conceptual alignment)

| Source | How our synthetic data aligns |
|--------|-------------------------------|
| **CMS Appeals Data** | Plan types include Medicare Advantage and Medicare Original; appeal outcomes (overturn rate, statuses), appeal types, and denial reasons are CMS-like. Structure supports similar reporting (e.g. by plan, denial reason, appeal type). Data is synthetic, not real CMS data. |
| **Medicare Advantage Appeals** | MA is a plan_type; appeals link to members and plans. Mix of PA vs claim denials and provider vs member appeals is consistent with MA appeals reporting. |
| **Proprietary insurer datasets** | Schema is insurer-like: members, eligibility, providers, prior authorizations, claims, appeals, with standard codes (CO-XX, CPT, HCPCS, ICD-10) and denial categories. |
| **Healthcare research datasets** | De-identified, synthetic, with clear entity relationships and codes; suitable for research-style analyses and ML experiments. |

---

## Conclusion

- **Medical claims / claims adjudication:** ✅ Strong match — `claims` supports approval/denial prediction and adjudication analysis.
- **Appeal decision (upheld/overturned):** ✅ Strong match — `appeals` with `is_overturned` and `appeal_status` explicitly model upheld vs overturned.
- **Prior authorization:** ✅ Strong match — `prior_authorizations` supports PA approval/denial prediction.
- **Coverage determination / medical necessity:** ⚠️ Partial — medical necessity is present as denial reason and in appeal context; no dedicated coverage determination table.
- **Standard sources:** ✅ Structurally and conceptually aligned with CMS-style appeals, MA appeals, and typical insurer/research claims data; data is synthetic, not real CMS or proprietary data.

If you need to strengthen coverage-determination alignment, the next step would be to add a dedicated coverage determination table (or a medical-necessity flag/outcome) and link it to PAs and claims.
