# Appeals Review Agent Architecture (Reference/Planning)

**Note**: This document describes the planned architecture for the appeals review agent. The agent will be built separately using Genie Spaces, MCP connections, and Agent Bricks. This is for reference only.

## Data Flow & Integration

### Overview

The appeals review agent will integrate multiple data sources to make informed decisions:

```
┌─────────────────┐
│  Appeals Intake │
│  (Forms/Email)  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│         Appeals Review Agent                   │
│  (Multi-Agent Supervisor / Playground)        │
└────────┬───────────────────────────────────────┘
         │
         ├─────────────────┬──────────────────┬──────────────────┐
         ▼                 ▼                  ▼                  ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   Genie      │  │   Medicare   │  │   NPI        │  │  Knowledge   │
│   Space      │  │   MCP        │  │   Registry   │  │  Assistant   │
│              │  │              │  │   (MCP)       │  │              │
│ Internal     │  │ Coverage     │  │ Provider     │  │ Policy       │
│ Data Query   │  │ Rules        │  │ Validation   │  │ Documents    │
└──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘
```

## Data Sources

### 1. Internal Data (Genie Space)

**Purpose**: Query internal healthcare data using natural language SQL

**Tables Available**:
- `members` - Patient demographics
- `eligibility` - Coverage periods and active status
- `providers` - Provider information with NPI
- `prior_authorizations` - Original authorization requests
- `claims` - Medical service claims
- `appeals` - Appeals against denials

**Use Cases**:
- "Was this member active on the service date?"
- "What was the original denial reason for this prior auth?"
- "Show me all appeals from this provider in the last 6 months"
- "What's the overturn rate for 'member not active' denials?"

**Example Query**:
```sql
SELECT 
    a.appeal_id,
    a.original_denial_reason,
    a.denial_code,
    e.is_active,
    e.coverage_start,
    e.coverage_end
FROM appeals a
JOIN eligibility e ON a.member_id = e.member_id
WHERE a.appeal_id = 'APL-00000001'
  AND a.service_date BETWEEN e.coverage_start AND COALESCE(e.coverage_end, '9999-12-31')
```

### 2. Medicare MCP Server

**Purpose**: External data enrichment for Medicare-specific rules and coverage

**Expected Capabilities**:
- **Coverage Rules**: What services are covered under Medicare plans
- **Medical Necessity Guidelines**: CMS coverage determinations
- **Prior Authorization Requirements**: Which services require PA
- **Benefit Limits**: Annual/lifetime limits for services
- **National Coverage Determinations (NCD)**: CMS coverage policies
- **Local Coverage Determinations (LCD)**: Regional coverage policies

**Example Use Cases**:
- "Is CPT code 70551 covered under Medicare Advantage?"
- "What are the medical necessity criteria for MRI of the brain?"
- "Does this diagnosis code support medical necessity for this procedure?"

**Integration Pattern**:
```python
# Agent calls Medicare MCP tool
medicare_coverage = medicare_mcp.check_coverage(
    cpt_code="70551",
    diagnosis_code="I10",
    plan_type="Medicare Advantage"
)
# Returns: coverage status, medical necessity criteria, PA requirements
```

### 3. NPI Registry MCP

**Purpose**: Validate and enrich provider information

**Expected Capabilities**:
- **Provider Validation**: Verify NPI exists and is active
- **Provider Details**: Specialty, credentials, practice location
- **Network Status**: Is provider in Medicare network?
- **Sanctions**: Any exclusions or sanctions

**Example Use Cases**:
- "Is NPI 1234567890 valid and active?"
- "What is the specialty of this provider?"
- "Is this provider in the Medicare network?"

**Integration Pattern**:
```python
# Agent calls NPI Registry MCP tool
provider_info = npi_mcp.lookup_provider(npi="1234567890")
# Returns: provider details, specialty, network status, sanctions
```

### 4. Knowledge Assistant

**Purpose**: Answer questions about policies, guidelines, and procedures

**Documentation Sources**:
- Appeals process guidelines
- Denial code definitions
- Medical necessity criteria
- Coverage policies
- Appeals review procedures

**Example Use Cases**:
- "What does denial code CO-16 mean?"
- "What documentation is required for a medical necessity appeal?"
- "What is the deadline for filing an appeal?"

## Why Both Prior Auth AND Appeals Data?

### ✅ **This is Standard Architecture**

Having both prior authorization and appeals data together is **not strange** - it's **essential**:

1. **Appeals Reference Original Denials**:
   - Appeals table has `prior_auth_id` or `claim_id` foreign keys
   - Need original denial reason, codes, dates to review appeal
   - Agent needs to see: "Original denial said X, but appeal claims Y"

2. **Complete Story**:
   ```
   Prior Auth Request → Denied (CO-16: Member not active)
                          ↓
                    Appeal Filed
                          ↓
                    Agent Reviews:
                    - Original denial reason
                    - Member eligibility at service date
                    - New documentation provided
                    - Decision: Overturn or Uphold
   ```

3. **Pattern Analysis**:
   - "Which denial reasons get overturned most?"
   - "Do appeals with documentation have higher overturn rates?"
   - "Are 'member not active' denials often wrong?"

4. **Real-World Systems**:
   - Payer systems (Molina, UnitedHealthcare, etc.) store both
   - Appeals systems query original denials
   - EOBs (Explanation of Benefits) reference original claims

### Data Relationship

```
Prior Authorization (PA-00000001)
├── Denied: CO-16 (Member not active)
├── Service Date: 2024-06-15
├── CPT Code: 70551 (MRI Brain)
└── Diagnosis: I10 (Hypertension)
    │
    └──> Appeal (APL-00000001)
         ├── References: PA-00000001
         ├── Appeal Reason: "Member was actually active"
         ├── Documentation: Yes
         └── Decision: Overturned (was_active = true)
```

## Denial Codes in Data Assets

### ✅ **Yes, Denial Codes are Natural**

Denial codes come from **payer systems** when claims/auths are processed:

1. **Source**: Payer adjudication system generates denial codes
2. **Format**: Standard codes (CO-XX, PR-XX) from ANSI X12
3. **Location**: 
   - In EOB (Explanation of Benefits) for claims
   - In denial letters for prior auths
   - In appeals records (original denial code)

4. **Why They're Important**:
   - **Structured**: Easier to analyze than free text
   - **Standardized**: Same codes across payers
   - **Actionable**: Each code has specific appeal process

### Denial Code Examples

| Code | Category | Meaning |
|------|----------|---------|
| CO-16 | Eligibility | Claim/service lacks information needed for adjudication |
| CO-50 | Medical Necessity | These services are not covered because they are not medically necessary |
| CO-4 | Coverage | This service/procedure is not covered under the patient's plan |
| CO-27 | Network | Expenses incurred after coverage terminated |
| CO-18 | Duplicate | Duplicate claim/service |
| CO-97 | Benefit Limit | The benefit for this service is included in the payment for another service |

## Agent Decision Flow

### Example: "Member Not Active" Appeal

```
1. INTAKE
   Appeal: APL-00000001
   Original Denial: CO-16 (Member not active)
   Appeal Context: "Member was actually active"

2. INTERNAL DATA QUERY (Genie)
   SELECT * FROM eligibility 
   WHERE member_id = 'MEM-000001'
     AND service_date BETWEEN coverage_start AND coverage_end
   Result: Member WAS active (eligibility record exists)

3. EXTERNAL ENRICHMENT (Medicare MCP)
   Check: Coverage rules for CPT 70551
   Result: Covered if medically necessary

4. DECISION
   Original denial: WRONG
   Member was active → OVERTURN
   Update: is_overturned = true, reviewer_notes = "Eligibility verified"
```

## Summary

- ✅ **Prior Auth + Appeals Together**: Standard architecture, appeals reference originals
- ✅ **Denial Codes in Data**: Natural - come from payer systems
- ✅ **Medicare MCP**: Provides coverage rules, medical necessity criteria
- ✅ **NPI Registry**: Validates providers, network status
- ✅ **Genie Space**: Queries internal data with natural language
- ✅ **Knowledge Assistant**: Answers policy/guideline questions

This architecture enables the agent to make informed decisions by combining internal data, external validation, and policy knowledge.
