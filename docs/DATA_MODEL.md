# Healthcare Appeals Data Model

## Overview

This document describes the synthetic healthcare data model for the appeals review agent.

## Entity Relationship Diagram

```
Members (1) ──< (M) Eligibility
Members (1) ──< (M) Prior Authorizations
Members (1) ──< (M) Claims
Members (1) ──< (M) Appeals

Providers (1) ──< (M) Prior Authorizations
Providers (1) ──< (M) Claims
Providers (1) ──< (M) Appeals

Prior Authorizations (1) ──< (0..1) Appeals
Claims (1) ──< (0..1) Appeals
```

## Tables

### 1. Members
**Purpose**: Patient/member demographics and enrollment information

**Key Fields**:
- `member_id`: Unique identifier (MEM-XXXXXX)
- `first_name`, `last_name`: Member name
- `date_of_birth`: DOB for age calculations
- `gender`: M/F/Other
- `ssn`: Social Security Number (stored without dashes)
- `address_line1`, `city`, `state`, `zip_code`: Address
- `phone`, `email`: Contact information
- `plan_type`: Medicare Advantage, Medicare Original, Medicaid, CHIP, Commercial

**Volume**: ~5,000 members

### 2. Eligibility
**Purpose**: Member enrollment and coverage periods (tracks active/inactive status)

**Key Fields**:
- `eligibility_id`: Unique identifier
- `member_id`: Foreign key to Members
- `coverage_start`: Coverage start date
- `coverage_end`: Coverage end date (NULL if currently active)
- `is_active`: Boolean flag for active coverage
- `plan_type`: Plan type during this period

**Use Case**: Critical for validating "member not active" denial reasons

**Volume**: Multiple records per member (enrollment periods)

### 3. Providers
**Purpose**: Healthcare providers (doctors, facilities, labs)

**Key Fields**:
- `provider_id`: Unique identifier (PROV-XXXXXX)
- `npi`: National Provider Identifier (10-digit) - **connects to external NPI registry**
- `provider_name`: Provider name (company or doctor name)
- `provider_type`: Physician, Hospital, Clinic, Specialist, Urgent Care, Lab, Pharmacy
- `specialty`: Medical specialty (Cardiology, Orthopedics, etc.)
- `address_line1`, `city`, `state`, `zip_code`: Provider address
- `phone`: Contact phone
- `tax_id`: Tax identification number

**Volume**: ~500 providers

### 4. Prior Authorizations
**Purpose**: Pre-service authorization requests and decisions

**Key Fields**:
- `prior_auth_id`: Unique identifier (PA-XXXXXXXX)
- `member_id`: Foreign key to Members
- `provider_id`: Foreign key to Providers
- `service_type`: MRI, CT Scan, Surgery, Specialist Consultation, etc.
- `service_date`: Date of service
- `request_date`: When PA was requested
- `decision_date`: When decision was made
- `is_approved`: Boolean approval status
- `denial_reason`: Reason for denial (if denied)
- `was_member_active`: Whether member was active at service date
- Procedure/drug: `cpt_code`, `hcpcs_code` for non-drug services; for **Prescription Drug**, `drug_name` (mock drug name in synthetic data). `ndc_code` is left null; NDC codes are joined from an external source by drug name.

**Denial Reasons**:
- Not medically necessary
- Member not active at time of service
- Service not covered under plan
- Missing documentation
- Provider not in network
- Duplicate request
- Exceeds benefit limit
- Experimental/investigational
- Prior authorization expired
- Incomplete information

**Volume**: ~15,000 prior authorizations (75% approved, 25% denied)

### 5. Claims
**Purpose**: Medical service claims submitted for payment

**Key Fields**:
- `claim_id`: Unique identifier (CLM-XXXXXXXX)
- `member_id`: Foreign key to Members
- `provider_id`: Foreign key to Providers
- `service_type`: Type of service
- `service_date`: Date of service
- `billed_amount`: Amount billed
- `paid_amount`: Amount paid (if paid/partially paid)
- `status`: Paid, Denied, Pending, Partially Paid
- `denial_reason`: Reason for denial (if denied)
- `was_member_active`: Whether member was active at service date
- `submitted_date`: When claim was submitted
- `processed_date`: When claim was processed
- Procedure/drug: `cpt_code`, `hcpcs_code` for non-drug services; for **Prescription Drug**, `drug_name` (mock drug name in synthetic data). `ndc_code` is left null; NDC codes are joined from an external source by drug name.

**Volume**: ~30,000 claims

### 6. Appeals
**Purpose**: Appeals against denials (THE CORE ENTITY FOR THE AGENT)

**Key Fields**:
- `appeal_id`: Unique identifier (APL-XXXXXXXX)
- `appeal_type`: Prior Auth Denial, Claim Denial, Partial Denial
- `appeal_source`: Provider (85%) or Member (15%)
- `member_id`: Foreign key to Members
- `provider_id`: Foreign key to Providers
- `prior_auth_id`: Foreign key to Prior Authorizations (if Prior Auth appeal)
- `claim_id`: Foreign key to Claims (if Claim/Partial Denial appeal)
- `original_denial_reason`: The original reason for denial
- `appeal_date`: When appeal was filed
- `appeal_status`: Pending, Under Review, Approved, Denied, Withdrawn
- `is_overturned`: Whether original decision was overturned (37% overturn rate)
- `has_documentation`: Whether additional documentation was provided
- `appeal_context`: Context for why appeal was filed
- `reviewer_notes`: Notes from reviewer (populated by agent)

**Appeal Contexts** (why appeal was filed):
- Member was actually active at time of service
- Additional medical records provided
- Provider clarification submitted
- Change in member eligibility
- Coding error corrected
- Medical necessity documentation added
- Prior authorization was obtained but not linked

**Volume**: ~2,000 appeals
- 60% from Prior Auth Denials
- 35% from Claim Denials
- 5% from Partial Denials

**Key Metric**: 37% overturn rate (goal is to reduce this)

## Data Patterns

### Temporal Patterns
- Data spans last 6 months from current date
- Appeals filed 1-45 days after denial
- Prior auths requested 1-30 days before service date

### Distribution Patterns
- **Plan Types**: Age-based (Medicare for 65+, Medicaid for low-income, Commercial for others)
- **Appeal Sources**: 85% Provider, 15% Member
- **Overturn Rate**: 37% (matches real-world scenario)
- **Documentation**: 75% of appeals have additional documentation

### Referential Integrity
- All appeals reference valid members and providers
- Appeals reference either prior_auth_id OR claim_id (not both)
- Eligibility records ensure member status can be validated at any point in time

## Integration Points

### External MCP Connections
1. **NPI Registry**: Provider `npi` field connects to external NPI registry lookup
2. **CMS Coverage**: Member `plan_type` and eligibility data can be enriched with CMS coverage rules

### Internal Genie Space
- All tables will be exposed via Genie Space for natural language SQL queries
- Enables agent to query:
  - Member eligibility history
  - Provider information
  - Prior authorization patterns
  - Claim patterns
  - Appeal trends

## Next Steps

1. Execute data generation script on Databricks
2. Create Spark Declarative Pipeline to load raw data into bronze/silver/gold tables
3. Set up Genie Space with all tables
4. Configure external MCP connections
5. Build Multi-Agent Supervisor for appeals review
