# EHR Data Structure Compliance Analysis

## Overview

This document compares the generated healthcare data structure against real-world EHR standards and payer data models.

## Standards Reference

- **HL7 FHIR R4**: Modern healthcare data exchange standard
- **X12 EDI**: Healthcare transaction standards (837 claims, 278 prior auth, 270/271 eligibility)
- **CMS 1500/UB-04**: Paper claim forms (still referenced in digital formats)
- **NCPDP**: Pharmacy transactions
- **Real-world payer systems**: Molina, UnitedHealthcare, Aetna data models

---

## 1. MEMBERS (Patients)

### Current Structure
```python
{
    "member_id": "MEM-000001",
    "first_name": "...",
    "last_name": "...",
    "date_of_birth": "1980-01-15",
    "gender": "M|F|Other",
    "ssn": "123456789",
    "address_line1": "...",
    "city": "...",
    "state": "CA",
    "zip_code": "90210",
    "phone": "...",
    "email": "...",
    "plan_type": "Medicare Advantage|Medicaid|Commercial|...",
    "created_at": "..."
}
```

### Comparison to Real Standards

#### ✅ **ALIGNED WITH FHIR Patient Resource**
- `first_name`, `last_name` → FHIR `name.given`, `name.family`
- `date_of_birth` → FHIR `birthDate`
- `gender` → FHIR `gender` (M/F/Other matches FHIR)
- `address_line1`, `city`, `state`, `zip_code` → FHIR `address`
- `phone` → FHIR `telecom` (phone)
- `email` → FHIR `telecom` (email)
- `ssn` → FHIR `identifier` (SSN)

#### ⚠️ **MISSING FIELDS (Common in Real EHR)**
1. **Member ID Format**: Real systems use:
   - Subscriber ID (format varies by payer)
   - Member suffix (e.g., "01", "02" for dependents)
   - Group number (employer group)
   - **Recommendation**: Add `subscriber_id`, `member_suffix`, `group_number`

2. **Demographics**:
   - `middle_name` / `middle_initial`
   - `suffix` (Jr., Sr., III)
   - `marital_status`
   - `race` / `ethnicity` (required for CMS reporting)
   - `preferred_language`
   - **Recommendation**: Add these fields

3. **Contact**:
   - `emergency_contact_name`, `emergency_contact_phone`
   - `preferred_contact_method`
   - **Recommendation**: Add emergency contact

4. **Plan Details**:
   - `plan_id` / `plan_code` (specific plan identifier)
   - `group_name` (employer name)
   - `subscriber_relationship` (self, spouse, child)
   - `effective_date`, `termination_date` (should be in eligibility table)
   - **Recommendation**: Move plan details to eligibility table

#### ✅ **GOOD PRACTICES**
- SSN stored without dashes (common practice)
- Age-based plan assignment logic (realistic)
- Email optional (30% missing - realistic)

---

## 2. ELIGIBILITY/ENROLLMENT

### Current Structure
```python
{
    "eligibility_id": "ELG-00000001",
    "member_id": "MEM-000001",
    "coverage_start": "2024-01-01",
    "coverage_end": null,  # or date if terminated
    "is_active": true,
    "plan_type": "Medicare Advantage",
    "created_at": "..."
}
```

### Comparison to X12 270/271 (Eligibility Inquiry/Response)

#### ✅ **ALIGNED WITH X12 271**
- `coverage_start` → X12 `DTP*348` (Eligibility Begin Date)
- `coverage_end` → X12 `DTP*349` (Eligibility End Date)
- `is_active` → Derived from dates (standard practice)

#### ⚠️ **MISSING FIELDS (Critical for Appeals)**
1. **Benefit Information**:
   - `copay_amount`, `coinsurance_percent`, `deductible_amount`
   - `out_of_pocket_maximum`
   - `in_network` vs `out_of_network` benefits
   - **Recommendation**: Add benefit details table

2. **Coverage Details**:
   - `coverage_type` (Medical, Dental, Vision, Pharmacy)
   - `network_id` / `network_name`
   - `primary_care_provider_id` (PCP assignment)
   - **Recommendation**: Add coverage details

3. **Eligibility Status Codes**:
   - `eligibility_status` (Active, Terminated, Suspended, Pending)
   - `termination_reason` (Voluntary, Non-payment, Death, etc.)
   - **Recommendation**: Add status codes

4. **Retroactive Coverage**:
   - `retroactive_start_date` (for backdated eligibility)
   - **Critical for appeals**: "Member wasn't active" denials when retroactive coverage exists

#### ✅ **GOOD PRACTICES**
- Multiple enrollment periods per member (realistic)
- Gaps in coverage (realistic)
- Date-based active/inactive logic

---

## 3. PROVIDERS

### Current Structure
```python
{
    "provider_id": "PROV-000001",
    "npi": "1234567890",
    "provider_name": "Dr. John Smith" or "City Hospital",
    "provider_type": "Physician|Hospital|Clinic|...",
    "specialty": "Cardiology|...",
    "address_line1": "...",
    "city": "...",
    "state": "CA",
    "zip_code": "90210",
    "phone": "...",
    "tax_id": "12-3456789",
    "created_at": "..."
}
```

### Comparison to FHIR Practitioner/Organization & NPI Registry

#### ✅ **ALIGNED WITH FHIR**
- `npi` → FHIR `identifier` (NPI) - **10 digits (correct)**
- `provider_name` → FHIR `name` (Practitioner) or `name` (Organization)
- `specialty` → FHIR `qualification.specialty`
- `address` → FHIR `address`
- `phone` → FHIR `telecom`
- `tax_id` → FHIR `identifier` (Tax ID)

#### ⚠️ **MISSING FIELDS**
1. **Provider Credentials**:
   - `license_number`, `license_state`
   - `dea_number` (for controlled substances)
   - `credentials` (MD, DO, NP, PA, etc.)
   - **Recommendation**: Add credentials

2. **Network Information**:
   - `is_in_network` (for each plan/network)
   - `network_ids` (multiple networks)
   - `contract_start_date`, `contract_end_date`
   - **Recommendation**: Add provider-network relationship table

3. **Organization Details** (for facilities):
   - `facility_type` (Hospital, ASC, SNF, etc.)
   - `bed_count`
   - `certification_numbers` (CMS, Joint Commission)
   - **Recommendation**: Separate organization details

4. **NPI Validation**:
   - Current: Random 10-digit number
   - **Issue**: NPI has checksum algorithm (Luhn algorithm)
   - **Recommendation**: Generate valid NPIs or use real NPI registry

#### ✅ **GOOD PRACTICES**
- NPI format (10 digits)
- Provider type categorization
- Tax ID (EIN) included

---

## 4. PRIOR AUTHORIZATIONS

### Current Structure
```python
{
    "prior_auth_id": "PA-00000001",
    "member_id": "MEM-000001",
    "provider_id": "PROV-000001",
    "service_type": "MRI|CT Scan|Surgery|...",
    "service_date": "2024-06-15",
    "request_date": "2024-06-01",
    "decision_date": "2024-06-05",
    "is_approved": true|false,
    "denial_reason": "...",
    "was_member_active": true,
    "created_at": "..."
}
```

### Comparison to X12 278 (Prior Authorization)

#### ✅ **ALIGNED WITH X12 278**
- `request_date` → X12 `DTP*607` (Requested Service Date)
- `decision_date` → X12 `DTP*607` (Authorization Date)
- `is_approved` → X12 `HI*BK` (Authorization Number) or denial
- `denial_reason` → X12 `HI*BF` (Denial Reason Code)

#### ⚠️ **MISSING FIELDS (Critical for Appeals)**
1. **Service Details**:
   - `cpt_code` / `hcpcs_code` (procedure codes)
   - `icd10_diagnosis_codes` (diagnosis codes - array)
   - `service_description` (human-readable)
   - `quantity` / `units`
   - `place_of_service` (POS code: 11=Office, 21=Hospital, etc.)
   - **Recommendation**: Add procedure/diagnosis codes

2. **Authorization Details**:
   - `authorization_number` (if approved)
   - `authorized_units` / `authorized_amount`
   - `expiration_date` (authorizations expire)
   - `referring_provider_id` (if referred)
   - **Recommendation**: Add authorization details

3. **Clinical Information**:
   - `clinical_notes` / `medical_necessity_justification`
   - `supporting_documentation` (file references)
   - `urgency` (Routine, Urgent, Emergency)
   - **Recommendation**: Add clinical fields

4. **Denial Details**:
   - `denial_code` (standard code: CO-XX, PR-XX)
   - `denial_category` (Medical Necessity, Coverage, etc.)
   - `appeal_deadline` (days to appeal)
   - **Recommendation**: Add structured denial codes

#### ✅ **GOOD PRACTICES**
- Member eligibility check (`was_member_active`)
- Realistic denial reasons
- Request before service date logic

---

## 5. CLAIMS

### Current Structure
```python
{
    "claim_id": "CLM-00000001",
    "member_id": "MEM-000001",
    "provider_id": "PROV-000001",
    "service_type": "MRI|...",
    "service_date": "2024-06-15",
    "billed_amount": 1250.00,
    "paid_amount": 1000.00,
    "status": "Paid|Denied|Pending|Partially Paid",
    "denial_reason": "...",
    "was_member_active": true,
    "submitted_date": "2024-06-20",
    "processed_date": "2024-07-05"
}
```

### Comparison to CMS 1500 / UB-04 / X12 837

#### ✅ **ALIGNED WITH CLAIM STANDARDS**
- `service_date` → CMS 1500 Field 24A (Date of Service)
- `billed_amount` → CMS 1500 Field 24F (Charges)
- `paid_amount` → EOB (Explanation of Benefits) payment amount
- `status` → Claim status (standard values)
- `submitted_date` → Claim submission date
- `processed_date` → Claim adjudication date

#### ⚠️ **MISSING FIELDS (Critical for Appeals)**
1. **Procedure Codes**:
   - `cpt_code` / `hcpcs_code` (procedure code)
   - `modifier_codes` (modifiers: -25, -59, etc.)
   - `diagnosis_codes` (ICD-10 codes - array, primary + secondary)
   - `diagnosis_pointer` (which diagnosis applies to which procedure)
   - **Recommendation**: Add procedure/diagnosis codes

2. **Financial Details**:
   - `allowed_amount` (allowed by plan)
   - `deductible_applied`
   - `copay_amount`
   - `coinsurance_amount`
   - `patient_responsibility`
   - `write_off_amount`
   - **Recommendation**: Add detailed financial breakdown

3. **Claim Details**:
   - `place_of_service` (POS code)
   - `units` / `quantity`
   - `rendering_provider_id` (who performed service)
   - `billing_provider_id` (who bills - may differ)
   - `referring_provider_id`
   - `prior_auth_id` (link to authorization)
   - **Recommendation**: Add claim details

4. **Denial Details**:
   - `denial_code` (CO-XX, PR-XX standard codes)
   - `denial_category`
   - `adjustment_reason_code`
   - **Recommendation**: Add structured denial codes

5. **Claim Lines**:
   - Current: One row per claim
   - **Reality**: Claims have multiple line items
   - **Recommendation**: Consider `claim_lines` table

#### ✅ **GOOD PRACTICES**
- Member eligibility check
- Realistic payment percentages (70-95%)
- Partial payment status
- Processing timeline

---

## 6. APPEALS

### Current Structure
```python
{
    "appeal_id": "APL-00000001",
    "appeal_type": "Prior Auth Denial|Claim Denial|Partial Denial",
    "appeal_source": "Provider|Member",
    "member_id": "MEM-000001",
    "provider_id": "PROV-000001",
    "prior_auth_id": "PA-...",
    "claim_id": "CLM-...",
    "original_denial_reason": "...",
    "appeal_date": "2024-07-01",
    "appeal_status": "Pending|Under Review|Approved|Denied|Withdrawn",
    "is_overturned": true|false,
    "has_documentation": true,
    "appeal_context": "...",
    "reviewer_notes": null,
    "created_at": "..."
}
```

### Comparison to CMS Appeals Process

#### ✅ **ALIGNED WITH CMS APPEALS**
- `appeal_type` → CMS appeal levels (Level 1, 2, 3)
- `appeal_source` → Provider vs Member (realistic split)
- `appeal_status` → Standard appeal statuses
- `is_overturned` → Overturn tracking (37% rate - realistic)
- `has_documentation` → Documentation flag

#### ⚠️ **MISSING FIELDS**
1. **Appeal Level**:
   - `appeal_level` (Level 1, Level 2, Level 3, ALJ, Federal)
   - `previous_appeal_id` (if Level 2+)
   - **Recommendation**: Add appeal level

2. **Appeal Details**:
   - `appeal_reason` (structured reason codes)
   - `submitted_documentation` (file references/URIs)
   - `reviewer_id` / `reviewer_name`
   - `review_date`
   - `decision_rationale` (why overturned/upheld)
   - **Recommendation**: Add review details

3. **Timeline**:
   - `deadline_date` (appeal deadline)
   - `acknowledgment_date`
   - `review_start_date`
   - `decision_date`
   - **Recommendation**: Add timeline fields

4. **Appeal Context** (Current: Free text):
   - Should be structured:
     - `member_was_active` (boolean)
     - `documentation_provided` (boolean)
     - `coding_corrected` (boolean)
     - `eligibility_changed` (boolean)
   - **Recommendation**: Structure appeal context

#### ✅ **GOOD PRACTICES**
- Links to original denial (prior_auth_id or claim_id)
- Original denial reason preserved
- Overturn tracking
- Documentation flag

---

## Overall Assessment

### ✅ **STRENGTHS**
1. **Core Structure**: Aligned with FHIR and X12 standards
2. **Relationships**: Proper foreign keys and referential integrity
3. **Business Logic**: Realistic eligibility checks, denial reasons, appeal patterns
4. **Data Quality**: Non-uniform distributions, temporal patterns

### ⚠️ **GAPS FOR EHR-LIKE DATA**
1. **Missing Clinical Codes**: No CPT/HCPCS, ICD-10 codes
2. **Missing Financial Details**: Limited claim financial breakdown
3. **Missing Provider Credentials**: No licenses, DEA numbers
4. **Missing Benefit Details**: No copay, deductible, OOP max
5. **Missing Network Information**: No in-network/out-of-network flags

### 📊 **RECOMMENDATIONS FOR ENHANCEMENT**

#### High Priority (For Appeals Use Case)
1. **Add Clinical Codes**:
   - CPT/HCPCS codes to Prior Auths and Claims
   - ICD-10 diagnosis codes
   - This is critical for appeals review

2. **Add Structured Denial Codes**:
   - CO-XX (Contractual Obligation) codes
   - PR-XX (Patient Responsibility) codes
   - Standard denial reason codes

3. **Add Appeal Documentation**:
   - File references/URIs for supporting documents
   - Structured appeal context fields

#### Medium Priority
4. **Add Benefit Details Table**:
   - Copay, coinsurance, deductible
   - Out-of-pocket maximums
   - In-network vs out-of-network benefits

5. **Add Provider Network Relationships**:
   - Which providers are in which networks
   - Contract dates

#### Low Priority (Nice to Have)
6. **Add Member Demographics**:
   - Race/ethnicity (for CMS reporting)
   - Preferred language
   - Emergency contacts

7. **Add Claim Line Items**:
   - Multiple procedures per claim
   - Line-level financials

---

## Conclusion

**Current State**: ✅ **Good foundation** - Core structure aligns with healthcare standards

**For Appeals Use Case**: ⚠️ **Needs enhancement** - Missing clinical codes and structured denial codes that are critical for appeals review

**Recommendation**: Add clinical codes (CPT/HCPCS, ICD-10) and structured denial codes to make this production-ready for appeals review.
