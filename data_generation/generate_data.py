# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Medicare Appeals Analytics — Synthetic Data Generation
# MAGIC
# MAGIC Generates 6 tables with referential integrity:
# MAGIC - `members` (200 rows)
# MAGIC - `providers` (50 rows)
# MAGIC - `eligibility` (250 rows)
# MAGIC - `prior_authorizations` (300 rows)
# MAGIC - `claims` (500 rows)
# MAGIC - `appeals` (150 rows)
# MAGIC
# MAGIC **Edit `CATALOG` and `SCHEMA` below before running.**

# COMMAND ----------

CATALOG = "hls_amer_catalog"  # <-- Change to your catalog
SCHEMA = "rmukherjee"          # <-- Change to your schema

# COMMAND ----------

# MAGIC %pip install faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import random
import uuid
from datetime import date, datetime, timedelta
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    BooleanType, DateType,
)

spark = SparkSession.builder.getOrCreate()
fake = Faker()
Faker.seed(42)
random.seed(42)

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference data

# COMMAND ----------

PLAN_TYPES = ["Medicare", "Medicare Advantage", "Medicare Supplement"]
GENDERS = ["M", "F"]
STATES = ["CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI",
          "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI"]

PROVIDER_TYPES = ["Hospital", "Physician", "Specialist", "Lab", "DME Supplier", "Pharmacy"]
SPECIALTIES = [
    "Internal Medicine", "Cardiology", "Orthopedics", "Oncology",
    "Endocrinology", "Neurology", "Pulmonology", "Rheumatology",
    "Gastroenterology", "Nephrology", "General Surgery", "Family Medicine",
]

SERVICE_TYPES = [
    "Inpatient", "Outpatient", "Lab", "Imaging", "DME",
    "Pharmacy", "Surgery", "Therapy", "Home Health",
]

CPT_CODES = [
    99213, 99214, 99215, 99223, 99232, 99283, 99284, 99285,
    70553, 71046, 73721, 74177, 77067,
    27447, 43239, 43249, 47562, 49505,
    90834, 97110, 97140, 97530,
]

HCPCS_CODES = [
    "E0601", "E0260", "E0470", "E0691", "E1390",
    "J1745", "J2505", "J3490", "J7613", "J9035",
    "L0650", "A4253", "A6216", "K0823", "Q4131",
]

GLP1_DRUGS = ["semaglutide", "liraglutide", "dulaglutide", "tirzepatide"]
OTHER_DRUGS = [
    "metformin", "atorvastatin", "lisinopril", "amlodipine", "omeprazole",
    "albuterol", "levothyroxine", "gabapentin", "hydrochlorothiazide", "prednisone",
]

NDC_CODES = [
    "00169-4132-12", "00169-4130-12", "00002-1506-80", "00002-1506-01",
    "00024-5850-01", "00024-5851-01", "00169-4775-13", "00169-4771-13",
    "00002-4448-01", "00074-3799-01", "00378-1805-01", "00071-0156-23",
    "00378-4150-05", "00093-7180-01", "00186-0379-01",
]

ICD10_CODES = [
    "E11.9", "E11.65", "E78.5", "I10", "J44.1", "M17.11", "M17.12",
    "Z96.641", "K21.0", "G89.29", "E66.01", "E66.09", "N18.3",
    "I25.10", "J45.20", "F32.1", "M54.5",
]

DENIAL_REASONS = [
    "Not medically necessary",
    "Service not covered under plan",
    "Prior authorization not obtained",
    "Out-of-network provider",
    "Benefit maximum exceeded",
    "Duplicate claim submission",
    "Experimental or investigational",
    "Missing or incomplete documentation",
    "Non-formulary drug",
    "Coverage terminated",
    "Pre-existing condition exclusion",
    "Timely filing limit exceeded",
]

DENIAL_CODES = [
    "CO-50", "CO-97", "CO-4", "CO-151", "OA-23",
    "PR-96", "CO-29", "CO-16", "CO-11", "CO-18",
    "CO-197", "CO-22",
]

DENIAL_CATEGORIES = [
    "Medical Necessity", "Coverage", "Administrative",
    "Network", "Benefit Limit", "Documentation",
]

APPEAL_TYPES = ["Standard", "Expedited", "External Review"]
APPEAL_SOURCES = ["Member", "Provider", "Representative"]
APPEAL_STATUSES = ["Pending", "Under Review", "Resolved - Upheld", "Resolved - Overturned", "Withdrawn"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Members (200 rows)

# COMMAND ----------

def generate_members(n=200):
    rows = []
    for i in range(n):
        member_id = f"MBR-{i+1:05d}"
        rows.append({
            "member_id": member_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "date_of_birth": fake.date_of_birth(minimum_age=65, maximum_age=95).isoformat(),
            "gender": random.choice(GENDERS),
            "ssn": fake.ssn(),
            "address": fake.street_address(),
            "city": fake.city(),
            "state": random.choice(STATES),
            "zip": fake.zipcode(),
            "phone": fake.phone_number(),
            "email": fake.email(),
            "plan_type": random.choice(PLAN_TYPES),
            "created_at": fake.date_between(start_date="-3y", end_date="-1y").isoformat(),
        })
    return rows

members = generate_members()
df_members = spark.createDataFrame(members)
df_members.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.members")
print(f"members: {df_members.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Providers (50 rows)

# COMMAND ----------

def generate_providers(n=50):
    rows = []
    for i in range(n):
        provider_id = f"PRV-{i+1:04d}"
        ptype = random.choice(PROVIDER_TYPES)
        rows.append({
            "provider_id": provider_id,
            "npi": str(fake.random_number(digits=10, fix_len=True)),
            "provider_name": fake.company() + " " + ptype,
            "provider_type": ptype,
            "specialty": random.choice(SPECIALTIES),
            "address": fake.street_address(),
            "city": fake.city(),
            "state": random.choice(STATES),
            "zip": fake.zipcode(),
            "phone": fake.phone_number(),
            "tax_id": fake.bothify("##-#######"),
            "created_at": fake.date_between(start_date="-5y", end_date="-2y").isoformat(),
        })
    return rows

providers = generate_providers()
provider_ids = [p["provider_id"] for p in providers]
df_providers = spark.createDataFrame(providers)
df_providers.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.providers")
print(f"providers: {df_providers.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Eligibility (250 rows)

# COMMAND ----------

member_ids = [m["member_id"] for m in members]

def generate_eligibility(n=250):
    rows = []
    for i in range(n):
        mid = random.choice(member_ids)
        start = fake.date_between(start_date="-3y", end_date="-6m")
        end = start + timedelta(days=random.choice([365, 730, 1095]))
        is_active = end >= date.today()
        rows.append({
            "eligibility_id": f"ELG-{i+1:05d}",
            "member_id": mid,
            "coverage_start": start,
            "coverage_end": end,
            "is_active": is_active,
            "plan_type": random.choice(PLAN_TYPES),
            "created_at": start,
        })
    return rows

eligibility = generate_eligibility()
schema_elig = StructType([
    StructField("eligibility_id", StringType()),
    StructField("member_id", StringType()),
    StructField("coverage_start", DateType()),
    StructField("coverage_end", DateType()),
    StructField("is_active", BooleanType()),
    StructField("plan_type", StringType()),
    StructField("created_at", DateType()),
])
df_eligibility = spark.createDataFrame(eligibility, schema=schema_elig)
df_eligibility.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.eligibility")
print(f"eligibility: {df_eligibility.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prior Authorizations (300 rows)

# COMMAND ----------

def generate_prior_auths(n=300):
    rows = []
    for i in range(n):
        mid = random.choice(member_ids)
        pid = random.choice(provider_ids)
        svc = random.choice(SERVICE_TYPES)
        is_approved = random.random() < 0.65
        request_dt = fake.date_between(start_date="-2y", end_date="-1m")
        decision_dt = request_dt + timedelta(days=random.randint(1, 14))
        service_dt = decision_dt + timedelta(days=random.randint(1, 30))
        drug = random.choice(GLP1_DRUGS + OTHER_DRUGS) if svc == "Pharmacy" else None

        denial_reason = None
        denial_code = None
        denial_cat = None
        if not is_approved:
            denial_reason = random.choice(DENIAL_REASONS)
            denial_code = random.choice(DENIAL_CODES)
            denial_cat = random.choice(DENIAL_CATEGORIES)

        rows.append({
            "prior_auth_id": f"PA-{i+1:05d}",
            "member_id": mid,
            "provider_id": pid,
            "service_type": svc,
            "service_description": fake.sentence(nb_words=6),
            "cpt_code": random.choice(CPT_CODES),
            "hcpcs_code": random.choice(HCPCS_CODES),
            "ndc_code": random.choice(NDC_CODES) if drug else None,
            "primary_diagnosis_code": random.choice(ICD10_CODES),
            "secondary_diagnosis_codes": ",".join(random.sample(ICD10_CODES, k=random.randint(0, 3))),
            "service_date": service_dt.isoformat(),
            "request_date": request_dt.isoformat(),
            "decision_date": decision_dt.isoformat(),
            "is_approved": is_approved,
            "authorization_number": f"AUTH-{fake.random_number(digits=8, fix_len=True)}",
            "denial_reason": denial_reason,
            "denial_code": denial_code,
            "denial_category": denial_cat,
            "was_member_active": random.random() < 0.92,
            "created_at": request_dt.isoformat(),
            "drug": drug,
        })
    return rows

prior_auths = generate_prior_auths()
pa_ids = [pa["prior_auth_id"] for pa in prior_auths]
df_prior_auths = spark.createDataFrame(prior_auths)
df_prior_auths.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.prior_authorizations")
print(f"prior_authorizations: {df_prior_auths.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Claims (500 rows)

# COMMAND ----------

def generate_claims(n=500):
    rows = []
    for i in range(n):
        mid = random.choice(member_ids)
        pid = random.choice(provider_ids)
        pa_id = random.choice(pa_ids) if random.random() < 0.6 else None
        svc = random.choice(SERVICE_TYPES)
        status = random.choices(
            ["Approved", "Denied", "Pending", "In Review"],
            weights=[0.55, 0.25, 0.12, 0.08],
        )[0]
        billed = round(random.uniform(50, 25000), 2)
        allowed = round(billed * random.uniform(0.4, 0.95), 2)
        paid = round(allowed * random.uniform(0.6, 1.0), 2) if status == "Approved" else 0.0
        submitted = fake.date_between(start_date="-2y", end_date="-1m")
        processed = submitted + timedelta(days=random.randint(3, 45))
        drug = random.choice(GLP1_DRUGS + OTHER_DRUGS) if svc == "Pharmacy" else None

        denial_reason = None
        denial_code = None
        denial_cat = None
        if status == "Denied":
            denial_reason = random.choice(DENIAL_REASONS)
            denial_code = random.choice(DENIAL_CODES)
            denial_cat = random.choice(DENIAL_CATEGORIES)

        rows.append({
            "claim_id": f"CLM-{i+1:06d}",
            "member_id": mid,
            "provider_id": pid,
            "prior_auth_id": pa_id,
            "service_type": svc,
            "service_description": fake.sentence(nb_words=6),
            "cpt_code": random.choice(CPT_CODES),
            "hcpcs_code": random.choice(HCPCS_CODES),
            "ndc_code": random.choice(NDC_CODES) if drug else None,
            "primary_diagnosis_code": random.choice(ICD10_CODES),
            "secondary_diagnosis_codes": ",".join(random.sample(ICD10_CODES, k=random.randint(0, 3))),
            "service_date": (submitted - timedelta(days=random.randint(0, 30))).isoformat(),
            "billed_amount": billed,
            "allowed_amount": allowed,
            "paid_amount": paid,
            "status": status,
            "denial_reason": denial_reason,
            "denial_code": denial_code,
            "denial_category": denial_cat,
            "was_member_active": random.random() < 0.92,
            "submitted_date": submitted.isoformat(),
            "processed_date": processed.isoformat(),
        })
    return rows

claims = generate_claims()
claim_ids = [c["claim_id"] for c in claims]
df_claims = spark.createDataFrame(claims)
df_claims.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.claims")
print(f"claims: {df_claims.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Appeals (150 rows)

# COMMAND ----------

# Only reference denied claims and denied PAs
denied_claim_ids = [c["claim_id"] for c in claims if c["status"] == "Denied"]
denied_pa_ids = [pa["prior_auth_id"] for pa in prior_auths if not pa["is_approved"]]

def generate_appeals(n=150):
    rows = []
    for i in range(n):
        mid = random.choice(member_ids)
        pid = random.choice(provider_ids)
        appeal_status = random.choice(APPEAL_STATUSES)
        is_overturned = appeal_status == "Resolved - Overturned"

        # Reference a denied claim or PA (or both)
        cid = random.choice(denied_claim_ids) if denied_claim_ids and random.random() < 0.7 else None
        paid = random.choice(denied_pa_ids) if denied_pa_ids and random.random() < 0.5 else None
        denial_reason = random.choice(DENIAL_REASONS)

        appeal_dt = fake.date_between(start_date="-18m", end_date="-1m")

        rows.append({
            "appeal_id": f"APL-{i+1:05d}",
            "appeal_type": random.choice(APPEAL_TYPES),
            "appeal_source": random.choice(APPEAL_SOURCES),
            "member_id": mid,
            "provider_id": pid,
            "prior_auth_id": paid,
            "claim_id": cid,
            "original_denial_reason": denial_reason,
            "appeal_date": appeal_dt,
            "appeal_status": appeal_status,
            "is_overturned": is_overturned,
            "has_documentation": random.random() < 0.75,
            "appeal_context": fake.paragraph(nb_sentences=3),
            "reviewer_notes": fake.paragraph(nb_sentences=2) if appeal_status.startswith("Resolved") else None,
            "created_at": appeal_dt,
        })
    return rows

appeals = generate_appeals()
schema_appeals = StructType([
    StructField("appeal_id", StringType()),
    StructField("appeal_type", StringType()),
    StructField("appeal_source", StringType()),
    StructField("member_id", StringType()),
    StructField("provider_id", StringType()),
    StructField("prior_auth_id", StringType()),
    StructField("claim_id", StringType()),
    StructField("original_denial_reason", StringType()),
    StructField("appeal_date", DateType()),
    StructField("appeal_status", StringType()),
    StructField("is_overturned", BooleanType()),
    StructField("has_documentation", BooleanType()),
    StructField("appeal_context", StringType()),
    StructField("reviewer_notes", StringType()),
    StructField("created_at", DateType()),
])
df_appeals = spark.createDataFrame(appeals, schema=schema_appeals)
df_appeals.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.appeals")
print(f"appeals: {df_appeals.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

tables = ["members", "providers", "eligibility", "prior_authorizations", "claims", "appeals"]
for t in tables:
    count = spark.table(f"{CATALOG}.{SCHEMA}.{t}").count()
    print(f"  {t}: {count} rows")

print(f"\nAll tables written to {CATALOG}.{SCHEMA}")
