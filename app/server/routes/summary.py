"""GET /api/summary — return key dashboard metrics from the SQL warehouse."""

import logging
import os
import aiohttp

from fastapi import APIRouter

from server.config import get_serving_credentials

logger = logging.getLogger(__name__)
router = APIRouter()

WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "4b28691c780d9875")
CATALOG_SCHEMA = os.environ.get("CATALOG_SCHEMA", "hls_amer_catalog.rmukherjee")

QUERIES = {
    "appeals_by_status": f"""
        SELECT appeal_status, COUNT(*) as count,
               COUNT(CASE WHEN is_overturned='true' THEN 1 END) as overturned
        FROM {CATALOG_SCHEMA}.bronze_appeals
        GROUP BY appeal_status ORDER BY count DESC
    """,
    "claims_by_status": f"""
        SELECT status, COUNT(*) as count,
               ROUND(SUM(CAST(billed_amount AS DOUBLE)), 2) as total_billed,
               ROUND(SUM(CAST(paid_amount AS DOUBLE)), 2) as total_paid
        FROM {CATALOG_SCHEMA}.bronze_claims
        GROUP BY status ORDER BY count DESC
    """,
    "top_denial_reasons": f"""
        SELECT original_denial_reason as reason, COUNT(*) as count
        FROM {CATALOG_SCHEMA}.bronze_appeals
        WHERE original_denial_reason IS NOT NULL AND original_denial_reason != ''
        GROUP BY original_denial_reason ORDER BY count DESC LIMIT 5
    """,
    "recent_appeals": f"""
        SELECT appeal_id, member_id, appeal_type, appeal_status,
               original_denial_reason, appeal_date
        FROM {CATALOG_SCHEMA}.bronze_appeals
        ORDER BY appeal_date DESC LIMIT 8
    """,
    "member_count": f"""
        SELECT COUNT(DISTINCT member_id) as members,
               COUNT(DISTINCT CASE WHEN plan_type='Medicare' THEN member_id END) as medicare
        FROM {CATALOG_SCHEMA}.bronze_members
    """,
}


async def _run_sql(host: str, token: str, sql: str) -> list[dict]:
    """Execute SQL via the statement execution API and return rows as dicts."""
    url = f"{host}/api/2.0/sql/statements"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"warehouse_id": WAREHOUSE_ID, "statement": sql, "wait_timeout": "30s"}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers) as resp:
            body = await resp.json()

    if body.get("status", {}).get("state") != "SUCCEEDED":
        logger.error(f"SQL failed: {body.get('status', {})}")
        return []

    cols = [c["name"] for c in body.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows = body.get("result", {}).get("data_array", [])
    return [dict(zip(cols, row)) for row in rows]


@router.get("/summary")
async def summary():
    host, token = get_serving_credentials()
    results = {}

    for key, sql in QUERIES.items():
        try:
            results[key] = await _run_sql(host, token, sql)
        except Exception as e:
            logger.exception(f"Query {key} failed: {e}")
            results[key] = []

    return results
